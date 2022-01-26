import copy
from collections import Generator
from datetime import datetime
from functools import partial
from typing import cast

from corpus.datasources.openresearch import OREvent, OREventSeries
from fb4.widgets import Link, Image, LodTable
from lodstorage.jsonable import JSONAble
from lodstorage.lod import LOD
from onlinespreadsheet.spreadsheet import SpreadSheet
from onlinespreadsheet.tableediting import TableEditing
from onlinespreadsheet.tablequery import TableQuery
from ormigrate.fixer import ORFixer
from ormigrate.smw.pagefixer import PageFixerManager
from ormigrate.smw.rating import EntityRating
from werkzeug.exceptions import Unauthorized
from wikibot.wikiuser import WikiUser
from wikifile.wikiFile import WikiFile
from wikifile.wikiFileManager import WikiFileManager
from corpus.smw.topic import SMWEntity

from orapi.utils import WikiUserInfo, PageHistory


class WikiTableEditing(TableEditing):
    """
    Extends TableEditing by storing a wikiuser (publisher) and wikiFiles
    """

    def __init__(self, user: WikiUserInfo, **kwargs):
        super(WikiTableEditing, self).__init__(**kwargs)
        self.user = user

    @property
    def wikiFiles(self) -> dict:
        return getattr(self, "_wikiFiles", {})

    @wikiFiles.setter
    def wikiFiles(self, wikiFiles: dict):
        setattr(self, "_wikiFiles", wikiFiles)


class OrApi:
    """
    Functions to edit and extract events ans series from openresearch
    """

    EVENT_TEMPLATE_NAME = OREvent.templateName
    SERIES_TEMPLATE_NAME = OREventSeries.templateName

    def __init__(self, wikiId:str, authUpdates:bool=True, debug:bool=False):
        """

        Args:
            wikiId: id of the wiki
            authUpdates: apply updates to the wiki only if user is authenticated
            debug: print debug output if true
        """
        self.wikiId=wikiId
        self.authUpdates=authUpdates
        self.debug=debug
        self.eventTemplateProps={"pageTitle":"pageTitle", **{value:key for key, value in OREvent.getTemplateParamLookup().items()}}
        self.seriesTemplateProps = {"pageTitle":"pageTitle", **{value:key for key, value in OREventSeries.getTemplateParamLookup().items()}}
        self.optionalEnhancers={
            **OrMigrateWrapper.getOrMigrateFixers()
        }

    def getSeriesQuery(self, seriesAcronym:str) -> dict:
        """

        Args:
            seriesAcronym: acronym of the series

        Returns:
            Query for getting the series from the wiki
        """
        query={
            "name":self.SERIES_TEMPLATE_NAME,
            "ask":"{{#ask: [[%s]]OR[[Concept:Event series]][[EventSeries acronym::%s]]|mainlabel=pageTitle }}" % (seriesAcronym,seriesAcronym)
        }
        return query

    def getEventsOfSeriesQuery(self, seriesAcronym:str) -> dict:
        """

        Args:
            seriesAcronym: acronym of the series

        Returns:
            Query for getting the events in the given series from the wiki
        """
        query={
            "name":self.EVENT_TEMPLATE_NAME,
            "ask":"{{#ask: [[Concept:Event]][[Event in series::%s]]|mainlabel=pageTitle }}" % (seriesAcronym)
        }
        return query

    def getSeriesTableQuery(self, seriesAcronym:str):
        """

        Args:
            seriesAcronym: acronym of the series

        Returns:
            TableQuery for the given series
        """
        tableQuery=TableQuery(debug=self.debug)
        askQueries=[self.getSeriesQuery(seriesAcronym), self.getEventsOfSeriesQuery(seriesAcronym)]
        tableQuery.fromAskQueries(wikiId=self.wikiId, askQueries=askQueries)
        return tableQuery

    def getSeriesTableEditing(self, seriesAcronym:str, enhancers:list=None):
        """

        Args:
            seriesAcronym: acronym of the series

        Returns:
            WikiTableEditing for the given series
        """
        tableQuery=self.getSeriesTableQuery(seriesAcronym)
        tableEditing=cast(WikiTableEditing, tableQuery.tableEditing)
        tableEditing.addEnhancer(self.fetchEntityPropertiesFromMarkup)
        tableEditing.addEnhancer(partial(self.completeProperties, restrict=True))   # ToDo: restriction of exported properties needs to be discussed
        # to apply the fixers from ormigrate we need to normalize the entity property names
        tableEditing.addEnhancer(self.normalizeEntityProperties)

        if enhancers:
            for enhancer in enhancers:
                if enhancer in self.optionalEnhancers:
                    tableEditing.addEnhancer(self.optionalEnhancers.get(enhancer))
        # apply list of different fixers → conversion to EntityRaing required?

        # ? map property names back to template params so that the user sees which template param is going to be affected or stick to the normalized names?
        tableEditing.addEnhancer(partial(self.normalizeEntityProperties, reverse=True))
        return tableEditing

    def getSeriesTableEnhanceGenerator(self, tableEditing:WikiTableEditing):
        """

        Args:
            seriesAcronym:
            enhancers:

        Returns:

        """
        yield "Starting Enhancement Phase<br>"
        fnLookup={v:k for k,v in self.optionalEnhancers.items()}
        for callback in tableEditing.enhanceCallbacks:
            fnName=""
            if callback in fnLookup:
                fnName=fnLookup.get(callback)
            elif isinstance(callback, partial):
                fnName=callback.func.__name__
            else:
                fnName=callback.__name__
            yield f"Starting {fnName}"
            callback(tableEditing)
            yield "✅<br>"
        yield "Completed Enhancement Phase"

    def getTableEditingFromSpreadsheet(self, document, publisher:WikiUserInfo) -> WikiTableEditing:
        """
        converts the given document/file/BytesIO to TableEditing object
        Args:
            document: document to load

        Returns:
            WikiTableEditing
        """
        spreadsheet=SpreadSheet.load(document)
        tableEditing=WikiTableEditing(user=publisher)
        tableEditing.fromSpreadSheet(spreadsheet)
        # fromSpreadSheet not implemented yet
        tableEditing.addLoD(name=OrApi.EVENT_TEMPLATE_NAME, lod=spreadsheet.getTable(OrApi.EVENT_TEMPLATE_NAME))
        tableEditing.addLoD(name=OrApi.SERIES_TEMPLATE_NAME, lod=spreadsheet.getTable(OrApi.SERIES_TEMPLATE_NAME))
        return tableEditing

    def publishTableGenerator(self, tableEditing:WikiTableEditing, userWikiSessionCookie:str=None) -> Generator:
        """
        Uses the given table to update the wikipages corresponding to the lod records.
        Args:
            tableEditing: entity records which are used to update the wiki

        Returns:
            yields the progress of the update

        Raises:
        Unauthorized if the user is not logged into the wiki or does not have the required rights to edit pages
        """
        if self.authUpdates:
            wikiUserInfo=WikiUserInfo.fromWiki(self.wikiUrl, headers={"Cookie":userWikiSessionCookie})
            if not wikiUserInfo.isVerified():
                raise Unauthorized("To update the wikipages you need to be logged into the wiki and have the necessary rights.")
        self.normalizeEntityProperties(tableEditing, reverse=True)
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId, targetWikiId=self.wikiId)
        for entityType, entities in tableEditing.lods.items():
            if isinstance(entities, list):
                for entity in entities:
                    self.normalizePropsForWiki(entity)
                    if isinstance(entity, dict):
                        entity = copy.deepcopy(entity)
                        pageTitle=entity.get('pageTitle')
                        del entity["pageTitle"]
                        # ToDo: Limit entity properties to existing entity properties
                        yield f"Updating {pageTitle} ..."
                        wikiFile=wikiFileManager.getWikiFileFromWiki(pageTitle)
                        wikiFile.updateTemplate(template_name=entityType, args=entity, prettify=True, overwrite=True)
                        wikiFile.pushToWiki(f"Updated through orapi")
                        yield "✅<br>"

    def normalizePropsForWiki(self, entity:dict):
        for key, value in entity.items():
            if isinstance(value, datetime):
                if value.hour == 0 and value.minute == 0:
                    entity[key] = value.date()
            elif isinstance(value, float):
                if (value).is_integer():
                    entity[key]=int(value)


    @property
    def wikiUrl(self):
        wikiUser = WikiUser.ofWikiId(self.wikiId)
        return wikiUser.getWikiUrl()

    def addPageHistoryProperties(self, tableEditing:WikiTableEditing):
        """
        Adds or updates the properties pageCreator and pageEditor
        Args:
            tableEditing: table to be edited
        """
        for name, lods in tableEditing.lods.items():
            if lods is None:
                continue
            for record in lods:
                if isinstance(record, dict):
                    pageTitle=record.get("pageTitle")
                    pageCreator = PageHistory(pageTitle, self.wikiUrl).getPageOwner()
                    if pageCreator is None:
                        pageCreator = tableEditing.user.name
                    record["pageCreator"]=pageCreator
                    if tableEditing.user.hasName():
                        record["pageEditor"]=tableEditing.user.name

    def fetchEntityPropertiesFromMarkup(self, tableEditing:WikiTableEditing):
        """
        Fetches for each entity in the lod the entity properties from the page markup
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods

        Returns:
            Nothing
        """
        extractedLods={}
        wikiFiles={}
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId)
        for name, lods in tableEditing.lods.items():
            extractedEntites=[]
            for lod in lods:
                if isinstance(lod, dict):
                    pageTitle=lod.get("pageTitle")
                    wikiFile=wikiFileManager.getWikiFileFromWiki(pageTitle)
                    wikiFiles[pageTitle]=wikiFile
                    wikiSONs=wikiFile.extractTemplate(templateName=name)
                    if len(wikiSONs) == 1:
                        if len(wikiSONs)>1: print(f"{pageTitle} has multiple definitions of the {name}")
                        extractedEntites.append({**lod, **wikiSONs[0]})
                    else:
                        print(f"{pageTitle} did not have the WikiSON for {name}")
            extractedLods[name]=extractedEntites
        tableEditing.lods=extractedLods
        tableEditing.wikiFiles=wikiFiles

    def completeProperties(self, tableEditing:WikiTableEditing, restrict:bool=False):
        """
        completes the entities in the tabelEditing lods by adding missing properties (if missing set value None)
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods
            restrict(bool): If True limit the entity properties to the proerties defined in the corresponding Entity samples

        Returns:
            Nothing
        """
        lot = [  # (templateName, templateParamMap)
            (OREvent.templateName, {"pageTitle": "pageTitle", **OREvent.getTemplateParamLookup()}),
            (OREventSeries.templateName, {"pageTitle": "pageTitle", **OREventSeries.getTemplateParamLookup()})
        ]
        for templateName, templateParams in lot:
            entityRecords=tableEditing.lods.get(templateName)
            if entityRecords:
                LOD.setNone4List(entityRecords, templateParams)
                if restrict:
                    tableEditing.lods[templateName]=LOD.filterFields(entityRecords, templateParams, reverse=True)

    def normalizeEntityProperties(self, tableEditing:WikiTableEditing, reverse:bool=False):
        """
        Normalizes the entiryRecord property names
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods
            reverse: If False the property names will be normalized. Otherwise, the property names will be changed to the template param names

        Returns:
            Nothing
        """
        lot = [  # (templateName, templateParamMap)
            (OREvent.templateName, {"pageTitle":"pageTitle", **OREvent.getTemplateParamLookup()}),
            (OREventSeries.templateName, {"pageTitle":"pageTitle", **OREventSeries.getTemplateParamLookup()})
        ]
        for templateName, templateParamMap in lot:
            entityRecords = tableEditing.lods.get(templateName)
            if entityRecords:
                if reverse:
                    templateParamMap={v:k for k,v in templateParamMap.items()}
                tableEditing.lods[templateName]=OrApi.updateKeys(entityRecords, templateParamMap)

    def getHtmlTables(self, tableEditing:WikiTableEditing):
        """
        Converts the given tables into a html table
        Args:
            tableEditing:

        Returns:

        """
        lods=copy.deepcopy(tableEditing.lods)
        valueMap = {
            "pageTitle": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}", title=value),
            "Homepage": lambda value: Link(url=value, title=value),
            "wikidataId": lambda value: Link(url=f"https://www.wikidata.org/wiki/{value}", title=value),
            "WikiCfpSeries": lambda value: Link(url=f"http://www.wikicfp.com/cfp/program?id={value}", title=value),
            "wikicfpId": lambda value: Link(url=f"http://www.wikicfp.com/cfp/servlet/event.showcfp?eventid={value}",title=value),
            "DblpConferenceId": lambda value: Link(url=f"https://dblp2.uni-trier.de/db/conf/{value}", title=value),
            "DblpSeries": lambda value: Link(url=f"https://dblp.org/db/conf/{value}/index.html", title=value),
            "Series": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}",title=value),
            "TibKatId": lambda value: Link(url=f"https://www.tib.eu/en/search/id/TIBKAT:{value}", title=value),
            "Logo": lambda value: Image(url=f"{self.wikiUrl}/index.php?title=Special:Redirect/file/File:{value}",alt=value) if value else value
        }
        seriesLod = self.convertLodValues(lods[OrApi.SERIES_TEMPLATE_NAME], valueMap)
        eventLod = self.convertLodValues(lods[OrApi.EVENT_TEMPLATE_NAME], valueMap)
        seriesTable = LodTable(seriesLod, headers={v: v for v in LOD.getFields(seriesLod)},name="Event series")
        eventsTable = LodTable(eventLod, headers={v: v for v in LOD.getFields(eventLod)},
                               name="Events", isDatatable=True)
        return seriesTable, eventsTable

    @staticmethod
    def convertLodValues(lod: list, valueMap: dict):
        """
        Converts the lod values based on the given map of key to convert functions
        ToDo: Migrate to LOD in pyLODStorage
        Args:
            lod: list of dicts to convert
            valueMap: map of lod keys to convert functions

        Returns:
            lod
        """
        if lod is None:
            return lod
        for record in lod.copy():

            for key, function in valueMap.items():
                if key in record:
                    record[key] = function(record[key])
        return lod

    @staticmethod
    def updateKeys(lod:list, map:dict, strictMapping:bool=False):
        """
        Updates the keys of the given LoD with the new keys from the given map
        ToDo: migrate to LOD in pyLODStorage
        Args:
            lod: List of dicts to be updated
            map: map from old key to new key
            strictMapping: If True key value pairs where the key is not mentioned in the map will be removed

        Returns:
            List of Dicts with the updated keys
        """
        return [{map.get(k,k):v for k,v in d.items() if not strictMapping or k in map}for d in lod]


class OrMigrateWrapper(object):
    """
    Wrapper for ormigrate to use the fixers as Enhancement callbacks
    """

    @staticmethod
    def getOrMigrateFixers()->dict:
        """
        Returns dict of all ormigrate fixers that have a fixer class
        """
        fixers={}
        manager = PageFixerManager(pageFixerClassList=[f for f in PageFixerManager.getAllFixers() if f.__name__ != 'CountryFixer'], wikiFileManager=None)
        fixersWithFixFn = {k:f for k,f in manager.pageFixers.items() if PageFixerManager.hasFixer(f)}
        for name, fixer in fixersWithFixFn.items():
            fixers[name]=partial(OrMigrateWrapper._applyFixer, fixer=fixer)
        return fixers

    @staticmethod
    def _getEntityRatingForRecord(record: dict, wikiFile:WikiFile) -> EntityRating:
        '''
        creates EntityRating for given record
        '''
        entity = JSONAble()
        entity.__dict__.update(record)
        smwHandler = SMWEntity(entity=entity, wikiFile=wikiFile)
        setattr(entity, "smwHandler", smwHandler)
        entityRating = EntityRating(entity)

        return entityRating

    @staticmethod
    def _applyFixer(tableEditing:WikiTableEditing, fixer:ORFixer) -> WikiTableEditing:
        """
        Applies the given fixer to the given tableEditing
        Args:
            tableEditing: table to be fixed
            fixer: fixer to be applied

        Returns:
            fixed TableEditing object
        """
        wikiFiles={}
        fixedRecords=[]
        if hasattr(tableEditing, "wikiFiles"):
            wikiFiles=tableEditing.wikiFiles

        for entityType in [OrApi.EVENT_TEMPLATE_NAME, OrApi.SERIES_TEMPLATE_NAME]:
            if entityType not in fixer.worksOn:
                continue
            for eventRecord in tableEditing.lods[OrApi.EVENT_TEMPLATE_NAME]:
                pageTitle=eventRecord.get("pageTitle")
                wikiFile=wikiFiles.get(pageTitle,None)
                entity=OrMigrateWrapper._getEntityRatingForRecord(eventRecord, wikiFile)
                fixer.fix(entity)
                fixedRecords.append(entity.getRecord())
            tableEditing.lods[OrApi.EVENT_TEMPLATE_NAME]=fixedRecords
        return tableEditing

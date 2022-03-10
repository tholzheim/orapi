import copy
import json
from collections.abc import Generator
from datetime import datetime
from functools import partial
from time import sleep
from typing import cast

import requests
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

from orapi.locationService import LocationService
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

    def __init__(self, wikiId:str, targetWikiId:str=None, authUpdates:bool=True, debug:bool=False):
        """

        Args:
            wikiId: id of the wiki
            authUpdates: apply updates to the wiki only if user is authenticated
            debug: print debug output if true
        """
        self.allowedTemplateParams = {
            OREvent.templateName:[*OREvent.getTemplateParamLookup().keys(), "pageCreator", "pageEditor"],
            OREventSeries.templateName: [*OREventSeries.getTemplateParamLookup().keys(), "pageCreator", "pageEditor"]
        }
        self.wikiId=wikiId
        self.targetWikiId=targetWikiId
        self.authUpdates=authUpdates
        self.debug=debug
        self.eventTemplateProps={"pageTitle":"pageTitle", **{value:key for key, value in OREvent.getTemplateParamLookup().items()}}
        self.seriesTemplateProps = {"pageTitle":"pageTitle", **{value:key for key, value in OREventSeries.getTemplateParamLookup().items()}}
        self.optionalEnhancers={
            #**OrMigrateWrapper.getOrMigrateFixers(wikiId)
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

    def getListOfDblpEventSeries(self) -> list:
        """
        Retries a list of all dblp event series
        """
        query = """
        {{#ask: [[IsA::Event series]][[DblpSeries::+]] || [[Has_Bibliography::+]] || [[WikiCfpSeries::+]]
        | mainlabel=pageTitle
        |?title=title
        |?Homepage=homepage
        |?Has_Bibliography=Has Bibliography
        |?DblpSeries=DblpSeries
        |?Wikidataid=wikidataId
        |?WikiCfpSeries=WikiCfpSeries
        |format=table
        |limit=200
        }}
        """
        tableQuery = TableQuery(debug=self.debug)
        tableQuery.fromAskQueries(wikiId=self.wikiId, askQueries=[{"name":"List of DBLPEventSeries", "ask":query}])
        return list(tableQuery.tableEditing.lods.values())[0]

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
        tableEditing.addEnhancer(self.sanitizeEntityPropertyValues)

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
            sleep(0.05)  # slow down progress display (also workaround for delayed msg progress display )
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

    def uploadLodTableGenerator(self,
                                tableEditing:WikiTableEditing,
                                headers=None,
                                ensureLocationsExits: bool = True,
                                isDryRun:bool=False) -> Generator:
        """
        Uses the given table to update the wikipages corresponding to the lod records.
        Args:
            tableEditing: entity records which are used to update the wiki
            isDryRun(bool): Only if False the pages in the wiki are updated.
            ensureLocationsExits(bool): If true ensure that for the locations of the events a corresponding page exists

        Returns:
            yields the progress of the update

        Raises:
        Unauthorized if the user is not logged into the wiki or does not have the required rights to edit pages
        """
        if self.authUpdates:
            wikiUserInfo=WikiUserInfo.fromWiki(self.wikiUrl, headers=headers)
            if not wikiUserInfo.isVerified():
                raise Unauthorized("To update the wikipages you need to be logged into the wiki and have the necessary rights.")
        if isDryRun:
            yield "Dry Run!!!<br>"
        self.normalizeEntityProperties(tableEditing, reverse=True)
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId, targetWikiId=self.wikiId)
        ts = wikiFileManager.wikiPush.toWiki.site.site
        targetWikiUrl = ts["server"] + ts["scriptpath"]
        locations = set()
        for entityType, entities in tableEditing.lods.items():
            if isinstance(entities, list):
                for entity in entities:
                    self.normalizePropsForWiki(entity)
                    if isinstance(entity, dict):
                        pageTitle = entity.get('pageTitle')
                        entity = {key:value for key, value in entity.items() if key in self.allowedTemplateParams.get(entityType, [])}
                        wikiFile = wikiFileManager.getWikiFileFromWiki(pageTitle)
                        yield f"Updating {self.getPageLink(targetWikiUrl, pageTitle, exists=wikiFile.wikiText)} ..."

                        wikiFile.updateTemplate(template_name=entityType, args=entity, prettify=True, overwrite=True)
                        if not isDryRun:
                            wikiFile.pushToWiki(f"Updated through orapi")
                        else:
                            yield "Dryrun! (not updated)"
                        yield "✅<br>"
                        for locationType in ["Country", "Region", "State", "City"]:
                            locations.add(entity.get(locationType, None))
        if ensureLocationsExits:
            yield from self.ensureLocationExists(locations, isDryRun=isDryRun)
        yield "Completed Upload!"

    def validate(self, tableEditing:WikiTableEditing, validationServices:dict):
        """
        Args:
            tableEditing:

        Returns:

        """
        self.normalizeEntityProperties(tableEditing)
        validationResult = {}
        isValid = True
        for validationService, url in validationServices.items():
            res = requests.post(url, json=json.dumps(tableEditing.lods))
            lods = res.json()
            for entityType, entityRecords in lods.items():
                if not entityRecords:
                    continue
                for entityName, evr in entityRecords.items():
                    if not entityRecords:
                        continue
                    if entityType not in validationResult:
                        validationResult[entityType]={}
                    if entityName not in validationResult[entityType]:
                        validationResult[entityType][entityName]={}
                    validationResult[entityType][entityName][validationService]=evr
                    isValid = isValid and evr.get("result", False)
        return isValid, validationResult

    def normalizePropsForWiki(self, entity:dict):
        for key, value in entity.items():
            if isinstance(value, datetime):
                if value.hour == 0 and value.minute == 0:
                    entity[key] = value.date()
            elif isinstance(value, float):
                if (value).is_integer():
                    entity[key]=int(value)

    def publishSeries(self, seriesAcronym:str, publisher:str, ensureLocationsExits:bool=True, isDryRun:bool=False) -> Generator:
        """
        Publishes the pages belonging to the given series from the source wiki to the defined target wiki

        Args:
            seriesAcronym(str): name of the series to be published
            publisher(str): name of the publisher
            ensureLocationsExits(bool): If True the location pages mentioned in the entity records will also be pushed to the target wiki
            isDryRun(bool): If True the pages will not be pushed to the target wiki

        Returns:
            yields progress messages of the publishing process
        """
        wikiFileManager = WikiFileManager(sourceWikiId=self.wikiId, targetWikiId=self.targetWikiId)
        tableEditing = self.getSeriesTableEditing(seriesAcronym)
        ts = wikiFileManager.wikiPush.toWiki.site.site
        targetWikiUrl = ts["server"] + ts["scriptpath"]
        locations = set()
        for entityType, lod in tableEditing.lods.items():
            for record in lod:
                entityName = record.get("pageTitle")
                pageHistory = PageHistory(entityName, targetWikiUrl)
                yield f"Publishing: {self.getPageLink(targetWikiUrl, entityName, exists=pageHistory.exists())} ..."
                pageCreator = pageHistory.getPageOwner()
                wikiFile = wikiFileManager.getWikiFileFromWiki(entityName)
                record = wikiFile.extractTemplate(entityType)[0]
                for locationType in ["Country", "Region", "State", "City"]:
                    locations.add(record.get(locationType, None))
                args = {
                    "pageCreator": pageCreator,
                    "pageEditor": publisher
                }
                wikiFile.updateTemplate(entityType, overwrite=True, args=args, prettify=True)
                if not isDryRun:
                    wikiFile.pushToWiki(f"Published changes from {self.wikiId} by {publisher}")
                else:
                    yield "Dryrun! (not updated)"
                yield "✅<br>"
        if ensureLocationsExits:
            yield from self.ensureLocationExists(locations, isDryRun=isDryRun)
        yield "Completed Publish"

    def getPageLink(self, wikiUrl:str, pageTitle:str, exists:bool=True):
        """

        Args:
            wikiUrl: url of the wiki
            pageTitle: pageTitle
            exists: True if the page exists. Otherwise the link will be rendered in red

        Returns:
        Link to the page
        """
        style = None
        if not exists:
            style = "color: red"
        return Link(f'{wikiUrl}/index.php?title={pageTitle}', pageTitle, style=style)

    def ensureLocationExists(self, locations:set, isDryRun:bool=False) -> Generator:
        """
        Ensures that for the given list of locations the corresponding location page exists in the wiki
        Args:
            locations(list): list of locations for which a corresponding location page should exist

        Returns:
            yields progress
        """
        wikiFileManager = WikiFileManager(sourceWikiId=self.targetWikiId, targetWikiId=self.targetWikiId)
        ts = wikiFileManager.wikiPush.toWiki.site.site
        targetWikiUrl = ts["server"] + ts["scriptpath"]
        yield f"<br>Ensure location pages exist for published series:<br>"
        locationService = LocationService()
        for location in locations:
            if location is None:
                continue
            page = wikiFileManager.wikiPush.toWiki.getPage(location)
            if page.exists:
                yield f"Already exists: {self.getPageLink(targetWikiUrl, location, page.exists)} ✅<br>"
            else:
                locationRecord = locationService.getLocationByOrName(location)
                if locationRecord == {}:
                    yield f"Invalid location: {location}<br>"
                else:
                    yield f"Publishing: {self.getPageLink(targetWikiUrl, location, page.exists)} ..."

                    wikiFile = WikiFile(location, wikiFileManager=wikiFileManager, wikiText="")
                    wikiFile.addTemplate("Location", data=locationRecord, prettify=True)
                    if not isDryRun:
                        wikiFile.pushToWiki(f"Pushed from {self.wikiId}")
                    else:
                        yield "Dryrun! (not updated)"
                    yield "✅<br>"


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
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId, login=False)
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
        completes the entities in the tableEditing LoDs by adding missing properties (if missing set value None)
        Args:
            tableEditing: TableEditing with the entities to fetch in the LoDs
            restrict(bool): If True limit the entity properties to the properties defined in the corresponding Entity samples

        Returns:
            Nothing
        """
        lot = [  # (templateName, templateParamMap)
            (OREvent.templateName, ["pageTitle", *OREvent.getTemplateParamLookup().keys()]),
            (OREventSeries.templateName, ["pageTitle", *OREventSeries.getTemplateParamLookup().keys()])
        ]
        for templateName, templateParams in lot:
            entityRecords=tableEditing.lods.get(templateName)
            if entityRecords:
                LOD.setNone4List(entityRecords, templateParams)
                if restrict:
                    tableEditing.lods[templateName]=LOD.filterFields(entityRecords, templateParams, reverse=True)
            else:
                # add one blank record for this entity type see issue #29
                tableEditing.lods[templateName] = [{p:None for p in templateParams}]

    @staticmethod
    def normalizeEntityProperties(tableEditing:WikiTableEditing, reverse:bool=False):
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

    def sanitizeEntityPropertyValues(self, tableEditing:WikiTableEditing):
        """
        Curates some entity values e.g converting float to int
        Args:
            tableEditing:

        Returns:

        """
        worksOn = [OREvent.templateName, OREventSeries.templateName]
        for name in worksOn:
            entityRecords = tableEditing.lods.get(name)
            for entityRecord in entityRecords:
                for key, value in entityRecord.items():
                    if isinstance(value, float) and value.is_integer():
                        entityRecords[key]=int(value)

    def apiEnhancer(self, tableEditing:WikiTableEditing, apiUrl:str):
        """
        enhances the given TableEditing by calling the given apiUrl
        Args:
            tableEditing: table to enhance
            apiUrl: RESTful API to call

        Returns:

        """
        qres = requests.post(apiUrl, json=tableEditing.lods)
        lods = qres.json()
        tableEditing.lods=lods

    def getHtmlTables(self, tableEditing:WikiTableEditing):
        """
        Converts the given tables into a html table
        Args:
            tableEditing:

        Returns:

        """
        lods=copy.deepcopy(tableEditing.lods)
        valueMap = self.propertyToLinkMap()
        seriesLod = self.convertLodValues(lods[OrApi.SERIES_TEMPLATE_NAME], valueMap)
        eventLod = self.convertLodValues(lods[OrApi.EVENT_TEMPLATE_NAME], valueMap)
        seriesTable = LodTable(seriesLod, headers={v: v for v in LOD.getFields(seriesLod)},name="Event series")
        eventPropertyOrder=["pageTitle", "Acronym", "Ordinal", "Year", "City", "Start date", "End date", "Title", "Series", "wikidataId","wikicfpId","DblpConferenceId","TibKatId"]
        eventFields = LOD.getFields(eventLod)
        eventHeaders={**{v:v for v in eventPropertyOrder if v in eventFields}, **{v:v for v in eventFields if v not in eventPropertyOrder}}
        eventsTable = LodTable(eventLod, headers=eventHeaders, name="Events", isDatatable=True)
        return seriesTable, eventsTable

    def getValidationTable(self, validationResult:dict):
        """
        Converts given validation result to html table
        Args:
            validationResult: validation result

        Returns:

        """
        tables = []
        for entityType, records in validationResult.items():
            entityValidations = []
            for entityName, validation in records.items():
                entityValidation = {"pageTitle":entityName}
                for valName, valRes in validation.items():
                    isValid = valRes.get("result")
                    errors = ','.join(valRes.get("errors"))
                    if isValid:
                        entityValidation[valName] = "✅"
                    else:
                        entityValidation[valName] = f"<div>❗ <p>{errors}</p> </div>"
                entityValidations.append(entityValidation)
            headers = {"pageTitle":"pageTitle", **{k:k for k in LOD.getFields(entityValidations)}}
            table = LodTable(entityValidations, headers=headers, name=entityType, isDatatable=True)
            tables.append(table)
        return " ".join([str(table) for table in tables])




    def propertyToLinkMap(self) -> dict:
        """
        Returns a mapping to convert a property to the corresponding link
        """
        map={
            "pageTitle": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}", title=value),
            "Homepage": lambda value: Link(url=value, title=value),
            "wikidataId": lambda value: Link(url=f"https://www.wikidata.org/wiki/{value}", title=value),
            "WikiCfpSeries": lambda value: Link(url=f"http://www.wikicfp.com/cfp/program?id={value}", title=value),
            "wikicfpId": lambda value: Link(url=f"http://www.wikicfp.com/cfp/servlet/event.showcfp?eventid={value}",title=value),
            "DblpConferenceId": lambda value: Link(url=f"https://dblp2.uni-trier.de/db/conf/{value}", title=value),
            "DblpSeries": lambda value: Link(url=f"https://dblp.org/db/conf/{value}/index.html", title=value),
            "Series": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}",title=value),
            "TibKatId": lambda value: Link(url=f"https://www.tib.eu/en/search/id/TIBKAT:{value}", title=value),
            "Logo": lambda value: Image(url=f"{self.wikiUrl}/index.php?title=Special:Redirect/file/File:{value}",alt=value) if value else value,
            "Ordinal": lambda value: int(value) if isinstance(value, float) and value.is_integer() else value,
            "City": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}", title=value),
            "State": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}", title=value),
            "Country": lambda value: Link(url=f"{self.wikiUrl}/index.php?title={value}", title=value),
            "pageEditor": lambda value: Link(url=f"{self.wikiUrl}/index.php?title=User:{value}", title=value),
            "pageCreator": lambda value: Link(url=f"{self.wikiUrl}/index.php?title=User:{value}", title=value),
        }
        return map

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


class OrApiService:
    """
    Handles OrApi for multiple wikis
    """

    def __init__(self, wikiIds:list=None, authUpdates:bool=True, defaultSourceWiki:str="orclone", debug:bool=False):
        """

        Args:
            wikiIds: wiki ids for wich an ArApi should be provided
            authUpdates: apply updates to the wiki only if user is authenticated
            debug: print debug output if true
        """
        self.debug=debug
        self.defaultSourceWiki=defaultSourceWiki
        self.authUpdates=authUpdates
        self.orapis={}
        self.enhancerURLs = {}
        wikiUserIds = list(WikiUser.getWikiUsers().keys())
        if wikiIds is None:
            self.wikiIds = wikiUserIds
        else:
            self.wikiIds = []
            for wikiId in wikiIds:
                if wikiId in wikiUserIds:
                    self.wikiIds.append(wikiId)
                else:
                    print(f"Wiki id '{wikiId}' is not known")

    def getOrApi(self, wikiId:str, targetWikiId:str=None) -> OrApi:
        """
        Returns the OrApi corresponding to the given wikiId
        Args:
            wikiId: wiki id

        Returns:
            OrApi
        """
        orapi = OrApi(wikiId=wikiId, targetWikiId=targetWikiId, authUpdates=self.authUpdates, debug=self.debug)
        for enhancerName, url in self.enhancerURLs.items():
            orapi.optionalEnhancers[enhancerName] = partial(orapi.apiEnhancer, apiUrl=url)
        return orapi

    def getAvailableWikiChoices(self) -> list:
        return [(wid, wid) for wid in self.wikiIds]

    def addEnhancerURLs(self, enhancerURLs:dict):
        """
        adds the given enhancer urls
        Args:
            enhancerURLs: enhancer nam ena d url pairs
        """
        if enhancerURLs is not None:
            self.enhancerURLs = {**self.enhancerURLs, **enhancerURLs}


class OrMigrateWrapper(object):
    """
    Wrapper for ormigrate to use the fixers as Enhancement callbacks
    """

    @staticmethod
    def getOrMigrateFixers(wikiid:str)->dict:
        """
        Args:
            wikiid(str): id of the wiki

        Returns dict of all ormigrate fixers that have a fixer class
        """
        fixers={}
        wikiFileManager=WikiFileManager(sourceWikiId=wikiid, login=False)
        excludedFixers = ["WikiCfpIdSeriesFixer", "CountryFixer"]
        manager = PageFixerManager(pageFixerClassList=[f for f in PageFixerManager.getAllFixers() if f.__name__ not in excludedFixers], ccID=None, wikiFileManager=wikiFileManager)
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

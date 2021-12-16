from functools import partial

from corpus.datasources.openresearch import OREvent, OREventSeries
from lodstorage.lod import LOD
from lodstorage.query import Query
from spreadsheet.tablequery import TableQuery
from wikifile.wikiFileManager import WikiFileManager

from orapi.tableediting import TableEditing


class OrApi:
    """
    Functions to edit and extract events ans series from openresearch
    """

    EVENT_TEMPLATE_NAME = OREvent.templateName
    SERIES_TEMPLATE_NAME = OREventSeries.templateName

    def __init__(self, wikiId:str):
        self.wikiId=wikiId

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
        tableQuery=TableQuery()
        askQueries=[self.getSeriesQuery(seriesAcronym), self.getEventsOfSeriesQuery(seriesAcronym)]
        tableQuery.fromAskQueries(wikiId=self.wikiId, askQueries=askQueries)
        return tableQuery

    def getSeriesTableEditing(self, seriesAcronym:str):
        """

        Args:
            seriesAcronym: acronym of the series

        Returns:
            TableEditing for the given series
        """
        tableQuery=self.getSeriesTableQuery(seriesAcronym)
        tableEditing=tableQuery.tableEditing
        tableEditing.addEnhancer(self.fetchEntityPropertiesFromMarkup)
        tableEditing.addEnhancer(partial(self.completeProperties, restrict=True))   # ToDo: restriction of exported properties needs to be discussed
        # to apply the fixers from ormigrate we need to normalize the entity property names
        tableEditing.addEnhancer(self.normalizeEntityProperties)

        # apply list of different fixers → conversion to EntityRaing required?

        # ? map property names back to template params so that the user sees which template param is going to be affected or stick to the normalized names?
        tableEditing.addEnhancer(partial(self.normalizeEntityProperties, reverse=True))
        tableEditing.enhance()
        return tableEditing

    def fetchEntityPropertiesFromMarkup(self, tableEditing:TableEditing):
        """
        Fetches for each entity in the lod the entity properties from the page markup
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods

        Returns:
            Nothing
        """
        extractedLods={}
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId)
        for name, lod in tableEditing.lods.items():
            extractedEntites=[]
            if isinstance(lod, dict):
                for pageTitle in lod.keys():
                    wikiFile=wikiFileManager.getWikiFileFromWiki(pageTitle)
                    wikiSONs=wikiFile.extractTemplate(templateName=name)
                    if len(wikiSONs) == 1:
                        if len(wikiSONs)>1: print(f"{pageTitle} has multiple definitions of the {name}")
                        extractedEntites.append(wikiSONs[0])
                    else:
                        print(f"{pageTitle} did not have the WikiSON for {name}")
            extractedLods[name]=extractedEntites
        tableEditing.lods=extractedLods


    def completeProperties(self, tableEditing:TableEditing, restrict:bool=False):
        """
        completes the entities in the tabelEditing lods by adding missing properties (if missing set value None)
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods
            restrict(bool): If True limit the entity properties to the proerties defined in the corresponding Entity samples

        Returns:
            Nothing
        """
        lot=[  # (templateName, templateParams)
            (OREvent.templateName, OREvent.getTemplateParamLookup().keys()),
            (OREventSeries.templateName, OREventSeries.getTemplateParamLookup().keys())
        ]
        for templateName, templateParams in lot:
            entityRecords=tableEditing.lods.get(templateName)
            if entityRecords:
                LOD.setNone4List(entityRecords, templateParams)
                if restrict:
                    tableEditing.lods[templateName]=LOD.filterFields(entityRecords, templateParams, reverse=True)

    def normalizeEntityProperties(self, tableEditing:TableEditing, reverse:bool=False):
        """
        Normalizes the entiryRecord property names
        Args:
            tableEditing: TableEditing with the entites to fetch in the lods
            reverse: If False the property names will be normalized. Otherwise, the property names will be changed to the template param names

        Returns:
            Nothing
        """
        lot = [  # (templateName, templateParamMap)
            (OREvent.templateName, OREvent.getTemplateParamLookup()),
            (OREventSeries.templateName, OREventSeries.getTemplateParamLookup())
        ]
        for templateName, templateParamMap in lot:
            entityRecords = tableEditing.lods.get(templateName)
            if entityRecords:
                if reverse:
                    templateParamMap={v:k for k,v in templateParamMap.items()}
                tableEditing.lods[templateName]=OrApi.updateKeys(entityRecords, templateParamMap)

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

import datetime
from collections import Generator

from corpus.datasources.openresearch import OREvent
from lodstorage.lod import LOD
from onlinespreadsheet.tableediting import TableEditing
from onlinespreadsheet.tablequery import TableQuery
from wikifile.wikiFileManager import WikiFileManager

from orapi.orapiservice import OrApi, WikiTableEditing
from orapi.utils import WikiUserInfo
from tests.basetest import Basetest


class TestOrApi(Basetest):
    """
    tests OrApi
    """

    def setUp(self,debug=False,profile=True):
        super().setUp(debug=debug, profile=profile)
        self.testSeriesAcronym="AAAI"
        self.testUser = WikiUserInfo(**WikiUserInfo.getSamples()[0])
        self.wikiId = getattr(self.getWikiUser("orfixed"), "wikiId")
        self.targWikiId = self.wikiId
        self.orapi=OrApi(wikiId=self.wikiId, targetWikiId=self.targWikiId, authUpdates=False, debug=True)


    def test_getSeriesTableQuery(self):
        """
        tests the creation of a Table query for a series and its events
        """
        tableQuery=self.orapi.getSeriesTableQuery(self.testSeriesAcronym)
        self.assertTrue(isinstance(tableQuery, TableQuery))
        self.assertTrue(len(tableQuery.queries)==2)
        self.assertTrue(len(tableQuery.tableEditing.lods)==2)
        self.assertTrue(OrApi.EVENT_TEMPLATE_NAME in tableQuery.tableEditing.lods)
        self.assertTrue(len(tableQuery.tableEditing.lods.get(OrApi.EVENT_TEMPLATE_NAME)) > 20)

    def test_getSeriesTableEditing(self):
        """
        tests the creation of TableEditing from given acronym and if the enhancers are applied correctly
        """
        if self.inCI():
            return
        enhancers=["DateFixer", "OrdinalFixer"]
        tableEditing=self.orapi.getSeriesTableEditing(self.testSeriesAcronym, enhancers)
        self.assertTrue(isinstance(tableEditing, TableEditing))
        self.assertTrue(len(tableEditing.enhanceCallbacks)>len(enhancers))

    def test_getSeriesTableEnhanceGenerator(self):
        if self.inCI():
            return
        enhancers=["DateFixer", "OrdinalFixer"]
        tableEditing=self.orapi.getSeriesTableEditing(self.testSeriesAcronym, enhancers)
        enhancementGenerator=self.orapi.getSeriesTableEnhanceGenerator(tableEditing)
        self.assertTrue(isinstance(enhancementGenerator, Generator))

    def test_getListOfDblpEventSeries(self):
        """
        tests the retrieval of the dblp event series list
        """
        lod = self.orapi.getListOfDblpEventSeries()
        self.assertGreater(len(lod), 100)
        fields = LOD.getFields(lod)
        for prop in ["pageTitle", "DblpSeries", "WikiCfpSeries", "wikidataId"]:
            self.assertIn(prop, fields)
        pageTitles = {d.get("pageTitle", None) for d in lod}
        self.assertIn("AAAI", pageTitles)

    def test_uploadLodTableGenerator(self):
        """
        tests the upload of an series (as dryRun without updating the actual wiki)
        """
        if self.inCI():
            return
        tableEditing = WikiTableEditing(user=self.testUser)
        tableEditing.lods[OREvent.templateName] = [{"pageTitle":"3DUI 2020", "ordinal":27}]
        generator = self.orapi.uploadLodTableGenerator(tableEditing=tableEditing, isDryRun=True)
        logs = ""
        for log in generator:
            logs += log
        self.assertIn("Updating", logs)
        self.assertIn("3DUI 2020", logs)
        self.assertIn("Dryrun!", logs)


    def test_normalizePropsForWiki(self):
        """
        tests the normalizing of a record
        """
        expectedRecord = {
            "ordinal": 13,
            "startDate": datetime.date(year=2020, month=1, day=1)
        }
        record = {
            "ordinal": 13.0,
            "startDate": datetime.datetime(year=2020, month=1, day=1)
        }
        self.orapi.normalizePropsForWiki(record)
        self.assertDictEqual(expectedRecord, record)

    def test_publishSeries(self):
        """
        tests publishing of a series
        """
        if self.inCI():
            return
        generator = self.orapi.publishSeries(seriesAcronym=self.testSeriesAcronym,
                                             publisher=self.testUser.name,
                                             ensureLocationsExits=True,
                                             isDryRun=True)
        logs = ""
        for log in generator:
            logs += log
        self.assertIn("Publishing:", logs)
        self.assertIn(">AAAI<", logs)
        self.assertIn("AAAI 2020", logs)
        self.assertIn("US/NY/New York City", logs)
        self.assertIn("Dryrun!", logs)


    def test_addPageHistoryProperties(self):
        """
        tests the PageHistory addition
        """
        tableEditing = WikiTableEditing(user=self.testUser)
        tableEditing.lods[OREvent.templateName] = [{"pageTitle": "3DUI 2020"}]
        self.orapi.addPageHistoryProperties(tableEditing=tableEditing)
        updatedRecord = tableEditing.lods[OREvent.templateName][0]
        self.assertEqual(updatedRecord.get("pageCreator"), "Th")
        self.assertEqual(updatedRecord.get("pageEditor"), self.testUser.name)

    def test_fetchEntityPropertiesFromMarkup(self):
        """
        tests fetching of entity properties
        """
        tableEditing = WikiTableEditing(user=self.testUser)
        tableEditing.lods[OREvent.templateName] = [{"pageTitle": "3DUI 2020"}]
        self.orapi.fetchEntityPropertiesFromMarkup(tableEditing=tableEditing)
        fetchedRecords = tableEditing.lods[OREvent.templateName]
        self.assertGreater(len(fetchedRecords[0]), 5)
        expectedFields = ["pageTitle", "Acronym", "Series", "Start date"]
        actualFields = LOD.getFields(fetchedRecords)
        for field in expectedFields:
            self.assertIn(field, actualFields)


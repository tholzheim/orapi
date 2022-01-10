from collections import Generator

from spreadsheet.tableediting import TableEditing
from spreadsheet.tablequery import TableQuery

from orapi.orapiservice import OrApi
from tests.basetest import Basetest


class TestOrApi(Basetest):
    """
    tests OrApi
    """

    def setUp(self,debug=False,profile=True):
        super().setUp(debug=debug, profile=profile)
        self.testSeriesAcronym="AAAI"
        self.orapi=OrApi(wikiId="orfixed", debug=True)


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
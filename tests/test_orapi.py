from orapi.orapiservice import OrApi
from tests.basetest import Basetest


class TestOrApi(Basetest):
    """
    Tests OrApi
    """

    def testSeriesTableQuery(self):
        """
        tests the extraction and conversion to LoD of a series
        """
        orApi=OrApi(wikiId=getattr(self.getWikiUser("orfixed"), "wikiId"))
        tableQuery=orApi.getSeriesTableQuery("AAAI")
        if self.debug:
            print(tableQuery.tableEditing.lods)
        self.assertTrue(len(tableQuery.tableEditing.lods[OrApi.SERIES_TEMPLATE_NAME]) > 0)
        self.assertTrue(len(tableQuery.tableEditing.lods[OrApi.EVENT_TEMPLATE_NAME]) > 15)


    def testSeriesTableEditing(self):
        """
        tests the extraction and conversion to LoD of a series
        """
        orApi=OrApi(wikiId="orfixed")
        tableEditing=orApi.getSeriesTableEditing("AAAI")
        print(tableEditing.lods)
        self.assertTrue(len(tableEditing.lods[OrApi.SERIES_TEMPLATE_NAME]) > 0)
        self.assertTrue(len(tableEditing.lods[OrApi.EVENT_TEMPLATE_NAME]) > 15)
        #ToDo: test enhancements
        #ToDo: pyOnlienSpreadSheetEditing toSpreadSheet() in incomplete
        # excel=tableEditing.toSpreadSheet(SpreadSheetType.EXCEL, "test_AAAI")
        # excel.tables=tableEditing.lods
        # excel.saveToFile(excel.name)


    def test_updateKeys(self):
        """
        tests the updating of lod keys
        """
        map={
            "oldName":"newName",
            "oldAge":"newAge"
        }
        lod=[{"oldName":"Bob", "oldAge":42},{"oldName":"Alice", "oldAge":38}]
        expectedLod=[{"newName":"Bob", "newAge":42},{"newName":"Alice", "newAge":38}]
        actualLod=OrApi.updateKeys(lod,map)
        for i, expectedDict in enumerate(expectedLod):
            self.assertDictEqual(expectedDict, actualLod[i])
from unittest import TestCase
from tempfile import TemporaryDirectory
from datetime import datetime
from orapi.odsDocument import OdsDocument, ExcelDocument


class TestOdsDocument(TestCase):

    def setUp(self) -> None:
        self.tmpDir = TemporaryDirectory(prefix=self.__class__.__name__)
        self.testLoD=[
            {
                "name":"Jon",
                "lastname":"Doe"
            },
            {
                "name":"Bob",
                "lastname":"Doe",
                "modificationDate":datetime.now(),
                "dept":234.23
            },
            {
                "name":"Alice",
                "lastname":"Doe",
                "age":2,
                "url":"http://www.opendocumentformat.org/developers"
            },
        ]

    def tearDown(self) -> None:
        self.tmpDir.cleanup()

    def test_lod2table(self):
        """
        tests the roundtrip of LoD → .ods → LoD
        """
        doc=OdsDocument("Test")
        fileName = f"{self.tmpDir.name}/{doc.filename}"
        doc.addTable(self.testLoD, OdsDocument.lod2Table, name="Persons")
        doc.saveToFile(fileName)
        docReloaded=OdsDocument("TestReloaded")
        docReloaded.loadFromFile(fileName)
        extractedData=docReloaded.getLodFromTable("Persons")
        for i, record in enumerate(self.testLoD):
            for key,expectedValue in record.items():
                self.assertEqual(expectedValue, extractedData[i].get(key))

class TestExcelDocument(TestCase):

    def setUp(self) -> None:
        self.tmpDir = TemporaryDirectory(prefix=self.__class__.__name__)
        self.testLoD=[
            {
                "name":"Jon",
                "lastname":"Doe"
            },
            {
                "name":"Bob",
                "lastname":"Doe",
                "modificationDate":datetime.now().isoformat(' ', 'seconds'),   # the roundtrip adds rounding errors to the microseconds of this property
                "dept":234.23
            },
            {
                "name":"Alice",
                "lastname":"Doe",
                "age":2,
                "url":"http://www.opendocumentformat.org/developers"
            },
        ]

    def tearDown(self) -> None:
        self.tmpDir.cleanup()

    def assertLodEqual(self, expectedLOD, actualLOD):
        """
        Compares the two given list of dicts (LOD) and checks if they are equal.
        Order of the dicts is here relevant for equality.
        Args:
            expectedLOD: expected LOD
            actualLOD: actual LOD
        """
        for i, record in enumerate(expectedLOD):
            for key,expectedValue in record.items():
                self.assertEqual(expectedValue, actualLOD[i].get(key))

    def test_lod2table(self):
        """
        tests the roundtrip of LoD → .xlsx → LoD
        """
        doc=ExcelDocument("Test")
        fileName = f"{self.tmpDir.name}/{doc.filename}"
        doc.addTable("Persons", self.testLoD)
        doc.addTable("Persons2", self.testLoD)
        doc.saveToFile(fileName)
        docReloaded=ExcelDocument("TestReloaded")
        docReloaded.loadFromFile(fileName)
        extractedData=docReloaded.getTable("Persons")
        self.assertLodEqual(self.testLoD, extractedData)

    def test_bufferLoading(self):
        """
        test the buffer loading of xlsx documents
        """
        doc = ExcelDocument("Test")
        doc.addTable("Persons", self.testLoD)
        buffer = doc.toBytesIO()
        docReloaded = ExcelDocument("TestReloaded")
        docReloaded.loadFromFile(buffer)
        extractedData = docReloaded.getTable("Persons")
        self.assertLodEqual(self.testLoD, extractedData)
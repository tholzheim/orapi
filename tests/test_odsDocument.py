from unittest import TestCase
from tempfile import TemporaryDirectory
from datetime import datetime

from orapi.odsDocument import OdsDocument


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

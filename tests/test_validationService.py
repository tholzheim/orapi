from corpus.datasources.openresearch import OREvent
from spreadsheet.tableediting import TableEditing

from orapi.orapiservice import OrApi
from orapi.validationService import HomepageValidator, OrdinalValidator
from tests.basetest import Basetest


class TestValidationService(Basetest):
    """
    tests ValidationService
    """

    def setUp(self,debug=False,profile=True):
        super().setUp(debug=debug, profile=profile)
        self.testSeriesAcronym="AAAI"
        self.wikiId = getattr(self.getWikiUser("orfixed"), "wikiId")
        self.orapi=OrApi(wikiId=self.wikiId, authUpdates=False, debug=True)

    def getSeriesTableEditing(self, acronym:str=None) -> TableEditing:
        """
        Returns the TableEditing for the given series
        Args:
            acronym: series

        Returns:
            TableEditing
        """
        if acronym is None:
            acronym = self.testSeriesAcronym
        tableEditing = self.orapi.getSeriesTableEditing(acronym)
        self.orapi.fetchEntityPropertiesFromMarkup(tableEditing)
        self.orapi.normalizeEntityProperties(tableEditing)
        return tableEditing

    def test_validateHomepage(self):
        """
        tests validating the homepage of series and events
        """
        tableEditing = self.getSeriesTableEditing()
        validationResult = HomepageValidator.validate(tableEditing)
        self.assertIn(OREvent.templateName, validationResult)
        validatedEventUrls = [e['result'] for e in validationResult[OREvent.templateName].values()]
        self.assertGreater(len([x for x in validatedEventUrls if x]), 20)

    def test_validateUrl(self):
        """
        tests validating a url
        """
        testMatrix = [
            {
                "url": "www.aaai.org/Conferences/AAAI-22/",
                "mustContain": "AAAI",
                "expectedResult": (False, ["Url not in valid format", "Site not available", "(but archived)"])
            },
            {
                "url":"https://aaai.org/Conferences/AAAI-22/",
                "mustContain":"AAAI",
                "expectedResult": (True, [])
            },
            {
                "url": "https://aaai.org/Conferences/AAAI-22/",
                "mustContain": "3DUI",
                "expectedResult": (False, ["Expected content not found"])
            },
            {
                "url": "https://www.2cd3684c-0566-4fcc-a64e-cbf4747cd0f2.org/",
                "mustContain": "NotAvailable",
                "expectedResult": (False, ["Site not available"])
            }
        ]
        for testRecord in testMatrix:
            isValid, errs = HomepageValidator.validateUrl(testRecord["url"], checkAvailability=True, mustContain=testRecord["mustContain"])
            self.assertEqual(testRecord["expectedResult"][0], isValid, testRecord)
            for errMsg in testRecord["expectedResult"][1]:
                self.assertIn(errMsg, ", ".join(errs))

    def test_validateOrdinalFormat(self):
        """
        tests if the ordinals are valid
        """
        testMatrix = [
            ("1st", False),
            ("1", True),
            ("Second", False),
            (1, True),
            (3.0, True),
            (1.5, False)
        ]
        for ordinal, expRes in testMatrix:
            self.assertEqual(expRes, OrdinalValidator.validateOrdinalFormat(ordinal))

    def test_validateOrdinal(self):
        """
        tests validating the ordinal of a series
        """
        tableEditing = self.getSeriesTableEditing("3DUI")
        validationResult = OrdinalValidator.validate(tableEditing)
        print(validationResult)

    def test_isArchivedUrl(self):
        """
        tests isArchivedUrl
        """
        if self.inCI():
            return
        url = "www.aaai.org/Conferences/AAAI-22/"
        self.assertTrue(HomepageValidator.isArchivedUrl(url))
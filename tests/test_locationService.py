from corpus.datasources.openresearch import OREvent
from flask import url_for
from orapi.locationService import LocationService
from tests.basetest import Basetest
from tests.test_webserver import TestWebServer


class TestLocationServiceBlueprint(TestWebServer):

    def setUp(self,debug=False,profile=True):
        super(TestLocationServiceBlueprint, self).setUp(debug=debug, profile=profile)

    def test_enhanceLocation(self):
        """
        tests enhancing the location
        """
        lods = {
            OREvent.templateName:[
                {"acronym": "AAAI 2020", "city":"Los Angeles"},
                {"acronym": "AAAI 2021", "city": "Boston", "country": "USA"},
            ]
        }
        resp = self.client.post(url_for("location.enhanceLocations"), json=lods)
        actualLods = resp.json
        expected = {
                "AAAI 2020": {"acronym": "AAAI 2020", "city":"US/CA/Los Angeles", "region":"US/CA", "country": "US", 'countryWikidataId': 'Q30', 'cityWikidataId': 'Q65', 'regionWikidataId': 'Q99'},
                "AAAI 2021": {"acronym": "AAAI 2021", "city": "US/MA/Boston", "region":"US/MA", "country": "US", 'countryWikidataId': 'Q30', 'cityWikidataId': 'Q100', 'regionWikidataId': 'Q771'}
        }
        for record in actualLods[OREvent.templateName]:
            self.assertDictEqual(expected[record['acronym']], record)

    def test_locations(self):
        path = url_for("location.getCity", country="US", region="CA", city="Los Angeles")
        resp = self.client.get(path)
        record = resp.json
        self.assertIn("wikidataid", record)
        self.assertEqual("Q65", record['wikidataid'])
        # test reduced response
        record_reduced = self.client.get(path, query_string={"reduce":True}).json
        expectedLocation={
            "coordinates":"34.05223, -118.24368",
            "level":5,
            "name":"Los Angeles",
            "partOf":"US/CA",
            "wikidataid":"Q65",
            "locationKind":"City"
        }
        self.assertDictEqual(expectedLocation, record_reduced)


class TestLocationService(Basetest):

    def setUp(self,debug=False,profile=True):
        super(TestLocationService, self).setUp(debug, profile)
        self.locationService = LocationService()

    def test_getCity(self):
        """
        tests getCity
        """
        res = self.locationService.getCity("US", "CA", "Los Angeles")
        self.assertIn("wikidataid", res)
        self.assertEqual(res["wikidataid"], "Q65")

    def test_getRegion(self):
        """
        tests getRegion
        """
        res = self.locationService.getRegion("US", "CA")
        self.assertIn("wikidataid", res)
        self.assertEqual(res["wikidataid"], "Q99")

    def test_getCountry(self):
        """
        tests getCountry
        """
        res = self.locationService.getCountry("US")
        self.assertIn("wikidataid", res)
        self.assertEqual(res["wikidataid"], "Q30")


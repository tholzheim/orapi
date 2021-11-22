import json
from unittest import TestCase

import requests
from lodstorage.csv import CSV


class TestWebServer(TestCase):
    """Test the WebServers RESTful interface"""

    def setUp(self) -> None:
        #ToDo: Start server
        pass

    def test_get_events_of_series(self):
        """tests downloading a csv file"""
        #self.fail()
        #ToDo: download csv (should not be protected)

    def test_verifyUser(self):
        """tests veriying a wiki user"""
        #self.fail()
        #ToDo: Test unregistered user

    def test_completeEventAndSeriesDownload(self):
        """
        tests the downloading of the complete series and events in different formats
        """
        urlSeries = "http://localhost:8558/api/series"
        urlEvents = "http://localhost:8558/api/events"
        testMatrix={
            "json":{
                "responseTransform":lambda response: response.json()
            },
            "jsonOverParameter": {
                "params":{"format":"json"},
                "responseTransform": lambda response: response.json()
            },
            "csv": {
                "headers": {"Accept":"text/csv"},
                "responseTransform": lambda response: CSV.fromCSV(response.text)
            },
            "csvOverParameter": {
                "params":{"format":"csv"},
                "responseTransform": lambda response: CSV.fromCSV(response.text)
            }
        }
        # test series
        for testVariant in testMatrix.values():
            response = requests.request("GET", urlSeries,
                                        headers=testVariant.get("headers"),
                                        params=testVariant.get("params"))
            series=testVariant.get("responseTransform")(response)
            self.assertTrue(len(series) > 1000)
            self.assertTrue("pageTitle" in series[0])
        # test events
        for testVariant in testMatrix.values():
            response = requests.request("GET", urlEvents,
                                        headers=testVariant.get("headers"),
                                        params=testVariant.get("params"))
            series = testVariant.get("responseTransform")(response)
            self.assertTrue(len(series) > 9000)
            self.assertTrue("pageTitle" in series[0])

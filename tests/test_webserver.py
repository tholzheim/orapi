import requests
from lodstorage.csv import CSV

from tests.basetest import Basetest
import warnings
from orapi.webserver import WebServer

class TestWebServer(Basetest):
    """Test the WebServers RESTful interface"""
    
    @staticmethod
    def getApp():
        warnings.simplefilter("ignore", ResourceWarning)
        ws=WebServer()
        app=ws.app
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        #hostname=socket.getfqdn()
        #app.config['SERVER_NAME'] = "http://"+hostname
        app.config['DEBUG'] = False
        client = app.test_client()
        return ws, app,client
    
    def setUp(self) -> None:
        Basetest.setUp(self)
        self.ws,self.app, self.client=TestWebServer.getApp()
       
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
        if self.inCI():
            return
        urlSeries = self.ws.basedUrl("/api/series")
        urlEvents = self.ws.basedUrl("/api/events")
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
            
            response = self.client.get(path=urlSeries,
                                        headers=testVariant.get("headers"),
                                        query_string=testVariant.get("params"))
            series=testVariant.get("responseTransform")(response)
            self.assertTrue(len(series) > 1000)
            self.assertTrue("pageTitle" in series[0])
        # test events
        for testVariant in testMatrix.values():
            response = self.client.get(path=urlEvents,
                                        headers=testVariant.get("headers"),
                                        query_string=testVariant.get("params"))
            series = testVariant.get("responseTransform")(response)
            self.assertTrue(len(series) > 9000)
            self.assertTrue("pageTitle" in series[0])

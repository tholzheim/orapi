import socket
from typing import List

import requests
from flask import url_for
from lodstorage.csv import CSV

from orapi.orapiservice import OrApiService
from tests.basetest import Basetest
import warnings
from orapi.webserver import WebServer

class TestWebServer(Basetest):
    """Test the WebServers RESTful interface"""
    
    @staticmethod
    def getApp(wikiIds:List[str], auth:bool=False):
        warnings.simplefilter("ignore", ResourceWarning)
        ws=WebServer()
        orapiService = OrApiService(wikiIds=wikiIds, defaultSourceWiki=wikiIds[0], authUpdates=auth)
        ws.init(orapiService)
        app=ws.app
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        hostname=socket.getfqdn()
        app.config['SERVER_NAME'] = hostname
        app.config['DEBUG'] = False
        client = app.test_client()
        return ws, app,client

    def setUp(self, **kwargs) -> None:
        Basetest.setUp(self, **kwargs)
        self.testWikiIds=["orfixed"]
        self.ws,self.app, self.client = TestWebServer.getApp(self.testWikiIds, auth=False)
        self.context = self.app.test_request_context()
        self.context.push()

    def tearDown(self):
        Basetest.tearDown(self)
        self.context.pop()

    def test_get_events_of_series(self):
        """tests downloading a csv file"""
        #self.fail()
        #ToDo: download csv (should not be protected)

    def test_verifyUser(self):
        """tests veriying a wiki user"""
        #self.fail()
        #ToDo: Test unregistered user

    def test_getListOfDblpSeries(self):
        """
        tests availability of list of dblp series
        """
        url=url_for("getListOfDblpSeries", _external=False)
        res = self.client.get(url)
        page = res.data.decode()
        self.assertIn("List of DBLPEventSeries", page)
        self.assertIn("AAAI", page)

    def test_updateSeries(self):
        """
        tests updateSeries route
        """
        url = url_for("updateSeries", _external=False)
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)
        self.assertIn("Upload", res.data.decode())

        # test unauthorised request
        ws, app, client = TestWebServer.getApp(self.testWikiIds, auth=True)
        with app.app_context():
            res = client.get(url)
            self.assertEqual(res.status_code, 200)   # Only the upload is protected not viewing the form
            self.assertIn("Upload", res.data.decode())

    def test_home(self):
        """
        tests home route
        """
        url = url_for("home", _external=False)
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)

    def test_publishSeries(self):
        """
        tests publishSeries route
        """
        url = url_for("publishSeries", series="3DUI", _external=False) + "?source=orfixed"
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)
        self.assertIn("Upload", res.data.decode())

        # test unauthorised request
        ws, app, client = TestWebServer.getApp(self.testWikiIds, auth=True)
        with app.app_context():
            res = client.get(url)
            self.assertEqual(res.status_code, 200)  # Only the upload is protected not viewing the form
            self.assertIn(" You need to be logged into the wiki to publish a series", res.data.decode())

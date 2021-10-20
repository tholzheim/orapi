import json
import os
from functools import partial
from os.path import expanduser
from typing import Optional, List

import requests
from SPARQLWrapper import CSV
from corpus.datasources.openresearch import OR
from fb4.app import AppWrap
from flask import request, flash, send_file
from wikibot.wikiuser import WikiUser
from wikifile.wikiFileManager import WikiFileManager
from corpus.lookup import CorpusLookup
from tempfile import TemporaryDirectory


class WebServer(AppWrap):
    """
    RESTful api to access and modify OPENRESAECH data
    """

    def __init__(self,wikiId:str, host='0.0.0.0', port=8558, verbose=True, debug=False):
        '''
        constructor

        Args:
            host(str): flask host
            port(int): the port to use for http connections
            debug(bool): True if debugging should be switched on
            verbose(bool): True if verbose logging should be switched on
            dblp(Dblp): preconfigured dblp access (e.g. for mock testing)
        '''
        self.debug = debug
        self.verbose = verbose
        self.wikiUser=WikiUser.ofWikiId(wikiId)
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        template_folder = scriptdir + '/../templates'
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.app_context().push()
        self.targetWiki="http://localhost:8000"
        self.initOpenResearch()
        self.csvFileBacklog=TemporaryDirectory(prefix="orapi_csv_")

        @self.app.route('/')
        def index():
            return self.index()

        @self.app.route('/series/<series>/events',methods=['GET','POST'])
        @self.csrf.exempt
        def handleEventsOfSeries(series:str):
            if request.method == 'GET':
                return self.getEventsOfSeries(series)
            else:
                return self.updateEventsOfSeries(series)

        @self.app.teardown_appcontext
        def teardown():
            self.csvFileBacklog.cleanup()

    @property
    def wikiId(self):
        return getattr(self.wikiUser,'wikiId')

    @property
    def wikiUrl(self):
        return getattr(self.wikiUser,'url')

    def initOpenResearch(self):
        """Inits the OPENRESEARCH data from ConferenceCorpus as loaded datasource"""
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId, debug=self.debug)
        patchEventSource = partial(self.patchEventSource, wikiFileManager=wikiFileManager)
        lookup = CorpusLookup(configure=patchEventSource, debug=self.debug)
        lookup.eventCorpus.addDataSource(OR(wikiId=self.wikiId, via="backup"))
        lookup.load(forceUpdate=False)  # forceUpdate to init the managers from the markup files
        self.orDataSource = lookup.getDataSource("orclone-backup")

    def patchEventSource(self, lookup: CorpusLookup, wikiFileManager: WikiFileManager):
        '''
        patches the EventManager and EventSeriesManager by adding wikiUser and WikiFileManager
        '''
        for lookupId in {"orclone-backup", "or-backup",f"{self.wikiId}-backup"}:  # only from backup since the api is intended to edit the wikimarkup values
            orDataSource = lookup.getDataSource(lookupId)
            if orDataSource is not None:
                if lookupId.endswith("-backup"):
                    orDataSource.eventManager.smwHandler.wikiFileManager = wikiFileManager
                    orDataSource.eventSeriesManager.smwHandler.wikiFileManager = wikiFileManager
                else:
                    orDataSource.eventManager.smwHandler.wikiUser = wikiFileManager.wikiUser
                    orDataSource.eventSeriesManager.smwHandler.wikiUser = wikiFileManager.wikiUser

    def index(self):
        """"""
        return "Hello World"

    def getEventsOfSeries(self, seriesPageTitle:str):
        """Returns the events of the requested series as csv file"""
        csvString = ''
        OREventManager = self.orDataSource.eventManager
        csvString = OREventManager.asCsv(selectorCallback=partial(OREventManager.getEventsInSeries, seriesPageTitle))
        filepath = f"{self.csvFileBacklog}/{seriesPageTitle}.csv"
        CSV.writeFile(csvString, filepath)
        if self.debug:
            print("sending file: ",filepath)
        return send_file(filepath, as_attachment=True, cache_timeout=-1)

    def updateEventsOfSeries(self, seriesPageTitle:str):
        """
        Updates the events of the series with the provided file

        Args:
            seriesPageTitle(str): pageTitle of the series to update

        Returns:
            Outcome of the update procedure (Not clear yet what to display)
        """
        #ToDo: Update entities of the given series
        print("update series")
        wikiUserInfo=WikiUserInfo.fromWiki(self.targetWiki, request)
        if wikiUserInfo.isVerified():
            # apply csv import
            return seriesPageTitle
        else:
            return 'To import data into the wiki you need to be logged in and have editing rights!'


class WikiUserInfo(object):
    """
    Simple class holding information about a wikiuser
    See https://www.mediawiki.org/wiki/API:Userinfo for more information on which data is queried
    """

    def __init__(self, id:int, name:str, rights: List[str]=None, registrationdate:str=None, acceptlang: List[str]=None, **kwargs):
        """

        Args:
            id(int): id of the user
            name(str): name of the user
            rights(list): list of the rights the user has

        """
        self.id = id
        self.name = name
        self.rights = rights
        self.registrationdate = registrationdate
        self.acceptlang = acceptlang

    def isVerified(self) -> bool:
        """
        Returns True if the user is a registered user and has the rights to edit pages
        """
        # List of mediawiki user rights https://www.mediawiki.org/wiki/Manual:User_rights#List_of_permissions
        requiredRights = {'createpage', 'edit'}
        isVerified = requiredRights.issubset(self.rights) and self.registrationdate is not None
        return isVerified

    @staticmethod
    def fromWiki(wikiUrl:str, request:request):
        """Queries the UserInfos for the user of the given request and returns a corresponding WikiUserInfo object
        Args:
            wikiUrl(str): url of the wiki to ask for the user
            request(request): request message of the user

        Returns:
            WikiUserInfo
        """
        response = requests.request(
            method="GET",
            params={'action': 'query',
                    'meta': 'userinfo',
                    'uiprop': 'rights|acceptlang|registrationdate',
                    'format': 'json'},
            url=wikiUrl+ "/api.php",
            headers={key: value for (key, value) in request.headers if key != 'Host'},
            allow_redirects=False)
        res = json.loads(response.text)
        userInfo={}
        if 'query' in res:
            queryRes = res.get('query')
            if 'userinfo' in queryRes:
                userInfo = queryRes.get('userinfo')
        if userInfo and 'id' in userInfo and 'name' in userInfo:
            wikiUserInfo=WikiUserInfo(**userInfo)
            return wikiUserInfo


if __name__ == '__main__':
    # construct the web application
    web=WebServer(wikiId="myor")
    parser = web.getParser(description="dblp conference webservice")
    args = parser.parse_args()
    web.optionalDebug(args)
    #web.init()
    web.run(args)
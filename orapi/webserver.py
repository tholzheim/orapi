import os
import json
import requests
from os import path
from io import BytesIO
from typing import List
from fb4.app import AppWrap
from functools import partial
from flask import request, send_file
from wikibot.wikiuser import WikiUser
from corpus.lookup import CorpusLookup
from corpus.datasources.openresearch import OR
from wikifile.wikiFileManager import WikiFileManager



class WebServer(AppWrap):
    """
    RESTful api to access and modify OPENRESAECH data
    """

    def __init__(self,wikiId:str, wikiTextPath:str, host='0.0.0.0', port=8558, verbose=True, debug=False):
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
        self.wikiTextPath=wikiTextPath
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        template_folder = scriptdir + '/../templates'
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.app_context().push()
        self.targetWiki="http://localhost:8000"
        self.initOpenResearch()

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


    @property
    def wikiId(self):
        return getattr(self.wikiUser,'wikiId')

    @property
    def wikiUrl(self):
        return getattr(self.wikiUser,'url')

    def initOpenResearch(self):
        """Inits the OPENRESEARCH data from ConferenceCorpus as loaded datasource"""
        wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId,wikiTextPath=self.wikiTextPath, debug=self.debug)
        self.orDataSource = OR(wikiId=self.wikiId, via="backup")
        self.orDataSource.eventManager.smwHandler.wikiFileManager = wikiFileManager
        self.orDataSource.eventSeriesManager.smwHandler.wikiFileManager = wikiFileManager
        self.orDataSource.load()

    def index(self):
        """"""
        return "Hello World"

    def getEventsOfSeries(self, seriesPageTitle:str):
        """Returns the events of the requested series as csv file"""
        csvString = ''
        OREventManager = self.orDataSource.eventManager
        csvString = OREventManager.asCsv(selectorCallback=partial(OREventManager.getEventsInSeries, seriesPageTitle))
        buffer = BytesIO()
        buffer.write(csvString.encode())
        buffer.seek(0)
        return send_file(buffer,attachment_filename=f"{seriesPageTitle}.csv", as_attachment=True, mimetype='text/csv')

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
    home=path.expanduser("~")
    web=WebServer(wikiId="myor",wikiTextPath=f"{home}/.or/generated/orfixed")
    parser = web.getParser(description="dblp conference webservice")
    args = parser.parse_args()
    web.optionalDebug(args)
    #web.init()
    web.run(args)
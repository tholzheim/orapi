import os
import json
import requests
from os import path
from io import BytesIO
from typing import List
from fb4.app import AppWrap
from functools import partial
from lodstorage.lod import LOD
from wikibot.wikiuser import WikiUser
from orapi.odsDocument import OdsDocument
from corpus.datasources.openresearch import OR
from wikifile.wikiFileManager import WikiFileManager
from flask import request, send_file, redirect, render_template, flash

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
        template_folder = scriptdir + '/resources/templates'
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.app_context().push()
        self.targetWiki="http://127.0.0.1:8000"
        self.initOpenResearch()

        @self.app.route('/')
        def index():
            return self.index()

        @self.app.route('/series/<series>', methods=['GET', 'POST'])
        @self.csrf.exempt
        def handleEventsOfSeries(series: str):
            if request.method == 'GET':
                return self.getSeries(series)
            else:
                return self.updateSeries(series)

        # @self.app.route('/series/<series>/events',methods=['GET','POST'])
        # @self.csrf.exempt
        # def handleEventsOfSeries(series:str):
        #     if request.method == 'GET':
        #         return self.getEventsOfSeries(series)
        #     else:
        #         return self.updateEventsOfSeries(series)


    @property
    def wikiId(self):
        return getattr(self.wikiUser,'wikiId')

    @property
    def wikiUrl(self):
        return getattr(self.wikiUser,'url')

    def initOpenResearch(self):
        """Inits the OPENRESEARCH data from ConferenceCorpus as loaded datasource"""
        self.wikiFileManager=WikiFileManager(sourceWikiId=self.wikiId,wikiTextPath=self.wikiTextPath,targetWikiId=self.wikiId, debug=self.debug)
        self.orDataSource = OR(wikiId=self.wikiId, via="backup")
        self.orDataSource.eventManager.smwHandler.wikiFileManager = self.wikiFileManager
        self.orDataSource.eventSeriesManager.smwHandler.wikiFileManager = self.wikiFileManager
        self.orDataSource.load()
        for manager in self.orDataSource.eventManager, self.orDataSource.eventSeriesManager:
            for record in manager.getList():
                record.smwHandler.wikiFileManager=manager.wikiFileManager

    def index(self):
        """"""
        return "Hello World"

    def getEventsOfSeries(self, seriesPageTitle:str):
        """Returns the events of the requested series as csv file"""
        csvString = ''
        OREventManager = self.orDataSource.eventManager
        csvString = OREventManager.asCsv(selectorCallback=partial(OREventManager.getEventsInSeries, seriesPageTitle))
        if csvString:
            buffer = BytesIO()
            buffer.write(csvString.encode())
            buffer.seek(0)
            return send_file(buffer,attachment_filename=f"{seriesPageTitle}.csv", as_attachment=True, mimetype='text/csv')
        else:
            flash(f'The Event series {seriesPageTitle} does not have any events to download.', 'info')
            return render_template('errorPage.html', url=request.referrer, title=seriesPageTitle)

    def updateEventsOfSeries(self, seriesPageTitle:str):
        """
        Updates the events of the series with the provided file

        Args:
            seriesPageTitle(str): pageTitle of the series to update

        Returns:
            Outcome of the update procedure (Not clear yet what to display)
        """
        wikiUserInfo=WikiUserInfo("0","Test")#WikiUserInfo.fromWiki(self.targetWiki, request.headers)
        if wikiUserInfo.isVerified() or True:
            self.app.logger.info(f'{wikiUserInfo.name} imported csv')
            # apply csv import
            if request.files:
                if 'csv' in request.files:
                    csv = request.files["csv"]
                    csvString = csv.read().decode('utf-8')
                    if csvString:
                        publishToWiki = lambda entity, **kwargs: entity.smwHandler.pushToWiki(f"csv import by {wikiUserInfo.name} over orapi", overwrite=True, wikiFileManager=self.wikiFileManager) if hasattr(entity.smwHandler, 'pushToWiki') and callable(getattr(entity.smwHandler, 'pushToWiki')) else None
                        self.orDataSource.eventManager.fromCsv(csvString,updateEntitiesCallback=publishToWiki)
                        return redirect(request.referrer, code=302)
                    else:
                        flash('No file is selected', 'info')
                else:
                    flash('File is attached to the POST request but has an incorrect name', 'info')
            else:
                flash('No file is selected', 'info')
        else:
            self.app.logger.info(f'{wikiUserInfo.name} tried to import csv')
            flash('To import data into the wiki you need to be logged in and have editing rights!', 'warning')
        return render_template('errorPage.html', url=request.referrer, title=seriesPageTitle)

    def getSeries(self, series):
        """
        Return the series and its events as OpenDocument Spreadsheet

        Args:
            series: name of the series that should be returned

        Returns:

        """
        try:
            seriesLookup=self.orDataSource.eventSeriesManager.getLookup(self.orDataSource.eventSeriesManager.primaryKey, withDuplicates=True)
            eventSeries=seriesLookup.get(series)
            seriesTemplateProps = ["pageTitle", *self.orDataSource.eventSeriesManager.clazz.getTemplateParamLookup().values()]
            eventSeriesRecords=LOD.filterFields([s.__dict__ for s in eventSeries], fields=seriesTemplateProps, reverse=True)
            doc=OdsDocument(series)
            doc.addTable(eventSeriesRecords, doc.lod2Table, name="Series", headers=seriesTemplateProps)
            if len(eventSeriesRecords)==1:
                seriesAcronym=eventSeriesRecords[0].get("acronym")
                events=self.orDataSource.eventManager.getEventsInSeries(seriesAcronym)
                eventRecords=[event.__dict__ for event in events]
                eventTemplateProps=["pageTitle", *self.orDataSource.eventManager.clazz.getTemplateParamLookup().values()]
                eventRecords=LOD.filterFields(eventRecords, fields=eventTemplateProps, reverse=True)
                doc.addTable(eventRecords, doc.lod2Table,name="Events", headers=eventTemplateProps)
            self.orDataSource.eventManager.getEventsInSeries(series)
            buffer=doc.toBytesIO()
            return send_file(buffer, attachment_filename=doc.filename, as_attachment=True, mimetype='application/vnd.oasis.opendocument.spreadsheet')
        except Exception as e:
            print(e)
            flash(f'Something went wrong the requested documetn could not be generated', 'warning')
            return render_template('errorPage.html', url=request.referrer, title=series)


    def updateSeries(self, seriesPageTitle):
        wikiUserInfo = WikiUserInfo("0", "Test")  # WikiUserInfo.fromWiki(self.targetWiki, request.headers)
        if wikiUserInfo.isVerified() or True:
            self.app.logger.info(f'{wikiUserInfo.name} imported csv')
            # apply csv import
            if request.files:
                if 'csv' in request.files:
                    odsFile = request.files["csv"]
                    doc=OdsDocument(seriesPageTitle)
                    doc.loadFromFile(odsFile)
                    eventsLoD=doc.getLodFromTable("Events")
                    seriesLoD = doc.getLodFromTable("Series")
                    def publishEntity(entity, **kwargs):
                        """
                        Publishes the given entity to its target wiki.
                        Args:
                            entity: OREvent or OREventSeries (or similar event with smwHandler)
                            **kwargs:
                        """
                        smwHandler=entity.smwHandler
                        if hasattr(smwHandler, 'pushToWiki') and callable(getattr(smwHandler, 'pushToWiki')):
                            smwHandler.pushToWiki(f"spreadsheet import by {wikiUserInfo.name} through orapi",
                                                  overwrite=True,
                                                  wikiFileManager=self.wikiFileManager)
                    if eventsLoD:
                        # update events
                        self.orDataSource.eventManager.updateFromLod(eventsLoD, updateEntitiesCallback=publishEntity)
                    if seriesLoD:
                        # update event series
                        self.orDataSource.eventSeriesManager.updateFromLod(seriesLoD, updateEntitiesCallback=publishEntity)
                    return redirect(request.referrer, code=302)
                else:
                    flash('File is attached to the POST request but has an incorrect name', 'info')
            else:
                flash('No file is selected', 'info')
        else:
            self.app.logger.info(f'{wikiUserInfo.name} tried to import csv')
            flash('To import data into the wiki you need to be logged in and have editing rights!', 'warning')
        return render_template('errorPage.html', url=request.referrer, title=seriesPageTitle)


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
        self.rights = rights if rights else []
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
    def fromWiki(wikiUrl:str, headers):
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
            headers={key: value for (key, value) in headers if key != 'Host'},
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
    web=WebServer(wikiId="wikirenderTest",wikiTextPath=f"{home}/.or/generated/orfixed")
    parser = web.getParser(description="dblp conference webservice")
    args = parser.parse_args()
    web.optionalDebug(args)
    #web.init()
    web.run(args)
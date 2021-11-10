import itertools
import os
import json
import sys
import uuid

import requests
from os import path
from typing import List
from corpus.event import EventBaseManager
from fb4.app import AppWrap
from fb4.sse_bp import SSE_BluePrint
from fb4.widgets import Widget, LodTable, DropZoneField, ButtonField, Link
from flask_wtf import FlaskForm
from lodstorage.lod import LOD
from wikibot.wikiuser import WikiUser
from wtforms import StringField, SelectField, MultipleFileField, SubmitField, FileField, validators, Field
from orapi.odsDocument import OdsDocument, ExcelDocument
from corpus.datasources.openresearch import OR
from wikifile.wikiFileManager import WikiFileManager
from flask import request, send_file, redirect, render_template, flash, jsonify, Response, url_for


class WebServer(AppWrap):
    """
    RESTful api to access and modify OPENRESAECH data
    """

    def __init__(self, host='0.0.0.0', port=8558, verbose=True, debug=False):
        '''
        constructor

        Args:
            host(str): flask host
            port(int): the port to use for http connections
            debug(bool): True if debugging should be switched on
            verbose(bool): True if verbose logging should be switched on
        '''
        self.debug = debug
        self.verbose = verbose
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        template_folder = scriptdir + '/resources/templates'
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.app_context().push()
        self.authenticate=True
        self.sseBluePrint = SSE_BluePrint(self.app, 'sse')

        @self.app.route('/')
        def home():
            return redirect(self.basedUrl(url_for('series')))

        @self.app.route('/series', methods=['GET', 'POST'])
        def series():
            return self.series()

        @self.app.route('/api/series', methods=['POST'])
        @self.csrf.exempt
        def uploadSeries():
            try:
                for entity in self.updateSeries():
                    pass
                return Response("{‘success’:True}", status=200, mimetype='application/json')
            except Exception as ex:
                print(ex)
                return self._returnErrorMsg(ex)

        @self.app.route('/api/series/<series>', methods=['GET'])
        @self.csrf.exempt
        def handleEventsOfSeries(series: str):
            format = request.values.get("format", "json")
            return self.getSeries(series, format=format, returnTo=request.referrer)


    def init(self,wikiId:str, wikiTextPath:str):
        """

        Args:
            wikiId:
            wikiTextPath:

        Returns:

        """
        self.wikiUser = WikiUser.ofWikiId(wikiId)
        self.wikiTextPath = wikiTextPath
        self.targetWiki = self.wikiUser.getWikiUrl()
        self.initOpenResearch()

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
        self.orDataSource.load(True)
        for manager in self.orDataSource.eventManager, self.orDataSource.eventSeriesManager:
            for record in manager.getList():
                record.smwHandler.wikiFileManager=manager.wikiFileManager

        self.eventTemplateProps={"pageTitle":"pageTitle", **{value:key for key, value in self.orDataSource.eventManager.clazz.getTemplateParamLookup().items()}}
        self.seriesTemplateProps = {"pageTitle":"pageTitle", **{value:key for key, value in self.orDataSource.eventSeriesManager.clazz.getTemplateParamLookup().items()}}


    def series(self):
        seriesChoices = [(getattr(series, 'pageTitle'), getattr(series, 'pageTitle')) for series in self.orDataSource.eventSeriesManager.getList()]
        downloadForm=SeriesForm(seriesChoices)
        seriesRecord = None
        eventRecords = None
        uploadProgress = None
        selectedParam = request.values.get("series", None)
        if downloadForm.downloadSubmitted():
            series = downloadForm.searchValue
            if series:
                return redirect(self.basedUrl(url_for(f"/api/series/{series}?format=spreadsheet")))
            else:
                flash("Please select a event series for download", "info")
        if downloadForm.uploadSubmitted() and len(request.files)>0:
            try:
                generator = self.updateSeries()
                uploadProgress = self.sseBluePrint.streamDictGenerator(generator=generator)
            except Exception as ex:
                print(ex)
                flash(str(ex), "warning")
        if downloadForm.searchSubmitted() or selectedParam:
            seriesId=selectedParam
            if downloadForm.searchValue:
                seriesId=downloadForm.searchValue
            else:
                downloadForm.searchValue=seriesId
            seriesData = self._getSeries(seriesId)
            if seriesData:
                seriesRecord= [seriesData.get('series',[])]
                eventRecords = list(seriesData.get('events').values())

        valueMap = {
            "pageTitle": lambda value: Link(url=f"https://confident.dbis.rwth-aachen.de/orfixed/index.php?title={value}", title=value),
            "homepage": lambda value: Link(url=value, title=value),
            "wikidataId": lambda value: Link(url=f"https://www.wikidata.org/wiki/{value}", title=value),
            "WikiCfpSeries": lambda value: Link(url=f"http://www.wikicfp.com/cfp/program?id={value}", title=value),
            "wikicfpId": lambda value: Link(url=f"http://www.wikicfp.com/cfp/servlet/event.showcfp?eventid={value}", title=value),
            "DblpConferenceId": lambda value: Link(url=f"https://dblp2.uni-trier.de/db/conf/{value}", title=value),
            "dblpSeries": lambda value: Link(url=f"https://dblp.org/db/conf/{value}/index.html", title=value),
            "inEventSeries": lambda value: Link(url=f"https://confident.dbis.rwth-aachen.de/orfixed/index.php?title={value}", title=value),
        }
        def convertValues(lod:list, valueMap:dict):
            if lod is None:
                return lod
            for record in lod.copy():
                for key, function in valueMap.items():
                    if key in record:
                        record[key] = function(record[key])
            return lod

        seriesLod=convertValues(seriesRecord, valueMap)
        eventLod=convertValues(eventRecords, valueMap)


        return render_template('series.html',
                               progress=uploadProgress,
                               downloadForm=downloadForm,
                               series=LodTable(seriesLod, headers=self.seriesTemplateProps, name="Event series"),
                               events=LodTable(eventLod, headers=self.eventTemplateProps, name="Events", isDatatable=True))


    def getSeries(self, series:str="", format:str="json", returnTo:str=""):
        """
        Return the series and its events as OpenDocument Spreadsheet

        Args:
            series(str): name of the series that should be returned
            format(str): format of the response. Supported json, spreadsheet

        Returns:

        """
        try:
            seriesData = self._getSeries(series)
            if not seriesData:
                return flash("Series not found")
            if format == "application/vnd.oasis.opendocument.spreadsheet":
                doc=OdsDocument(series)
                doc.addTable([seriesData.get("series",{})], doc.lod2Table, name="series", headers=self.seriesTemplateProps.values())
                doc.addTable(list(seriesData.get('events').values()), doc.lod2Table,name="events", headers=self.eventTemplateProps.values())
                buffer=doc.toBytesIO()
                return send_file(buffer, attachment_filename=doc.filename, as_attachment=True, mimetype=doc.MIME_TYPE)
            elif format in ["spreadsheet", ExcelDocument.MIME_TYPE]:
                doc = ExcelDocument(series)
                doc.addTable("series",[seriesData.get("series", {})],headers=self.seriesTemplateProps)
                doc.addTable("events", list(seriesData.get('events').values()), headers=self.eventTemplateProps)
                buffer = doc.toBytesIO()
                return send_file(buffer, attachment_filename=doc.filename, as_attachment=True, mimetype=doc.MIME_TYPE)
            else:
                return jsonify(seriesData)
        except Exception as e:
            print(e)
            flash(f'Something went wrong the requested document could not be generated', 'warning')
            return render_template('errorPage.html', url=returnTo, title=series)

    def _getSeries(self, series:str="") -> dict:
        """
        Get the event series records and the records of the events of the series
        Args:
            series: pageTitle of the series to retrieve

        Returns:
            dict with  the elements 'series' (containing the series records) and 'events' (containing the events of the series as dict with the pageTitle as key)
        """
        seriesLookup = self.orDataSource.eventSeriesManager.getLookup(self.orDataSource.eventSeriesManager.primaryKey,withDuplicates=True)
        if series in seriesLookup:
            eventSeries = seriesLookup.get(series)
            eventSeriesRecords = LOD.filterFields([s.__dict__ for s in eventSeries], fields=list(self.seriesTemplateProps.keys()),reverse=True)
            eventRecords = []
            if len(eventSeriesRecords) == 1:
                seriesAcronym = eventSeriesRecords[0].get("acronym")
                events = self.orDataSource.eventManager.getEventsInSeries(seriesAcronym)
                if events:
                    eventRecords = [event.__dict__ for event in events]
                    eventRecords = LOD.filterFields(eventRecords, fields=list(self.eventTemplateProps.keys()), reverse=True)
            res={
                "series":eventSeriesRecords[0],
                "events": {record.get('pageTitle'): record for record in eventRecords}
            }
            return res
        else:
            return None

    def updateSeries(self):
        """
        Updates the series
        Returns:
            Generator for updating event series and events yields the updated entity record
        """
        if self.authenticate:
            wikiUserInfo = WikiUserInfo.fromWiki(self.targetWiki, request.headers)
        else:
            wikiUserInfo = WikiUserInfo("0", "Test")
        if not self.authenticate or wikiUserInfo.isVerified():
            if request.content_type == "application/json":
                seriesData = json.loads(request.data)
            elif request.content_type.startswith("multipart/form-data"):
                updateGenerators=[]
                for spreadsheetFile in request.files.values():
                    # check if an file was selected
                    if not (spreadsheetFile and spreadsheetFile.stream.read()):
                        # no or empty file was submitted
                        raise Exception("Please select a file.", "info", "series upload")
                    spreadsheetFile.stream.seek(0)
                    if spreadsheetFile.filename.endswith(".ods"):
                        doc = OdsDocument("UploadedFile")
                        doc.loadFromFile(spreadsheetFile)
                        series = doc.getLodFromTable("series")
                        seriesData = {
                            "series": series[0] if len(series) > 0 else {},
                            "events": doc.getLodFromTable("events")
                        }
                    else:
                        # try to load as excel document
                        doc = ExcelDocument("UploadedFile")
                        doc.loadFromFile(spreadsheetFile)
                        seriesData={
                            "series": doc.tables["series"][0] if "series" in doc.tables and len(doc.tables["series"]) > 0 else {},
                            "events": doc.tables["events"] if "events" in doc.tables else []
                        }
                    # postprocess the extracted values
                    # convert template property names back to CC property names
                    reverseSeriesPropLUT = {value:key for key, value in self.seriesTemplateProps.items()}
                    seriesData["series"] = {reverseSeriesPropLUT[key]:value for key, value in seriesData['series'].items() if key in reverseSeriesPropLUT}
                    reverseEventPropLUT = {value: key for key, value in self.eventTemplateProps.items()}
                    seriesData["events"] = [{reverseEventPropLUT[key]: value for key, value in record.items() if key in reverseEventPropLUT}for record in seriesData['events']]

                    def updateEntityGenerator(entities: list, manager: EventBaseManager):
                        def publishEntity(entity, **kwargs):
                            """
                            Publishes the given entity to its target wiki.
                            Args:
                                entity: OREvent or OREventSeries (or similar event with smwHandler)
                                **kwargs:
                            """
                            smwHandler = entity.smwHandler
                            if hasattr(smwHandler, 'pushToWiki') and callable(getattr(smwHandler, 'pushToWiki')):
                                smwHandler.pushToWiki(f"spreadsheet import by {wikiUserInfo.name} through orapi",
                                                      overwrite=True,
                                                      wikiFileManager=self.wikiFileManager)

                        for entity in entities:
                            manager.updateFromLod([entity], overwriteEvents=True, updateEntitiesCallback=publishEntity)
                            yield entity

                    eventUpdateGenerator = updateEntityGenerator(seriesData.get("events"),self.orDataSource.eventManager)
                    eventSeriesUpdateGenerator = updateEntityGenerator([seriesData.get("series")],self.orDataSource.eventSeriesManager)
                    updateGenerators.append(eventUpdateGenerator)
                    updateGenerators.append(eventSeriesUpdateGenerator)
                return itertools.chain(*updateGenerators)
            else:
                raise Exception("Content type not supported.")
            return




    def _returnErrorMsg(self, msg:str, status:str, returnToPage:str):
        """
        Returns the given error message as flash message on a html page
        Args:
            msg: error message to be displayed
            returnToPage: pageTitle of the page to return to
        Returns:

        """
        flash(msg, status)
        return render_template('errorPage.html', url=request.referrer, title=returnToPage)

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
            headers={key: value for (key, value) in headers if key =="Cookie"},
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





class SeriesForm(FlaskForm):
    """
    download event series and events as spreadsheet
    """
    search = SelectField('search', render_kw={"onchange": "this.form.submit()"},
                         description="Enter a Event series accronym to select the series for download")
    download = ButtonField(render_kw={"value": "false", "type": "submit", "onclick":"this.value=true;"})
    dropzone = DropZoneField(id="files", uploadId="upload", configParams={'acceptedFiles': ".ods, .xlsx"})
    upload = ButtonField(render_kw={"value": "false", "type": "submit", "onclick":"this.value=true;"})

    def __init__(self, choices:list=[]):
        super(SeriesForm, self).__init__()
        self.search.choices=choices
        self.search.validators=[validators.any_of([v1 for v1,v2 in choices])]

    def downloadSubmitted(self):
        return self.validate_on_submit() and self.data.get('download', False)=="true" and not self.uploadSubmitted() # download is handled as redirect thus the value is not updated → possible soultion cooldown for value

    def searchSubmitted(self):
        return self.validate_on_submit() and self.data.get('search', False)

    def uploadSubmitted(self):
        return self.validate_on_submit() and self.data.get('upload', False) == "true"

    @property
    def searchValue(self):
        if hasattr(self, 'data'):
            return self.data.get('search', "")

    @searchValue.setter
    def searchValue(self, seriesId:str):
        if seriesId in self.search.choices:
            self.search.data=seriesId
        else:
            flash("Series not found")


DEBUG = False

def main(argv=None):
    '''main program.'''
    # construct the web application
    web=WebServer()
    home=path.expanduser("~")
    parser = web.getParser(description="openresearch api to retrieve and edit data")
    parser.add_argument('--wikiTextPath',default=f"{home}/.or/generated/orfixed", help="location of the wikiMarkup files to be used to initialize the ConferenceCorpus")  #ToDo: Update default value
    parser.add_argument('-t', '--target', default="wikirenderTest", help="wikiId of the target wiki [default: %(default)s]")
    parser.add_argument('--verbose', default=True, action="store_true", help="should relevant server actions be logged [default: %(default)s]")
    args = parser.parse_args()
    web.optionalDebug(args)
    web.init(wikiId=args.target,wikiTextPath=args.wikiTextPath)
    web.run(args)

if __name__ == '__main__':
    sys.exit(main())
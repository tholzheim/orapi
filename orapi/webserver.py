import itertools
import os
import json
import sys
import uuid
from io import BytesIO, StringIO

import requests
from os import path
from typing import List

import wikibot.wikipush
from corpus.event import EventBaseManager
from fb4.app import AppWrap
from fb4.sse_bp import SSE_BluePrint
from fb4.widgets import Widget, LodTable, DropZoneField, ButtonField, Link
from flask_wtf import FlaskForm
from lodstorage.lod import LOD
from wikibot.wikiuser import WikiUser
from wtforms import StringField, SelectField, MultipleFileField, SubmitField, FileField, validators, Field
from orapi.odsDocument import OdsDocument, ExcelDocument
from corpus.datasources.openresearch import OR, OREvent, OREventSeries
from wikifile.wikiFileManager import WikiFileManager
from flask import request, send_file, redirect, render_template, flash, jsonify, Response, url_for

from orapi.utils import WikiUserInfo, PageHistory


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
        self.authenticate=False
        self.sseBluePrint = SSE_BluePrint(self.app, 'sse', baseUrl=self.baseUrl)

        @self.app.route('/')
        def home():
            #return redirect(self.basedUrl(url_for('series')))
            return render_template('home.html',
                                   seriesUrl=self.basedUrl("/api/series?format=csv"),
                                   eventsUrl=self.basedUrl("/api/events?format=csv"))

        @self.app.route('/series', methods=['GET', 'POST'])
        def series():
            return self.series()

        @self.app.route('/api/series', methods=['POST'])
        @self.csrf.exempt
        def uploadSeries():
            try:
                seriesUpdator = self.updateSeries()
                def generate(generator):
                    for entity in generator:
                        missing_message='series with pageTitle missing'
                        if isinstance(entity, dict):
                            pageTitle=entity.get('pageTitle', missing_message)
                        else:
                            pageTitle=getattr(entity, "pageTitle", missing_message)
                        yield f"Updated {pageTitle}" + "\n"
                    yield "Update completed\n"
                return Response(generate(seriesUpdator), mimetype='text/event-stream')
            except PermissionError as ex:
                return str(ex) +"\n"
            except TypeError as ex:
                return str(ex) +"\n"
            except Exception as ex:
                print(ex)
                return self._returnErrorMsg(str(ex))

        @self.app.route('/api/series/<series>', methods=['GET'])
        @self.csrf.exempt
        def handleEventsOfSeries(series: str):
            EXCEL_POSTFIX=".xlsx"
            ODS_POSTFIX=".ods"
            format = "json"
            if series.endswith(EXCEL_POSTFIX):
                format="spreadsheet"
                series=series[:-len(EXCEL_POSTFIX)]
            elif series.endswith(ODS_POSTFIX):
                format="application/vnd.oasis.opendocument.spreadsheet"
                series=series[:-len(ODS_POSTFIX)]
            print("Format: ", format, " Series: ", series)
            return self.getSeries(series, format=format, returnTo=request.referrer)

        @self.app.route('/api/series', methods=['GET'])
        def getSeries():
            format=request.values.get('format', None)
            if format is None:
                if request.accept_mimetypes['application/json']: format="json"
                elif request.accept_mimetypes['text/csv']: format="csv"
                else: format="json"
            if format=="json":
                eventSeries=self.orDataSource.eventSeriesManager.getList()
                eventSeriesRecords = LOD.filterFields([s.__dict__ for s in eventSeries],
                                                      fields=list(self.seriesTemplateProps.keys()), reverse=True)
                return jsonify(eventSeriesRecords), '200 OK'
            elif format=="csv":
                csvStr = self.orDataSource.eventSeriesManager.asCsv()
                return self.sendFile(csvStr, name="series.csv", mimetype="text/csv")
            else:
                return "Type not supported"

        @self.app.route('/api/events', methods=['GET'])
        def getEvents():
            format = request.values.get('format', None)
            if format is None:
                if request.accept_mimetypes['application/json']:
                    format = "json"
                elif request.accept_mimetypes['text/csv']:
                    format = "csv"
                else:
                    format = "json"
            if format == "json":
                events = self.orDataSource.eventManager.getList()
                eventRecords = LOD.filterFields([s.__dict__ for s in events],
                                                      fields=list(self.eventTemplateProps.keys()), reverse=True)
                return jsonify(eventRecords), '200 OK'
            elif format == "csv":
                csvStr = self.orDataSource.eventManager.asCsv()
                return self.sendFile(csvStr, name="series.csv", mimetype="text/csv")
            else:
                return "Type not supported"

        @self.app.route('/api/publish/series/<series>', methods=['GET'])
        @self.csrf.exempt
        def publishSeries(series: str):
            publisher=WikiUserInfo.fromWiki(self.wikiUser.getWikiUrl(), request.headers)
            if not publisher.isVerified():
                return self._returnErrorMsg(msg="Permission required to upload changes to a the wiki",
                                            returnToPage=series,
                                            status='401',
                                            url=f"{self.wikiUser.getWikiUrl()}/index.php?title={series}")
            publishPagesGenerator=self.publishSeries(series,
                                                     source=self.wikiId,
                                                     target=self.publishWikiUser.wikiId,
                                                     publisher=publisher)
            publishProgress = self.sseBluePrint.streamDictGenerator(generator=publishPagesGenerator)
            source = "orfixed" if self.wikiUser.scriptPath == "/orfixed" else self.wikiUser.wikiId
            target = "OpenResearch Clone" if self.publishWikiUser.scriptPath == "/or" else self.publishWikiUser.wikiId

            return render_template("publishedPages.html",
                                   series=series,
                                   source=source,
                                   sourceUrl=self.wikiUser.getWikiUrl(),
                                   target=target,
                                   targetUrl=self.publishWikiUser.getWikiUrl(),
                                   publishProgress=publishProgress)




    def init(self,wikiId:str, wikiTextPath:str, publishWikiId:str):
        """

        Args:
            wikiId:
            wikiTextPath:
            publishWikiId(str): wikiId of the wiki to which pages should be published
        Returns:

        """
        self.wikiUser = WikiUser.ofWikiId(wikiId)
        self.wikiTextPath = wikiTextPath
        self.targetWiki = self.wikiUser.getWikiUrl()
        self.publishWikiUser=WikiUser.ofWikiId(publishWikiId)
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
        downloadForm=SeriesForm(seriesChoices, basedUrlFn=self.basedUrl)
        seriesRecord = None
        eventRecords = None
        uploadProgress = None
        selectedParam = request.values.get("series", None)
        if downloadForm.downloadSubmitted():
            series = downloadForm.searchValue
            if series:
                return redirect(self.basedUrl(f"/api/series/{series}.xlsx"))
            else:
                flash("Please select a event series for download", "info")
        if downloadForm.uploadSubmitted() and len(request.files)>0:
            try:
                generator = self.updateSeries()
                uploadProgress = self.sseBluePrint.streamDictGenerator(generator=generator)
            except PermissionError as ex:
                flash("You need to be logged into the wiki to upload!")
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
                return None
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
            wikiUserInfo = WikiUserInfo("0", "unknown")
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
                    elif spreadsheetFile.filename.endswith(".xlsx"):
                        # try to load as excel document
                        doc = ExcelDocument("UploadedFile")
                        updateKeys=lambda lod, map: [{map.get(k,k):v for k,v in d.items()} for d in lod]
                        samples={
                            'events':updateKeys(OREvent.getSamples(), self.eventTemplateProps),
                            'series':updateKeys(OREventSeries.getSamples(), self.seriesTemplateProps)
                        }
                        doc.loadFromFile(spreadsheetFile, samples=samples)
                        seriesData={
                            "series": doc.tables["series"][0] if "series" in doc.tables and len(doc.tables["series"]) > 0 else {},
                            "events": doc.tables["events"] if "events" in doc.tables else []
                        }
                    else:
                        raise TypeError("File type is not supported. Must be '.xlsx' or '.ods'")
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
                            yield {k:str(v) for k,v in entity.items()}

                    eventUpdateGenerator = updateEntityGenerator(seriesData.get("events"),self.orDataSource.eventManager)
                    eventSeriesUpdateGenerator = updateEntityGenerator([seriesData.get("series")],self.orDataSource.eventSeriesManager)
                    updateGenerators.append(eventUpdateGenerator)
                    updateGenerators.append(eventSeriesUpdateGenerator)
                return itertools.chain(*updateGenerators)
            else:
                raise Exception("Content type not supported.")
            return
        else:
            raise PermissionError("Permission required to upload changes to a the wiki")

    def sendFile(self, data:str, name:str,mimetype:str="text" ):
        """
        Send the given string as file
        Args:
            data(str): string to be send as file
            name: na of the file
            mimetype: type of the file

        Returns:

        """
        buffer = BytesIO()
        buffer.write(data.encode())
        buffer.seek(0)
        return send_file(buffer, as_attachment=True, attachment_filename=name, mimetype=mimetype)

    def _returnErrorMsg(self, msg:str, status:str, returnToPage:str, url:str=None):
        """
        Returns the given error message as flash message on a html page
        Args:
            msg: error message to be displayed
            returnToPage: pageTitle of the page to return to
        Returns:

        """
        if url is None:
            url=request.referrer
        flash(msg, status)
        return render_template('errorPage.html', url=url, title=returnToPage)

    def publishSeries(self, series: str, source:str, target: str, publisher:WikiUserInfo):
        """
        publish given series to the given target

        Args:
            series(str): id of the series to be published
            source(str): id of the source wiki
            target(str): id of the target wiki

        Returns:

        """
        wikiFileManager=WikiFileManager(sourceWikiId=source, targetWikiId=target)
        eventsToPublish=[]
        if self.orDataSource.eventSeriesManager.getEventByKey(series):
            eventsToPublish.append(self.orDataSource.eventSeriesManager.getEventByKey(series))
        for event in self.orDataSource.eventManager.getEventsInSeries(series):
            eventsToPublish.append(event)
        targetWikiUser=WikiUser.ofWikiId(target)
        def updateAndPush(event):
            wikiFile=wikiFileManager.getWikiFileFromWiki(event.pageTitle)
            template=getattr(event, 'templateName')
            lod={
                "pageCreator":PageHistory(event.pageTitle, targetWikiUser.getWikiUrl()).getPageOwner(),
                "pageEditor":publisher.name
            }
            wikiFile.updateTemplate(template_name=template,args=lod, overwrite=True)
            wikiFileManager.pushWikiFilesToWiki([wikiFile], updateMsg=f"pushed from orfixed by {publisher.name}")
            return f"Published {event.pageTitle}"
        for event in eventsToPublish:
            yield updateAndPush(event)


class SeriesForm(FlaskForm):
    """
    download event series and events as spreadsheet
    """
    search = SelectField('search', render_kw={"onchange": "this.form.submit()"},
                         description="Enter a Event series accronym to select the series for download")
    download = ButtonField(render_kw={"value": "false", "type": "submit", "onclick":"this.value=true;"})
    dropzone = DropZoneField(id="files",url="/series", uploadId="upload", configParams={'acceptedFiles': ".ods, .xlsx"})
    upload = ButtonField(render_kw={"value": "false", "type": "submit", "onclick":"this.value=true;"})

    def __init__(self, choices:list=[], basedUrlFn:callable=None):
        super(SeriesForm, self).__init__()
        self.search.choices=choices
        self.search.validators=[validators.any_of([v1 for v1,v2 in choices])]
        if basedUrlFn:
            url=basedUrlFn(self.dropzone.config.get("url", ""))
            self.dropzone.updateConfigParams(url=url)
        self.dropzone.updateConfigParams(disablePreviews=False)

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
        if seriesId in [c1 for c1,c1 in self.search.choices]:
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
    parser.add_argument('-t', '--target', default="myor", help="wikiId of the target wiki [default: %(default)s]")
    parser.add_argument('--publishWikiId', default="wikirenderTest",help="wikiId of the wiki to which pages sould be published")
    parser.add_argument('--verbose', default=True, action="store_true", help="should relevant server actions be logged [default: %(default)s]")
    args = parser.parse_args()
    web.optionalDebug(args)
    web.init(wikiId=args.target,wikiTextPath=args.wikiTextPath, publishWikiId=args.publishWikiId)
    web.run(args)

if __name__ == '__main__':
    sys.exit(main())
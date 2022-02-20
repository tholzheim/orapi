import os
import sys
from enum import Enum, auto
from io import BytesIO
from os import path
from fb4.app import AppWrap
from fb4.sse_bp import SSE_BluePrint, DictStreamResult, DictStreamFileResult
from fb4.widgets import DropZoneField, ButtonField, Menu, MenuItem, LodTable, Link
from flask_wtf import FlaskForm
from markupsafe import Markup
from onlinespreadsheet.spreadsheet import SpreadSheetType, ExcelDocument, OdsDocument
from werkzeug.exceptions import Unauthorized
from wikibot.wikiclient import WikiClient
from wtforms import SelectField, SubmitField, BooleanField, StringField
from wtforms.widgets import Select as Select
from orapi.orapiservice import OrApi, WikiTableEditing, OrApiService
from flask import request, send_file, render_template, flash, jsonify, url_for
import socket
from orapi.utils import WikiUserInfo


class ResponseType(Enum):
    '''
    Format type of the response.
    Used to specify the format of a response.
    '''
    CSV=SpreadSheetType.CSV
    EXCEL=SpreadSheetType.EXCEL
    ODS=SpreadSheetType.ODS
    JSON=auto()
    HTML=auto()


class WebServer(AppWrap):
    """
    RESTful api to access and modify OPENRESAECH data
    """

    def __init__(self, host=None, port=8558, verbose=True, debug=False):
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
        if host is None:
            host=socket.gethostname()
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        self.app.app_context().push()
        self.authenticate=False
        self.sseBluePrint = SSE_BluePrint(self.app, 'sse')

        @self.app.route('/')
        def home():
            #return redirect(self.basedUrl(url_for('series')))
            return render_template('home.html', menu=self.getMenuList())

        @self.app.route('/api/series/<series>', methods=['GET','POST'])
        @self.csrf.exempt
        def getSeries(series: str):
            return self.getSeries(series)

        @self.app.route('/api/upload/series', methods=['GET','POST'])
        @self.csrf.exempt
        def updateSeries():
            return self.updateSeries()

        @self.app.route('/api/series')
        @self.csrf.exempt
        def getListOfDblpSeries():
            return self.getListOfDblpSeries()

        @self.app.route('/api/publish/series/<series>', methods=['GET','POST'])
        @self.csrf.exempt
        def publishSeries(series:str):
            return self.publishSeries(series)

    def init(self,orapiService:OrApiService, baseUrl:str=None):
        """
        Args:
            orApi(OrApi): api service to handle the requested actions
        """
        self.orapiService = orapiService
        self.baseUrl = baseUrl

    def getSeries(self, series:str=""):
        """
        Return the series and its events as OpenDocument Spreadsheet

        Args:
            series(str): name of the series that should be returned

        Returns:

        """
        downloadForm = DownloadForm(formatChoices=[('excel', '.xlsx (excel)'),
                                                   ('csv', '.csv (multiple csv files in a .zip)'),
                                                   ('json', '.json'),
                                                   ('ods', '.ods'),
                                                   ('html', 'html (display changes here)')],
                                    sourceWikiChoices=self.orapiService.getAvailableWikiChoices())
        responseFormat=None
        downloadProgress=None
        sourceWiki=None
        buffer=None
        if request.method == "POST":
            responseFormat=downloadForm.responseFormat
            sourceWiki=downloadForm.chosenSourceWiki
        else:
            responseFormat=self.getRequestedFormat()
            source = request.values.get('source', "")
            if source in self.orapiService.wikiIds:
                sourceWiki=source
                downloadForm.sourceWiki.data=source
        if sourceWiki is None:
            sourceWiki = self.orapiService.wikiIds[0]
        orapi = self.orapiService.getOrApi(sourceWiki)
        tableEditing=orapi.getSeriesTableEditing(series)
        if responseFormat is ResponseType.JSON:
            tableEditing.enhance()
            return jsonify(tableEditing.lods)
        elif request.method =="GET" and isinstance(responseFormat.value, Enum) and responseFormat.value in SpreadSheetType:
            tableEditing.enhance()
            doc = tableEditing.toSpreadSheet(responseFormat.value, name=series)
            buffer=doc.toBytesIO()
            return send_file(buffer, attachment_filename=doc.filename, as_attachment=True, mimetype=doc.MIME_TYPE)

        def generator():
            yield from orapi.getSeriesTableEnhanceGenerator(tableEditing)
            seriesTable, eventsTable = orapi.getHtmlTables(tableEditing)
            if isinstance(responseFormat.value, Enum) and responseFormat.value in SpreadSheetType:
                doc = tableEditing.toSpreadSheet(responseFormat.value, name=series)
                buffer = doc.toBytesIO()
                yield DictStreamFileResult(result=str(seriesTable) + str(eventsTable), file=buffer)
            else:
                yield DictStreamResult(str(seriesTable) + str(eventsTable))
        downloadProgress = self.sseBluePrint.streamDictGenerator(generator=generator())
        return render_template('seriesAndEvents.html',
                               menu=self.getMenuList(),
                               downloadForm=downloadForm,
                               progress=downloadProgress)

    def updateSeries(self):
        """
        Updates the series
        Returns:
            Generator for updating event series and events yields the updated entity record
        """
        uploadProgress = None
        uploadForm=UploadForm(targetWikiChoices=self.orapiService.getAvailableWikiChoices())
        if request.method == "POST":
            targetWiki = uploadForm.chosenTargetWiki
            if not self.isAuthorized(wikiId=targetWiki):
                return self._returnErrorMsg("You need to be logged into the wiki to publish a series", status="Error")
            orapi = self.orapiService.getOrApi(targetWiki)
            publisher = WikiUserInfo.fromWiki(orapi.wikiUrl, request.headers)
            cookie=request.headers.get("Cookie")
            if len(request.files) == 1:  #ToDo Extend for multiple file upload
                tableEditing=orapi.getTableEditingFromSpreadsheet(list(request.files.values())[0], publisher)
                #orapi.addPageHistoryProperties(tableEditing)
                try:
                    def generator(tableEditing:WikiTableEditing):
                        if not uploadForm.isDryRun:
                            updateGenerator = orapi.uploadLodTableGenerator(tableEditing, userWikiSessionCookie=cookie)
                            yield from updateGenerator
                        else:
                            yield "Dry Run!!!"
                        seriesTable, eventsTable = orapi.getHtmlTables(tableEditing)
                        yield DictStreamResult(str(seriesTable) + str(eventsTable))
                    uploadProgress=self.sseBluePrint.streamDictGenerator(generator(tableEditing))
                except Unauthorized as e:
                    flash(e.description, category="error")
                except Exception as e:
                    print(e)
                    raise e
        return render_template('upload.html',
                               menu=self.getMenuList(),
                               uploadForm=uploadForm,
                               progress=uploadProgress)

    def getListOfDblpSeries(self):
        """
        Returns the list of DBLPEventSeries of the requested wiki
        """
        orapi = self.orapiService.getOrApi(wikiId="orclone")
        lod = orapi.getListOfDblpEventSeries()
        # add orapi links/buttons
        for record in lod:
            pageTitle = record.get("pageTitle")
            download=Link(url=self.basedUrl(url_for(f"getSeries", series=pageTitle))+f"?source=or", title="Download")
            downloadExcel = Link(url=self.basedUrl(url_for(f"getSeries", series=pageTitle)) + f"?format=excel&source=or", title="Excel")
            upload = Link(url=self.basedUrl(url_for(f"updateSeries"))+f"?target=orclone", title="Upload")
            publish = Link(url=self.basedUrl(url_for(f"publishSeries", series=pageTitle))+f"?source=orclone", title="Publish")
            record["orapi"]=f"{download} ({downloadExcel}) | {upload} | {publish}"
        lod = orapi.convertLodValues(lod, orapi.propertyToLinkMap())
        headerOrder = ["pageTitle", "orapi", "wikidataId", "DblpSeries", "WikiCfpSeries", "homepage", "Has_Biblography"]
        return render_template('series.html',
                               series=LodTable(lod=lod, name="List of DBLPEventSeries", isDatatable=True, headers={h:h for h in headerOrder}),
                               menu=self.getMenuList())

    def publishSeries(self, series:str):
        """
        Publishes a series from source wiki to target wiki
        """
        form = PublishForm(sourceWikiChoices=self.orapiService.getAvailableWikiChoices(),
                           targetWikiChoices=self.orapiService.getAvailableWikiChoices())
        if form.is_submitted():
            sourceWikiId = form.sourceWikiId.data
            targetWikiId = form.targetWikiId.data
            orapi = self.orapiService.getOrApi(wikiId=sourceWikiId, targetWikiId=targetWikiId)
            publisher = form.pageEditor.data
            if not self.isAuthorized(wikiId=sourceWikiId, wikiUserInfo=publisher):
                return self._returnErrorMsg("You need to be logged into the wiki to publish a series", status="Error")
            if not publisher:
                flash("You must define a page editor to publish a series", category="warning")
            else:
                publishGenerator = orapi.publishSeries(seriesAcronym=series, publisher=publisher)
                publishProgress = self.sseBluePrint.streamDictGenerator(generator=publishGenerator)
                return render_template('publishedPages.html',
                                       menu=self.getMenuList(),
                                       series=series,
                                       publishForm=form,
                                       publishProgress=publishProgress)

        sourceWikiId = request.values.get('source', None)
        targetWikiId = request.values.get('target', None)
        if sourceWikiId is None:
            #return error
            pass
        form.sourceWikiId.data = sourceWikiId
        orapi = self.orapiService.getOrApi(sourceWikiId)
        publisher = WikiUserInfo.fromWiki(self.getUrlForWikiId(sourceWikiId), request.headers)
        if not self.isAuthorized(wikiId=sourceWikiId, wikiUserInfo=publisher):
            return self._returnErrorMsg("You need to be logged into the wiki to publish a series", status="Error")
        elif publisher.name == "unknown":
            flash("You must define a page editor")
        else:
            flash("Please ensure that the page editor is correct", category="info")
        tableEditing = orapi.getSeriesTableEditing(series)
        if targetWikiId is not None and targetWikiId in [k for (k,v) in form.targetWikiId.choices]:
            form.targetWikiId.data=targetWikiId
        def generator():
            yield from orapi.getSeriesTableEnhanceGenerator(tableEditing)
            seriesTable, eventsTable = orapi.getHtmlTables(tableEditing)
            yield DictStreamResult(str(seriesTable) + str(eventsTable))
        sourceSeriesOverviewProgress = self.sseBluePrint.streamDictGenerator(generator=generator())
        form.pageEditor.data=publisher.name
        return render_template('publishedPages.html',
                               menu=self.getMenuList(),
                               series=series,
                               publishForm=form,
                               seriesSourceWiki=sourceSeriesOverviewProgress)

    def getUrlForWikiId(self, wikiId):
        """
        Returns the wiki url for given wikiId
        Args:
            wikiId: id of the wiki

        Returns:

        """
        wikiUser = WikiClient.ofWikiId(wikiId).wikiUser
        return wikiUser.url + wikiUser.scriptPath

    def isAuthorized(self, wikiId:str, wikiUserInfo:WikiUserInfo=None):
        """
        Checks if the user has the necessary rights
        Args:
            orapi:

        Returns:
            True if the user is authorized otherwise False
        """
        if self.orapiService.authUpdates:
            if wikiUserInfo is None:
                wikiUserInfo = WikiUserInfo.fromWiki(self.getUrlForWikiId(wikiId=wikiId), request.headers)
            return wikiUserInfo.isVerified()
        else:
            return True

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

    def _returnErrorMsg(self, msg:str, status:str, url:str=None):
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
        return render_template('errorPage.html', menu=self.getMenuList())

    def getRequestedFormat(self) -> ResponseType:
        """
        Returns the requested format type as ResponseType
        Returns:
            ResponseType
        """
        format_param = request.values.get('format', "")
        request_format = ResponseType.__members__.get(format_param.upper(), None)
        if request_format is None:
            if 'text/html' in request.accept_mimetypes:
                request_format = ResponseType.HTML
            elif 'application/json' in request.accept_mimetypes:
                request_format = ResponseType.JSON
            elif 'text/csv' in request.accept_mimetypes:
                request_format = ResponseType.CSV
            elif ExcelDocument.MIME_TYPE in request.accept_mimetypes:
                request_format = ResponseType.EXCEL
            elif OdsDocument.MIME_TYPE in request.accept_mimetypes:
                request_format = ResponseType.ODS
            else:
                request_format = ResponseType.HTML
        return request_format

    def getMenuList(self):
        '''
        set up the menu for this application
        '''
        menu=Menu()
        menu.addItem(MenuItem("/","Home"))
        menu.addItem(MenuItem(self.basedUrl(url_for("updateSeries")),"Upload"))
        menu.addItem(MenuItem(self.basedUrl(url_for("getListOfDblpSeries")),"Series"))
        menu.addItem(MenuItem('https://github.com/tholzheim/orapi','github'))
        menu.addItem(MenuItem('https://github.com/tholzheim/orapi', 'openresearch'))
        menu.addItem(MenuItem('https://confident.dbis.rwth-aachen.de/or/index.php?title=Main_Page', 'orclone'))
        menu.addItem(MenuItem('https://confident.dbis.rwth-aachen.de/orfixed/index.php?title=Main_Page', 'orfixed'))
        return menu


class Select2Widget(Select):
    """
    Widget fot select fields that renders the select field with select2

    see https://select2.org/
    """

    def __init__(self, multiple=False):
        """

        Args:
            multiple: If true the select field allows multiple selections. Otherwise only one selection is possible
        """
        super(Select2Widget, self).__init__(multiple=multiple)

    def __call__(self, field, **kwargs):
        activateMutliSearch=f"<script>$(document).ready(function() {{ $('#{field.name}').select2({str(field.render_kw)});; }});</script>"
        return Markup(super().__call__(field, **kwargs)) + Markup(activateMutliSearch)


class DownloadForm(FlaskForm):
    """
    download event series and events in different formats and allow to select different Enhancement steps
    """
    sourceWiki = SelectField("Source wiki", default="None")
    # enhancements=SelectMultipleField("Enhancements",
    #                                  widget=Select2Widget(multiple=True),
    #                                  render_kw={"placeholder": "Enhancement steps to apply before downloading",
    #                                             "allowClear": 'true'})
    format=SelectField()
    submit=SubmitField(label="Download")

    def __init__(self, enhancerChoices:list=None, formatChoices:list=None, sourceWikiChoices:list=None):
        super(DownloadForm, self).__init__()
        #self.enhancements.choices=enhancerChoices
        self.format.choices=formatChoices
        self.sourceWiki.choices=sourceWikiChoices

    @property
    def responseFormat(self)->ResponseType:
        if hasattr(self, 'format'):
            return ResponseType.__members__.get(self.data.get('format', "").upper(), None)

    @property
    def chosenEnhancers(self):
        if hasattr(self, 'data'):
            return self.data.get('enhancements', [])

    @property
    def chosenSourceWiki(self) -> str:
        if hasattr(self, 'data'):
            return self.data.get('sourceWiki', None)


class UploadForm(FlaskForm):
    """
    download event series and events in different formats and allow to select different Enhancement steps
    """
    targetWiki=SelectField("Target wiki", default="None")
    # enhancements=SelectMultipleField("Enhancements",
    #                                  widget=Select2Widget(multiple=True),
    #                                  render_kw={"placeholder": "Enhancement steps to apply before downloading",
    #                                             "allowClear": 'true'})
    dropzone = DropZoneField(id="files", url="/api/upload/series", uploadId="upload",configParams={'acceptedFiles': ".ods, .xlsx"})
    dryRun = BooleanField(id="Dry run", default="checked")
    upload = ButtonField()

    def __init__(self, enhancerChoices:list=None, targetWikiChoices:list=None):
        super().__init__()
        #self.enhancements.choices=enhancerChoices
        self.targetWiki.choices=targetWikiChoices

    @property
    def responseFormat(self)->ResponseType:
        if hasattr(self, 'format'):
            return ResponseType.__members__.get(self.data.get('format', "").upper(), None)

    @property
    def chosenEnhancers(self):
        if hasattr(self, 'data'):
            return self.data.get('enhancements', [])

    @property
    def chosenTargetWiki(self) -> str:
        if hasattr(self, 'data'):
            return self.data.get('targetWiki', None)

    @property
    def isDryRun(self)->bool:
        return self.dryRun.data

class PublishForm(FlaskForm):
    """
    publish a series
    Form to select a source and target wiki for a series to be published
    """
    sourceWikiId = SelectField("Source wiki")
    targetWikiId = SelectField("Target wiki")
    pageEditor = StringField("Page Editor")
    publish = SubmitField()

    def __init__(self, sourceWikiChoices:list, targetWikiChoices:list):
        super().__init__()
        self.targetWikiId.choices=targetWikiChoices
        self.sourceWikiId.choices=sourceWikiChoices

DEBUG = False

def main(argv=None):
    '''main program.'''
    # construct the web application
    web=WebServer()
    home=path.expanduser("~")
    parser = web.getParser(description="openresearch api to retrieve and edit data")
    parser.add_argument('--wikiTextPath',default=f"{home}/.or/generated/orfixed", help="location of the wikiMarkup files to be used to initialize the ConferenceCorpus")  #ToDo: Update default value
    parser.add_argument('--wikiIds',nargs='*', default="wikiIds",help="wikiIds for which orapi should be provided if none provided all wikiIds will are available")
    parser.add_argument('--requireAuthentication', action="store_true", help="Require wiki session cookie to update a wiki")
    parser.add_argument('--verbose', default=True, action="store_true", help="should relevant server actions be logged [default: %(default)s]")
    args = parser.parse_args()
    web.optionalDebug(args)
    #ToDo: Exchange OrApi with OrApi Service
    web.init(orapiService=OrApiService(wikiIds=args.wikiIds, authUpdates=args.requireAuthentication), baseUrl=args.baseUrl)
    web.run(args)

if __name__ == '__main__':
    sys.exit(main())
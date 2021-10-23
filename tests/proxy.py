import io
import os
from io import BytesIO
from tempfile import TemporaryDirectory
import requests
from fb4.app import AppWrap
from flask import request, Response
import socket

class ProxyServer(AppWrap):
    """
    The ProxyServer unites myor and orapi so that orapi also receives the cookies set by myor.
    Intended us of this proxy server is for testing the functionality of orapi
    """

    def __init__(self, host='0.0.0.0', port=8559, verbose=True, debug=False):
        self.debug = debug
        self.verbose = verbose
        self.lookup = None
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        template_folder = scriptdir + '/../templates'
        if not host:
            host=socket.getfqdn()
        super().__init__(host=host, port=port, debug=debug, template_folder=template_folder)
        self.app.app_context().push()
        self.WIKI_URL = f"http://{socket.getfqdn()}:8000"
        self.ORAPI_URL = 'http://localhost:8558'

        @self.app.route('/wiki', defaults={'path': ''},methods=['GET','POST'])
        @self.app.route('/wiki/<path:path>',methods=['GET','POST'])
        @self.app.route('/load.php', defaults={'path': '/load.php'},methods=['GET','POST'])
        @self.app.route('/api.php', defaults={'path': '/api.php'}, methods=['GET', 'POST'])
        @self.csrf.exempt
        def wikiProxy(path):
            """
            Forward requests to myor
            """
            request.values={key:value for key,value in request.values.items() if key != "returnto"}
            res=self._proxy(self.WIKI_URL+path)
            if res.status_code == 302:
                res.headers["Location"]=res.headers.get("Location").replace(self.WIKI_URL, self.basedUrl(''))
            return res

        @self.app.route('/index.php', defaults={'path': ''}, methods=['GET', 'POST'])
        @self.app.route('/index.php/<path:path>', methods=['GET', 'POST'])
        @self.csrf.exempt
        def directWikiProxy(path):
            """
            Forward requests to myor
            """
            request.values = {key: value for key, value in request.values.items() if key != "returnto"}
            if path:
                path='/'+path
            res = self._proxy(self.WIKI_URL + "/index.php" + path)
            if res.status_code == 302:
                res.headers["Location"]=res.headers.get("Location").replace(self.WIKI_URL, self.basedUrl(''))
            return res

        @self.app.route('/orapi', defaults={'path': ''}, methods=['GET','POST'])
        @self.app.route('/orapi/<path:path>', methods=['GET','POST'])
        @self.csrf.exempt
        def orapiProxy(path):
            """
            Forward requests to orapi
            """
            if path:
                path = '/' + path
            return self._proxy(self.ORAPI_URL+path)

    def _proxy(self, url:str, *args, **kwargs):
        """
        Forwards the request to the given url and returns the response. The request body/headers are adjusted accordingly
        see https://stackoverflow.com/questions/6656363/proxying-to-another-web-service-with-flask
        :param url: url to which the request should be forwarded
        :param args:
        :param kwargs:
        :return:
        """
        if request.files:
            files = request.files
            # for file, f in request.files.items():
            #     buffer=io.BytesIO(f.stream.read().decode())
            #     files[file]=(f.filename, buffer, f.mimetype)
            # If request with file attached use separate request → Had issues otherwise
            resp=requests.post(url, files=files, cookies=request.cookies, headers={key: value for (key, value) in request.headers if key not in ['Host','Content-Length','Content-Type']}  )
        else:
            resp = requests.request(
                method=request.method,
                params=request.values,
                url=url,
                headers={key: value for (key, value) in request.headers if key != 'Host'},
                data=request.get_data(),
                cookies=request.cookies,
                allow_redirects=False)

        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in resp.raw.headers.items()
                   if name.lower() not in excluded_headers]

        response = Response(resp.content, resp.status_code, headers)
        return response

if __name__ == '__main__':
    server=ProxyServer()
    parser = server.getParser(description="dblp conference webservice")
    args = parser.parse_args()
    server.optionalDebug(args)
    server.run(args)
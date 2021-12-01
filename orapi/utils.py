import json
from typing import List

import requests
from lodstorage.jsonable import JSONAble


class PageRevision(JSONAble):
    """
    Represents the revision of a mediawiki page
    """

    @classmethod
    def getSamples(cls):
        samples=[
            {'revid': 7056,
             'parentid': 0,
             'user': '127.0.0.1',
             'anon': '',
             'userid': 0,
             'timestamp': '2008-10-14T21:23:09Z',
             'size': 6905,
             'comment': 'Event created',
             'pageTitle': 'SOMITAS 2008',
             'pageId': 5481
             },
            {
                "revid": 8195,
                "parentid": 8194,
                "user": "Wf",
                "timestamp": "2021-11-11T12:50:31Z",
                "size": 910, "comment": ""
            }]
        return samples

    @staticmethod
    def getRevisons(pageTitle:str, wikiUrl:str):
        """
        Returns the revisons of the given page

        Args:
            pageTitle(str): pageTitle
            wikiUrl(str): location of the wiki the page is in

        Returns:
            yields retrieved PageRevisions
        """
        url = f"{wikiUrl}/api.php"
        params = {
            "action": "query",
            "prop": "revisions",
            "titles": pageTitle.replace(" ", "_"),
            "rvprop": "ids|timestamp|user|userid|comment|size",
            "rvlimit": 500,
            "format": "json"
        }
        resp = requests.get(url=url, params=params)
        data = resp.json()
        if "query" in data:
            queryRecord = data.get("query")
            if "pages" in queryRecord:
                pages = queryRecord.get("pages")
                if isinstance(pages, dict):
                    for page in pages.values():
                        pageId = page.get("pageid")
                        title = page.get("title")
                        revisions = page.get("revisions", {})
                        for rev in revisions:
                            rev["pageTitle"] = title
                            rev["pageId"] = pageId
                            pageRevision=PageRevision()
                            pageRevision.fromDict(rev)
                            yield pageRevision


class PageHistory:
    """
    Represents the history of a page
    """

    def __init__(self, pageTitle, wikiUrl):
        self.pageTitle=pageTitle
        self.wikiUrl=wikiUrl
        self.revisions=[rev for rev in PageRevision.getRevisons(self.pageTitle, self.wikiUrl)]

    def getPageOwner(self):
        """
        Returns the owner of the page (first trustworthy user)

        Returns:
            str
        """
        #ToDo: Add user validation once the list is ready
        for rev in self.revisions:
            if getattr(rev, 'parentid', -1) == 0:  # revision with parentid == 0 is the first revision of the page
                return getattr(rev, 'user')


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
        try:
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
        except Exception as e:
            print(e)
            return WikiUserInfo(id=-1, name="unkown")

'''
Created on 2021-08-19

@author: wf
'''
from unittest import TestCase
import time
import getpass
import os

from wikibot.wikiuser import WikiUser
from wikifile.wikiFileManager import WikiFileManager


class Basetest(TestCase):
    '''
    base test case
    '''
    
    def setUp(self,debug=False,profile=True):
        '''
        setUp test environment
        '''
        TestCase.setUp(self)
        self.debug=debug
        self.profile=profile
        msg=f"test {self._testMethodName}, debug={self.debug}"
        self.profiler=Profiler(msg,profile=self.profile)
        
    def tearDown(self):
        TestCase.tearDown(self)
        self.profiler.time()    
        
    def inCI(self):
        '''
        are we running in a Continuous Integration Environment?
        '''
        publicCI=getpass.getuser() in ["travis", "runner"] 
        jenkins= "JENKINS_HOME" in os.environ
        return publicCI or jenkins

    def getWikiUser(self, wikiId=None) -> WikiUser:
        if wikiId is None:
            wikiId = self.wikiId
        # make sure there is a wikiUser (even in public CI)
        wikiUser = self.getSMW_WikiUser(wikiId=wikiId, save=self.inCI())
        return wikiUser

    def getSMW_WikiUser(self, wikiId="or", save=False) -> WikiUser:
        '''
        get semantic media wiki users for SemanticMediawiki.org and openresearch.org
        '''
        iniFile = WikiUser.iniFilePath(wikiId)
        wikiUser = None
        if not os.path.isfile(iniFile):
            wikiDict = None
            if wikiId == "or":
                wikiDict = {"wikiId": wikiId, "email": "noreply@nouser.com", "url": "https://www.openresearch.org",
                            "scriptPath": "/mediawiki/", "version": "MediaWiki 1.31.1"}
            if wikiId == "orclone":
                wikiDict = {"wikiId": wikiId, "email": "noreply@nouser.com",
                            "url": "https://confident.dbis.rwth-aachen.de", "scriptPath": "/or/",
                            "version": "MediaWiki 1.35.1"}
            if wikiId == "cr":
                wikiDict = {"wikiId": wikiId, "email": "noreply@nouser.com", "url": "https://cr.bitplan.com",
                            "scriptPath": "/", "version": "MediaWiki 1.33.4"}
            if wikiId == "orfixed":
                wikiDict = {"wikiId": wikiId, "email": "noreply@nouser.com",
                            "url": "https://confident.dbis.rwth-aachen.de", "scriptPath": "/orfixed/",
                            "version": "MediaWiki 1.35.5"}

            if wikiDict is None:
                raise Exception("wikiId %s is not known" % wikiId)
            else:
                wikiUser = WikiUser.ofDict(wikiDict, lenient=True)
                if save:
                    wikiUser.save()
        else:
            wikiUser = WikiUser.ofWikiId(wikiId, lenient=True)
        return wikiUser

    @classmethod
    def getWikiFileManager(cls, wikiId=None, debug=False):
        wikiUser = cls.getWikiUser(wikiId)
        home = os.path.expanduser("~")
        wikiTextPath = f"{home}/.or/wikibackup/{wikiUser.wikiId}"
        wikiFileManager = WikiFileManager(wikiId, wikiTextPath, login=False, debug=debug)
        return wikiFileManager

class Profiler:
    '''
    simple profiler
    '''
    def __init__(self,msg,profile=True):
        '''
        construct me with the given msg and profile active flag
        
        Args:
            msg(str): the message to show if profiling is active
            profile(bool): True if messages should be shown
        '''
        self.msg=msg
        self.profile=profile
        self.starttime=time.time()
        if profile:
            print(f"Starting {msg} ...")
    
    def time(self,extraMsg=""):
        '''
        time the action and print if profile is active
        '''
        elapsed=time.time()-self.starttime
        if self.profile:
            print(f"{self.msg}{extraMsg} took {elapsed:5.1f} s")
        return elapsed
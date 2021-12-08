import getpass
import os


class Utils:
    """
    Helper functions for the unit tests
    """

    @staticmethod
    def inCI():
        '''
        are we running in a Continuous Integration Environment?
        '''
        publicCI=getpass.getuser() in ["travis", "runner"]
        jenkins= "JENKINS_HOME" in os.environ
        return publicCI or jenkins
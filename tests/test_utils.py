import uuid
from unittest import TestCase

from orapi.utils import PageHistory
from tests.basetest import Basetest


class TestPageHistory(Basetest):
    """
    Tests the PageHistory class
    """

    def setUp(self,debug=False,profile=True):
        super(TestPageHistory, self).setUp(debug, profile)
        self.wikiUrl="https://www.openresearch.org/mediawiki"

    def test_pageOwnerRetrival(self):
        """
        tests the retrieval of the pageCreator
        """

        pageHistory=PageHistory(pageTitle="AAAI", wikiUrl=self.wikiUrl)
        expectedOwner="Soeren"
        atMostExpectedRevisions=10
        self.assertEqual(expectedOwner, pageHistory.getPageOwner())
        self.assertTrue(len(pageHistory.revisions)>atMostExpectedRevisions)
        self.assertTrue(pageHistory.exists())


    def test_pageOwnerRetrivalForNoneExistantPage(self):
        """
        tests the retrieval of the pageCreator if the page does not exist
        """
        pageHistory = PageHistory(pageTitle=str(uuid.uuid1()), wikiUrl=self.wikiUrl)
        self.assertIsNone(pageHistory.getPageOwner())
        self.assertFalse(pageHistory.exists())
from unittest import TestCase

from orapi.utils import PageHistory
from tests.basetest import Basetest


class TestPageHistory(Basetest):
    """
    Tests the PageHistory class
    """

    def test_pageOwnerRetrival(self):
        """
        tests the retrival of the pageOwner
        """
        Basetest.setUp(self)
        wikiUrl="https://www.openresearch.org/mediawiki"
        pageHistory=PageHistory(pageTitle="AAAI", wikiUrl=wikiUrl)
        expectedOwner="Soeren"
        atMostExpectedRevisions=10
        self.assertEqual(expectedOwner, pageHistory.getPageOwner())
        self.assertTrue(len(pageHistory.revisions)>atMostExpectedRevisions)
from unittest import TestCase

from orapi.utils import PageHistory


class TestPageHistory(TestCase):
    """
    Tests the PageHistory class
    """

    def test_pageOwnerRetrival(self):
        """
        tests the retrival of the pageOwner
        """
        wikiUrl="https://www.openresearch.org/mediawiki"
        pageHistory=PageHistory(pageTitle="AAAI", wikiUrl=wikiUrl)
        expectedOwner="Soeren"
        atMostExpectedRevisions=10
        self.assertEqual(expectedOwner, pageHistory.getPageOwner())
        self.assertTrue(len(pageHistory.revisions)>atMostExpectedRevisions)
'''
Created on 08-Apr-2020

@author: nageb
'''
import unittest
from unittest.mock import patch

from healthmonitoring.framework import runner
from healthmonitoring.framework.actors.audit_html_report import \
    AuditHtmlReportActor
from healthmonitoring.framework.audit.output.html import HtmlReport
from healthmonitoring.framework.audit.report import Report
import healthmonitoring.framework.config
from healthmonitoring.framework.util import file_util

from tests.healthmonitoring.framework.utils import TestUtil


class TestAuditHTMLReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        runner.create_audit_reports(file_util.get_resource_path("."))
        healthmonitoring.framework.config.report_store.add(Report("test"))

    def test_generates_html_report(self):
        actor = AuditHtmlReportActor()
        actor.act({'report': 'test'})
        self.assertEqual(actor.html_report,
                         HtmlReport(Report()).generate_html())

    @patch('healthmonitoring.framework.actors.audit_html_report.EmailActor')
    def test_generates_email(self, MockEmailActor):
        actor = AuditHtmlReportActor()
        SUBJECT = 'Audit Report'
        BODY = "{report}"

        actor.act({'subject': SUBJECT, 'body': BODY, 'report': 'test'})
        html_report = HtmlReport(Report()).generate_html()
        MockEmailActor().act.assert_called_with({
            'subject': SUBJECT,
            'body': html_report
        })


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

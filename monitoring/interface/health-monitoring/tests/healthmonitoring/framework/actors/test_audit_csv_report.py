'''
Created on 14-Apr-2020

@author: nageb
'''
import filecmp
from pathlib import Path
import unittest

import healthmonitoring.framework

from healthmonitoring.framework.actors.audit_csv_report import \
    AuditCSVReportActor
from healthmonitoring.framework.audit.output.csv import CSVReport
from healthmonitoring.framework.audit.report import Report


class TestAuditCSVReport(unittest.TestCase):

    def setUp(self):
        healthmonitoring.framework.audit.report.current_report = Report()

    def test_generates_csv_report(self):
        actor = AuditCSVReportActor()
        PATHNAME_ACTUAL_FILE = 'actual_report.csv'
        PATHNAME_EXPECTED_FILE = 'expected_report.csv'
        actor.act({'pathname': PATHNAME_ACTUAL_FILE})
        CSVReport(Report()).generate_report(PATHNAME_EXPECTED_FILE)
        self.assertTrue(filecmp.cmp(
            PATHNAME_ACTUAL_FILE, PATHNAME_EXPECTED_FILE, shallow=False))
        for pathname in PATHNAME_ACTUAL_FILE, PATHNAME_EXPECTED_FILE:
            Path(pathname).unlink()


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

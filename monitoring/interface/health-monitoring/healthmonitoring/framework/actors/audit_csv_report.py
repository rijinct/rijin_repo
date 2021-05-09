'''
Created on 14-Apr-2020

@author: nageb
'''

from healthmonitoring.framework.audit.output.csv import CSVReport
import healthmonitoring.framework.audit.report as report


class AuditCSVReportActor:

    def __init__(self):
        self._csv_report = None

    @property
    def csv_report(self):
        return self._csv_report

    def act(self, notification_content):
        self._notification_content = notification_content
        self._generate_csv_report()

    def _generate_csv_report(self):
        pathname = self._notification_content['pathname']
        CSVReport(report.current_report).generate_report(pathname)

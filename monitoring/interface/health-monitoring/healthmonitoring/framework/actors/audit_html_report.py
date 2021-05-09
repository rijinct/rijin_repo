'''
Created on 08-Apr-2020

@author: nageb
'''

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.email import EmailResponseGenerator, \
    EmailActor
from healthmonitoring.framework.audit.output.html import HtmlReport

from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class AuditHtmlReportActor:

    def __init__(self):
        self._html_report = None

    @property
    def html_report(self):
        return self._html_report

    def act(self, notification_content):
        logger.info("Initiating HTML Audit Report execution...")
        self._notification_content = notification_content
        self._generate_html_report()
        self._send_email_if_required()

    def _generate_html_report(self):
        report_name = self._notification_content['report']
        self._html_report = HtmlReport(
            config.report_store.get(report_name)).generate_html()

    def _send_email_if_required(self):
        if 'subject' in self._notification_content:
            response = _CustomEmailResponseGenerator(
                self._notification_content['subject'],
                self._notification_content['body'],
                self._html_report).get_response()
            logger.debug("Response: %s", str(response))
            EmailActor().act(response)


class _CustomEmailResponseGenerator(EmailResponseGenerator):

    def __init__(self, subject_template, body_template, report):
        super().__init__(subject_template, body_template)
        self._params['report'] = report

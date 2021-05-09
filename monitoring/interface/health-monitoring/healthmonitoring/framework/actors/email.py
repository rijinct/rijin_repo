'''
Created on 07-Feb-2020

@author: nageb
'''

from copy import deepcopy
import smtplib

from healthmonitoring.framework import config
from healthmonitoring.framework.util.specification import SpecificationUtil
from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class EmailMedia:
    def __init__(self):
        email = SpecificationUtil.get_host("email")
        self._setup(
            email.fields.get('cemod_smtp_ip')
            or email.fields.get('cemod_pop3_server_ip'),
            email.fields.get('cemod_smtp_sender_emailids')
            or email.fields.get('cemod_smtp_sender_emailid'),
            email.fields.get('cemod_smtp_receiver_emailids'))

    def _setup(self, smtp_url, sender, recepients):
        self._smtp_url = smtp_url
        self._sender = sender
        self._recepients = deepcopy(recepients)

    @property
    def smtp_url(self):
        return self._smtp_url

    @property
    def sender(self):
        return self._sender

    @property
    def recepients(self):
        return self._recepients


class EmailAction:
    _media = None

    def __init__(self):
        if EmailAction._media is None:
            EmailAction._media = EmailMedia()

    def _get_formatted_email(self, email_subject, email_body):
        message = """From: {}
To: {}
MIME-Version: 1.0
Content-type: text/html
Subject: {}
<p style="font-size: large; font-style: bold">****AlertDetails****</p>
{}
""".format(self._media.sender, self._media.recepients, email_subject,
           email_body)
        return message

    def _send_mail(self, formatted_email):
        try:
            smtpObj = smtplib.SMTP(self._media.smtp_url)
            smtpObj.sendmail(self._media.sender, self._media.recepients,
                             formatted_email)
            smtpObj.quit()
        except smtplib.SMTPException:
            logger.info('Error: Unable to send email alert')

    def _frame_email_and_send(self, notification_content):
        formatted_email = self._get_formatted_email(
            notification_content['subject'], notification_content['body'])
        self._send_mail(formatted_email)

    def execute(self, notification_content):
        self._frame_email_and_send(notification_content)


class EmailResponseGenerator:
    def __init__(self, subject_template, body_template):
        self._subject_template = subject_template
        self._body_template = body_template
        self._subject, self._body = "", ""
        self._params = {}

    def get_response(self):
        self._create_subject()
        self._create_body()
        return {"subject": self._subject, "body": self._body}

    def _create_subject(self):
        self._subject = self._format(self._subject_template)

    def _create_body(self):
        self._body = self._format(self._body_template)

    def _format(self, what):
        return what.format(**self._params)


class EmailActor:
    def act(self, notification_content):
        logger.info("Initiating EmailAction execution...")
        EmailAction().execute(notification_content)

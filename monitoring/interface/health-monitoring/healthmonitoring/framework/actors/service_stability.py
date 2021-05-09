'''
Created on 19-Feb-2020

@author: nageb
'''

from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.email import EmailActor
from healthmonitoring.framework.actors.email import EmailResponseGenerator
from healthmonitoring.framework.util import string_util

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class _CustomEmailResponseGenerator(EmailResponseGenerator):

    def __init__(self, subject_template, body_template, trigger_list):
        super().__init__(subject_template, body_template)
        self._params["services"] = "\n".join([string_util.substring_after(
                ".", trigger.item.name) for trigger in trigger_list])
        self._params["failed_services_count"] = len(trigger_list)


class ConsolidateTriggersActor:

    def act(self, notification_content):
        logger.info("Initiating ModelAction execution...")
        trigger_list = notification_content['event'].triggers
        response = _CustomEmailResponseGenerator(
            notification_content['subject'],
            notification_content['body'],
            trigger_list).get_response()
        self._send_email(response)

    def _send_email(self, response):
        EmailActor().act(response)

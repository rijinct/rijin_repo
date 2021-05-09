'''
Created on 28-Feb-2020

@author: deerakum
'''
from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.util.trigger_util import TriggerUtil
from healthmonitoring.framework.actors.email import EmailActor
from healthmonitoring.framework.actors.email import EmailResponseGenerator

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class AggregationActor:

    def act(self, notification_content):
        logger.info("Initiating ModelAction execution...")
        response = _CustomEmailResponseGenerator(
            notification_content['subject'],
            notification_content['body'],
            notification_content["event"].triggers).get_response()
        self._send_email(response)

    def _send_email(self, response):
        EmailActor().act(response)


class _CustomEmailResponseGenerator(EmailResponseGenerator):

    def __init__(self, subject_template, body_template, triggers):
        super().__init__(subject_template, body_template)
        self._triggers = triggers

    def _create_body(self):
        self._body = ""
        var = config.properties['AggregationSection']
        for trigger_name, body_label in var.items():
            self._populate_body(
                trigger_name, body_label,
                list(self._triggers.keys()))

    def _populate_body(self, trigger_name, body_label, trigger_name_list):
        specific_trigger_name_list = TriggerUtil.get_specific_trigger_list(
            trigger_name, trigger_name_list)
        if specific_trigger_name_list:
            self._populate_body_for_specific_trigger_name_list(
                specific_trigger_name_list, body_label)

    def _populate_body_for_specific_trigger_name_list(
            self, specific_trigger_name_list, body_label):
        self._body += body_label + '\n'
        item_values = self._get_item_values(specific_trigger_name_list)
        self._body += "\n".join(item_values) + "\n"

    def _get_item_values(self, specific_trigger_name_list):
        item_values = []
        for specific_trigger_name in specific_trigger_name_list:
            item_values.append(self._get_item_value(specific_trigger_name))
        return item_values

    def _get_item_value(self, specific_trigger_name):
        trigger = self._triggers[specific_trigger_name]
        index = TriggerUtil.get_index_of_trigger(trigger.name)
        return trigger.item.params['hour_agg_dict_ds'][index]['jobname']

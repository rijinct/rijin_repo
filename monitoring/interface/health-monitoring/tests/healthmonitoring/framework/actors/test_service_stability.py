'''
Created on 07-Feb-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.service_stability import \
    _CustomEmailResponseGenerator, ConsolidateTriggersActor
from tests.healthmonitoring.framework.utils import TestUtil

from healthmonitoring.framework.store import Trigger, Item


class TestCustomEmailResponseGenerator(unittest.TestCase):
    SUBJECT = "{failed_services_count} services failed!"
    BODY = "List of failed services:\n{services}"

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def _create_trigger_list(self):
        trigger2 = Trigger(config.triggers_spec.get_trigger("trigger2"))
        trigger2.item = Item(
            config.items_spec.get_item("application1.item1"),
            count=10,
            type=1)
        trigger3 = Trigger(config.triggers_spec.get_trigger("trigger3"))
        trigger3.item = Item(
            config.items_spec.get_item("container.item1"), value1=1)
        trigger_list = [trigger2, trigger3]
        return trigger_list

    def test_create_subject(self):
        trigger_list = self._create_trigger_list()
        subject = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY,
            trigger_list).get_response()["subject"]
        self.assertEqual(subject, "2 services failed!")

    def test_create_body(self):
        trigger_list = self._create_trigger_list()
        body = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY,
            trigger_list).get_response()["body"]
        expected = """List of failed services:
item1
item1"""
        self.assertEqual(body, expected)


class TestConsolidateTriggersActor(unittest.TestCase):

    class Event:

        def __init__(self):
            self.triggers = []

    def _send_mail(self, response):
        self.response = response

    def test_act(self):
        event = TestConsolidateTriggersActor.Event()
        actor = ConsolidateTriggersActor()
        original_send_mail = actor._send_email
        actor._send_email = self._send_mail
        notification_content = {
            "subject": "Test subject",
            "body": "Test body",
            "event": event
        }
        actor.act(notification_content)
        expected = notification_content.copy()
        del (expected["event"])
        self.assertEqual(self.response, expected)
        actor._send_email = original_send_mail


if __name__ == "__main__":
    unittest.main()

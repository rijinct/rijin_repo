'''
Created on 28-Feb-2020

@author: deerakum
'''
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework.specification.defs import \
    TriggerSpecification, ItemSpecification, Application, Severity
from healthmonitoring.framework.store import Trigger, Item
from mock import patch

from healthmonitoring.framework.actors.aggregation import \
    _CustomEmailResponseGenerator, AggregationActor
from tests.healthmonitoring.framework.utils import TestUtil


class TestCustomEmailResponseGenerator(unittest.TestCase):
    SUBJECT = "Aggregation Status"
    BODY = ""

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def test_create_subject(self):
        trigger_list = _Util.create_triggers()
        response = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY,
            trigger_list).get_response()
        self.assertEqual(response["subject"],
                         TestCustomEmailResponseGenerator.SUBJECT)

    def test_create_body_when_execution_duration_breached_and_no_failed_jobs(
            self):
        item = _Util.create_item()
        trigger_names = [
            "execution_duration_breached_trigger_0",
            "execution_duration_breached_trigger_1"
        ]
        triggers = _Util._to_triggers_dict(trigger_names, item)
        response = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY, triggers).get_response()
        expected = """List of Long Running Jobs:
Perf_VOICE_SEGG_1_HOUR_AggregateJob
Perf_VOLTE_SEGG_1_HOUR_AggregateJob
"""
        self.assertEqual(response["body"], expected)

    def test_create_body_when_execution_duration_breached_and_with_failed_jobs(
            self):
        item = _Util.create_item()
        trigger_names = [
            "execution_duration_breached_trigger_0",
            "execution_duration_breached_trigger_1", "failed_jobs_trigger_1",
            "failed_jobs_trigger_2", "last_jobs_trigger_0"
        ]
        triggers = _Util._to_triggers_dict(trigger_names, item)
        response = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY, triggers).get_response()
        expected = """List of Long Running Jobs:
Perf_VOICE_SEGG_1_HOUR_AggregateJob
Perf_VOLTE_SEGG_1_HOUR_AggregateJob
List of Failed Jobs:
Perf_VOLTE_SEGG_1_HOUR_AggregateJob
Perf_SMS_SEGG_1_HOUR_AggregateJob
"""
        self.assertEqual(response["body"], expected)

    def test_create_body_when_no_exec_durn_breached_and_with_failed_jobs(self):
        item = _Util.create_item()
        trigger_names = [
            "failed_jobs_trigger_1", "failed_jobs_trigger_2",
            "last_jobs_trigger_0"
        ]
        triggers = _Util._to_triggers_dict(trigger_names, item)
        response = _CustomEmailResponseGenerator(
            TestCustomEmailResponseGenerator.SUBJECT,
            TestCustomEmailResponseGenerator.BODY, triggers).get_response()
        expected = """List of Failed Jobs:
Perf_VOLTE_SEGG_1_HOUR_AggregateJob
Perf_SMS_SEGG_1_HOUR_AggregateJob
"""
        self.assertEqual(response["body"], expected)


class TestAggregationActor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    @patch('healthmonitoring.framework.actors.aggregation.EmailActor')
    def test_act(self, MockEmailActor):
        SUBJECT = "Aggregation Status"
        BODY = """List of Failed Jobs:
Perf_VOLTE_SEGG_1_HOUR_AggregateJob
Perf_SMS_SEGG_1_HOUR_AggregateJob
"""
        item = _Util.create_item()
        triggers = {
            "failed_jobs_trigger_1":
            _Util.create_trigger("failed_jobs_trigger_1", item),
            "failed_jobs_trigger_2":
            _Util.create_trigger("failed_jobs_trigger_2", item),
            "last_jobs_trigger_0":
            _Util.create_trigger("last_jobs_trigger_0", item),
            "hour_jobs_failed_trigger":
            _Util.create_trigger("hour_jobs_failed_trigger", item)
        }
        event = Event(config.events_spec.get_event("event8"),
                      Severity.CRITICAL, triggers)
        notification_content = {'subject': SUBJECT, 'body': "", 'event': event}
        AggregationActor().act(notification_content)
        MockEmailActor().act.assert_called_with({
            'subject': SUBJECT,
            'body': BODY
        })


class _Util:

    @staticmethod
    def create_item():
        params = {
            "hour_agg_dict_ds": [{
                'jobname': 'Perf_VOICE_SEGG_1_HOUR_AggregateJob',
                'start_time': '2020-02-27 12:00:00',
                'status': 'S'
            }, {
                'jobname': 'Perf_VOLTE_SEGG_1_HOUR_AggregateJob',
                'start_time': '2020-02-27 13:00:00',
                'status': 'E'
            }, {
                'jobname': 'Perf_SMS_SEGG_1_HOUR_AggregateJob',
                'start_time': '2020-02-27 14:00:00',
                'status': 'E'
            }]
        }
        app = Application("job_monitoring")
        item_spec = ItemSpecification("hour_aggregations", "localhost",
                                      {'hour_agg_dict_ds': ""}, "", app)
        app.add_item(item_spec)
        return Item(item_spec, **params)

    @staticmethod
    def create_trigger(trigger_name, item):
        trigger_spec = TriggerSpecification(trigger_name, {}, "")
        trigger = Trigger(trigger_spec)
        trigger.item = item
        return trigger

    @staticmethod
    def create_triggers():
        trigger2 = Trigger(config.triggers_spec.get_trigger("trigger2"))
        trigger2.item = Item(config.items_spec.get_item("application1.item1"),
                             count=10,
                             type=1)
        trigger3 = Trigger(config.triggers_spec.get_trigger("trigger3"))
        trigger3.item = Item(config.items_spec.get_item("container.item1"),
                             value1=1)
        return {'trigger2': trigger2, 'trigger3': trigger3}

    @staticmethod
    def _to_triggers_dict(trigger_names, item):
        triggers = {}
        for trigger_name in trigger_names:
            triggers[trigger_name] = _Util.create_trigger(trigger_name, item)
        return triggers


if __name__ == "__main__":
    unittest.main()

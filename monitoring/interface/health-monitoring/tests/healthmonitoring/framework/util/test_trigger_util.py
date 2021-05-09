'''
Created on 22-May-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework.specification.defs import Application, \
    ItemSpecification, TriggerSpecification, EventSpecification, Severity
from healthmonitoring.framework.store import Item, Trigger
from healthmonitoring.framework.util.trigger_util import TriggerUtil


class TestTriggerUtil(unittest.TestCase):

    def test_is_index_trigger_when_true(self):
        self.assertTrue(TriggerUtil.is_index_trigger("trigger_1"))

    def test_is_index_trigger_when_false(self):
        self.assertFalse(TriggerUtil.is_index_trigger("trigger"))

    def test_remove_suffix_from_trigger_name(self):
        self.assertEqual(
            TriggerUtil.remove_suffix_from_trigger_name("trigger_1"),
            "trigger")

    def test_get_specific_trigger_list(self):
        trigger_list = [
            "trigger1_1", "trigger1_2", "trigger1", "trigger2_1", "trigger2_2",
            "trigger2"
        ]
        self.assertListEqual(
            TriggerUtil.get_specific_trigger_list("trigger1", trigger_list),
            ["trigger1_1", "trigger1_2", "trigger1"])

    def test_get_index_of_trigger(self):
        trigger_name = 'trigger_1'
        self.assertEqual(TriggerUtil.get_index_of_trigger(trigger_name), 1)

    def test_get_param_name_list(self):
        param = "['SUMMARY THRESHOLDS'][\"WeekJob\"]['value (\"/temp/\")']"
        expected_param_names = \
            ['SUMMARY THRESHOLDS', 'WeekJob', 'value ("/temp/")']
        self.assertEqual(TriggerUtil.get_param_name_list(param),
                         expected_param_names)

    def test_evaluate_dict(self):
        item_params = {
            "SUMMARYTHRESHOLDS": {
                "WeekJob": {
                    "value": 10
                    }
                }
            }

        self.assertEqual(TriggerUtil.evaluate_dict(
            item_params, ['SUMMARYTHRESHOLDS', 'WeekJob', 'value']), 10)

    def test_get_referenced_triggers(self):
        CONDITION = "t1 and (t2==t3 or t4) and    not t2"
        expected = {"t1", "t2", "t3", "t4"}
        self.assertSetEqual(TriggerUtil.get_referenced_triggers(CONDITION),
                            expected)

    def test_get_failures_list(self):
        app = Application("appname")
        item_spec = ItemSpecification("item1", "host1", {
            'table': 'cmd1'
        }, "", app)
        params = {'table': [{'Name': 'A', 'Value': 20},
                            {'Name': 'B', 'Value': 80},
                            {'Name': 'C', 'Value': 40}]}
        item = Item(item_spec, **params)
        trigger_variables = {
            'var1': "${appname.item1['table']['Value']}"
            }
        TRIGGER_CONDITION = "{var1} < 50"
        trigger = Trigger(TriggerSpecification(
            "triggerA", trigger_variables, TRIGGER_CONDITION))
        trigger.item = item
        trigger_0 = Trigger(TriggerSpecification(
            "triggerA_0", trigger_variables, TRIGGER_CONDITION))
        trigger_0.item = item
        trigger_2 = Trigger(TriggerSpecification(
            "triggerA_2", trigger_variables, TRIGGER_CONDITION))
        trigger_2.item = item
        event_spec = EventSpecification("eventname")
        event_spec.condition = "triggerA"
        event = Event(event_spec, Severity.CRITICAL,
                      {'triggerA': trigger,
                       'triggerA_0': trigger_0,
                       'triggerA_2': trigger_2
                       })

        expected = [{'Name': 'A', 'Value': 20},
                    {'Name': 'C', 'Value': 40}]
        actual = TriggerUtil.get_failures_list(event)

        self.assertEqual(actual, expected)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

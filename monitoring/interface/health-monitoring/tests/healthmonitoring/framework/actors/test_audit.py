'''
Created on 21-Apr-2020

@author: nageb
'''
import unittest
from mock import patch, MagicMock

from healthmonitoring.framework import config, runner
from healthmonitoring.framework.actor import Actor
from healthmonitoring.framework.analyzer import Analyzer
from healthmonitoring.framework.specification.defs import TriggerSpecification
from healthmonitoring.framework.store import Trigger

from tests.healthmonitoring.framework.utils import TestUtil
from healthmonitoring.framework.util import file_util


class Test(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        TestUtil.load_specification()
        runner.create_audit_reports(file_util.get_resource_path("."))
        self.analyzer = Analyzer(Actor(), config.events_spec)
        self._create_triggers()

    def _create_triggers(self):
        self.triggers = {}
        for trigger_name in [
                "node_down_trigger", "high_cpu_usage_trigger",
                "hadoop_access_slow_trigger",
                "too_many_name_node_entries_trigger"
        ]:
            trigger = Trigger(TriggerSpecification(trigger_name, {}, ""))
            self.triggers[trigger_name] = trigger

    @patch('healthmonitoring.framework.actors.audit.AuditActor')
    @patch('healthmonitoring.framework.analyzer.Event.__eq__',
           MagicMock(return_value=True))
    def test_when_audit_action_fired_act_is_called(self, MockAuditActor):
        trigger = self.triggers['node_down_trigger']
        trigger.state = True
        self.analyzer.inject_trigger(trigger)

        notification_content = {
            "event": None,
            "report": "standard",
            "category": "Hardware, Network, OS",
            "name": "Check if all Data nodes are UP",
        }
        MockAuditActor().act.assert_called_with(notification_content)

    def test_setting_of_result(self):
        trigger = self.triggers['high_cpu_usage_trigger']
        trigger.state = True

        category = config.report_store.get('standard').category_list[0]
        item = category.checklist[1]

        self.assertTrue(item.result)
        self.assertTrue(category.result)

        self.analyzer.inject_trigger(trigger)
        self.assertFalse(item.result)
        self.assertFalse(category.result)

    def test_resetting_of_result(self):
        trigger = self.triggers['high_cpu_usage_trigger']

        category = config.report_store.get('standard').category_list[0]
        item = category.checklist[1]

        item.result = False
        category.recompute()

        self.assertFalse(item.result)
        self.assertFalse(category.result)

        trigger.state = True
        self.analyzer.inject_trigger(trigger)
        trigger.state = False
        self.analyzer.inject_trigger(trigger)

        self.assertTrue(item.result)
        self.assertTrue(category.result)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

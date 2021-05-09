import unittest
import subprocess

from mock import patch

import healthmonitoring

from tests.healthmonitoring.framework.utils import TestUtil
from healthmonitoring.framework.specification.defs import Application, \
    ItemSpecification, TriggerSpecification, Severity
from healthmonitoring.framework.store import Item, Trigger
from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework import config
from healthmonitoring.framework.actors.restarters.topology import \
    _TopologyServiceRestarter


class UserDefinedMockedSubprocess:
    _output = ()
    _command = ""
    PIPE = '|'

    def Popen(self, *args, **kwargs):
        UserDefinedMockedSubprocess._command = args[0]
        if "'SMS'" in UserDefinedMockedSubprocess._command:
            UserDefinedMockedSubprocess._output = ('US_SMS_1', True)
            return UserDefinedMockedSubprocess()
        else:
            UserDefinedMockedSubprocess._output = ('US_VOICE_1', True)
            return UserDefinedMockedSubprocess()

    def communicate(self):
        return UserDefinedMockedSubprocess._output

    @staticmethod
    def get_command():
        return UserDefinedMockedSubprocess._command


class TestTopologyRestarter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def test_restart(self, mock_get_config_dir_path, mock_repopulate_hosts,
                     mock_get_config_map_time_stamp):
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            UserDefinedMockedSubprocess()
        service_restarter = self._get_service_restarter_object()
        service_restarter.restart()
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            subprocess
        self.assertTrue(True)

    def test_get_item_value(self):
        service_restarter = self._get_service_restarter_object()
        trigger_names = [
            "topology_failed_trigger_0", "topology_failed_trigger_1"
        ]
        expected_trigger_names = ['SMS_1_SINK_CONN', 'VOICE_1_SINK_CONN']
        actual_trigger_names = []
        for trigger_name in trigger_names:
            actual_trigger_names.append(
                service_restarter._get_item_value(trigger_name))
        self.assertEqual(actual_trigger_names, expected_trigger_names)

    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def test_get_adaptation_name(self, mock_get_config_dir_path,
                                 mock_repopulate_hosts,
                                 mock_get_config_map_time_stamp):
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            UserDefinedMockedSubprocess()
        service_restarter = self._get_service_restarter_object()
        failed_topology_names = ["SMS", "VOICE"]
        expected_adaptation_names = ['US_SMS_1', 'US_VOICE_1']
        actual_adaptation_names = []
        for failed_topology_name in failed_topology_names:
            actual_adaptation_names.append(
                service_restarter._get_adaptation_name(failed_topology_name))
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            subprocess
        self.assertEqual(actual_adaptation_names, expected_adaptation_names)

    def test_start_topology(self):
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            UserDefinedMockedSubprocess()
        service_restarter = self._get_service_restarter_object()
        service_restarter._start_topology("US_SMS_1", "SMS")
        expected_command = "/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -adaptation US_SMS_1 -version 1 -topologyname SMS"  # noqa: 501
        actual_command = UserDefinedMockedSubprocess.get_command()
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            subprocess
        self.assertEqual(actual_command, expected_command)

    def test_stop_topology(self):
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            UserDefinedMockedSubprocess()
        service_restarter = self._get_service_restarter_object()
        service_restarter._stop_topology("SMS")
        expected_command = "/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -kill SMS"  # noqa: 501
        actual_command = UserDefinedMockedSubprocess.get_command()
        healthmonitoring.framework.actors.restarters.topology.subprocess = \
            subprocess
        self.assertEqual(actual_command, expected_command)

    def _get_service_restarter_object(self):
        notification_content = {}
        trigger_names = [
            "topology_failed_trigger_0", "topology_failed_trigger_1"
        ]
        triggers = {}
        for trigger_name in trigger_names:
            triggers[trigger_name] = self._create_trigger(trigger_name)
        event = Event(config.events_spec.get_event("event8"),
                      Severity.CRITICAL, triggers)
        notification_content['event'] = event
        return _TopologyServiceRestarter(notification_content)

    def _create_trigger(self, trigger_name):
        params = {
            "topologies": [{
                'Time': '2020-05-07 19:15:32',
                'Connector_Type': 'SMS_1_SINK_CONN',
                'Connector_Status': 'Stopped',
                'Tasks_Status': 'Stopped',
                'Active_Tasks': '0',
                'Connector_Running_On_Node': 'N/A',
                'Tasks_Running_on_Nodes': '0',
                'Total_Tasks': '0'
            }, {
                'Time': '2020-05-07 19:18:32',
                'Connector_Type': 'VOICE_1_SINK_CONN',
                'Connector_Status': 'Stopped',
                'Tasks_Status': 'Stopped',
                'Active_Tasks': '0',
                'Connector_Running_On_Node': 'N/A',
                'Tasks_Running_on_Nodes': '0',
                'Total_Tasks': '0'
            }]
        }
        app = Application("topology_monitoring")
        item_spec = ItemSpecification("topologies", "localhost",
                                      {'topologies': ""}, "", app)
        app.add_item(item_spec)
        trigger_spec = TriggerSpecification(trigger_name, {}, "")
        trigger = Trigger(trigger_spec)
        trigger.item = Item(item_spec, **params)
        return trigger


if __name__ == "__main__":
    unittest.main()

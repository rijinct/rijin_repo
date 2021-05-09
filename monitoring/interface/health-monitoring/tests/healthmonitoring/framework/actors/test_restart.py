'''
Created on 07-Feb-2020

@author: nageb
'''

import unittest

from mock import patch

from healthmonitoring.framework.actors.restart import \
    RestartActor, _ServiceRestarter

from tests.healthmonitoring.framework.utils import TestUtil
from healthmonitoring.framework.actors.restarters.topology import \
    _TopologyServiceRestarter
from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework.specification.defs import Severity
from healthmonitoring.framework import config


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestRestartActor(unittest.TestCase):
    @classmethod
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def setUpClass(cls, mock_get_config_dir_path, mock_repopulate_hosts,
                   mock_get_config_map_time_stamp):
        TestUtil.load_specification()

    def _test_command_for_service(self, service, expected_command):
        restart_actor = RestartActor()
        f = _ServiceRestarter._run_command
        _ServiceRestarter._run_command = lambda x: None
        restart_actor.act({"service_name": service})
        _ServiceRestarter._run_command = f
        self.assertEqual(restart_actor.service_restarter.command_to_run,
                         expected_command)

    def test_platform_restart_command(self, mock_get_config_dir_path,
                                      mock_repopulate_hosts,
                                      mock_get_config_map_time_stamp):
        expected_command = 'ssh h1 "/opt/nsn/ngdb/ifw/bin/platform/platform_services_controller.pl --service spark --level restart"'  # noqa: E501
        self._test_command_for_service("spark", expected_command)

    def test_application_restart_command(self, mock_get_config_dir_path,
                                         mock_repopulate_hosts,
                                         mock_get_config_map_time_stamp):
        expected_command = 'ssh h4 "/opt/nsn/ngdb/ifw/bin/application/cem/application_services_controller.pl --service scheduler --level stop"'  # noqa: E501
        self._test_command_for_service("scheduler", expected_command)

    def test_portal_restart_command(self, mock_get_config_dir_path,
                                    mock_repopulate_hosts,
                                    mock_get_config_map_time_stamp):
        expected_command = 'ssh p1 "/home/portal/ifw/install/scripts/portal_services_controller.pl --service tomcat --level stop"'  # noqa: E501
        self._test_command_for_service("tomcat", expected_command)

    def test_topology_restart_command(self, mock_get_config_dir_path,
                                      mock_repopulate_hosts,
                                      mock_get_config_map_time_stamp):
        restart_actor = RestartActor()
        f = _TopologyServiceRestarter.restart
        _TopologyServiceRestarter.restart = lambda x: None
        event = Event(config.events_spec.get_event("event1"), Severity.MINOR,
                      [])
        restart_actor.act({"service_name": "topology", 'event': event})
        _ServiceRestarter._run_command = f
        self.assertIsInstance(restart_actor.service_restarter,
                              _TopologyServiceRestarter)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

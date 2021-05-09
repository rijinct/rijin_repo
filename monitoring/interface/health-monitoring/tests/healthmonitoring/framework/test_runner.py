'''
Created on 17-Mar-2020

@author: nageb
'''
import os
import unittest
from pathlib import Path

from mock import patch

from healthmonitoring.framework import config
from healthmonitoring.framework import runner
from healthmonitoring.framework.monitor import Monitor
from healthmonitoring.framework.scheduler import NowScheduler
import healthmonitoring.framework.scheduler
from healthmonitoring.framework.store import Store
from healthmonitoring.framework.util import file_util
from healthmonitoring.framework.specification.defs import DBType


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestRunner(unittest.TestCase):
    monitor = None

    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def setUp(self, mock_get_config_dir_path, mock_repopulate_hosts,
              mock_get_config_map_time_stamp):
        file_util.under_test = True
        path = file_util.get_resource_path('monitoring.properties')
        runner.load_config(path)
        runner.load_all_specs(path.parent, path.parent)

    def test_launch_workflow(self, mock_get_config_dir_path,
                             mock_repopulate_hosts,
                             mock_get_config_map_time_stamp):
        class MockMonitor(Monitor):
            def run(self):
                TestRunner.monitor = self

        def mock_create_monitor(store):
            scheduler_name = config.properties.get("SchedulerSection",
                                                   "Scheduler")
            scheduler = getattr(healthmonitoring.framework.scheduler,
                                scheduler_name)(store)
            return MockMonitor(store, scheduler)

        runner._create_monitor = mock_create_monitor
        runner.launch_workflow()

        self.assertIsNotNone(TestRunner.monitor)
        self.assertIsNotNone(TestRunner.monitor._store)
        self.assertIsNotNone(TestRunner.monitor._store._analyzer)

    @patch.dict(os.environ, {'DB_TYPE': 'postgres_sdk'})
    @patch('healthmonitoring.framework.runner.ConnectionFactory')
    def test_load_db_driver(self, MockConnectionFactory,
                            mock_get_config_dir_path, mock_repopulate_hosts,
                            mock_get_config_map_time_stamp):
        runner.load_db_driver()
        MockConnectionFactory.instantiate_connection.\
            assert_called_with(DBType.POSTGRES_SDK)

    def test_create_audit_reports(self, mock_get_config_dir_path,
                                  mock_repopulate_hosts,
                                  mock_get_config_map_time_stamp):
        runner.create_audit_reports(file_util.get_resource_path("."))
        self.assertListEqual(list(config.report_store.get_names()),
                             ['standard', 'summary'])

    def test_create_monitor(self, mock_get_config_dir_path,
                            mock_repopulate_hosts,
                            mock_get_config_map_time_stamp):
        store = Store()
        monitor = runner._create_monitor(store)
        self.assertEqual(monitor._store, store)
        self.assertIsInstance(monitor._scheduler, NowScheduler)

    @patch('healthmonitoring.framework.config.properties')
    def test_empty_priority_dir_loading(self, mock_get_config_dir_path,
                                        mock_repopulate_hosts,
                                        mock_get_config_map_time_stamp,
                                        MockConfigProperties):
        MockConfigProperties.get.return_value = ""
        actual = runner._get_priority_directories("", "")
        self.assertFalse(bool(actual))

    @patch('healthmonitoring.framework.config.properties.get',
           return_value="standard/healthmonitoring,standard/portal")
    def test_multiple_priority_dir_loading(self, mock_get,
                                           mock_get_config_dir_path,
                                           mock_repopulate_hosts,
                                           mock_get_config_map_time_stamp):
        actual = runner._get_priority_directories(Path('resources/'),
                                                  Path('.'))
        self.assertTrue(len(actual) == 2)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

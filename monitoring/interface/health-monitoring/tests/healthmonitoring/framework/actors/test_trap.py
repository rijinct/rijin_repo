'''
Created on 07-Feb-2020

@author: nageb
'''

import unittest
from mock import patch

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.trap import TrapActor, TrapMedia, \
    TrapAction
from healthmonitoring.framework.actors import trap

from tests.healthmonitoring.framework.utils import TestUtil
from healthmonitoring.framework.util.specification import SpecificationUtil


class TestSubprocessTrueCondition:

    PIPE = 'pipe'

    @staticmethod
    def Popen(*args, stdout):
        return TestPopen(0)


class TestSubprocessFalseCondition:

    PIPE = 'pipe'

    @staticmethod
    def Popen(*args, stdout):
        return TestPopen(1)


class TestPopen:
    def __init__(self, returncode):
        self._returncode = returncode

    @property
    def returncode(self):
        return self._returncode


class MockSchedulerJobFailure:
    def get_class_name(self):
        return 'MockSchedulerJobFailure'


class TestTrapMedia(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        trap.hosts_spec = config.hosts_spec

    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
        return_value="./../../resources")
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts'  # noqa: E501
    )
    @patch(
        'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
        return_value=0)
    def test_init(self, mock_get_config_dir_path, mock_repopulate_hosts,
                  mock_get_config_map_time_stamp):
        snmp_details = SpecificationUtil.get_host('snmp')
        trap_media = TrapMedia()
        self.assertEqual(snmp_details.fields['cemod_snmp_ip'],
                         trap_media.snmp_ip)
        self.assertEqual(snmp_details.fields['cemod_snmp_port'],
                         trap_media.snmp_port)
        self.assertEqual(snmp_details.fields['cemod_snmp_community'],
                         trap_media.snmp_community)


class TestTrapActor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        trap.hosts_spec = config.hosts_spec

    def test_get_class_name(self):
        module_name = 'scheduler_jobs_failure'
        expected_class_name = 'SchedulerJobsFailure'
        actual_class_name = TrapActor._get_class_name(module_name)
        self.assertEqual(expected_class_name, actual_class_name)

    def test_get_trap_action_instance(self):
        module_name = 'test_trap'
        class_name = 'MockSchedulerJobFailure'
        action = TrapActor._get_trap_action_instance(
            "tests.healthmonitoring.framework.actors.{}".format(module_name),
            class_name)
        self.assertEqual(class_name, action.get_class_name())

    def test_get_return_code_for_true_condition(self):
        trap.subprocess = TestSubprocessTrueCondition
        action = TrapAction()
        return_code = action._get_return_code("")
        self.assertEqual(return_code, 0)

    def test_get_return_code_for_false_condition(self):
        trap.subprocess = TestSubprocessFalseCondition
        action = TrapAction()
        return_code = action._get_return_code("")
        self.assertEqual(return_code, 1)

    def test_execute(self):
        trap.subprocess = TestSubprocessTrueCondition
        action = TrapAction()
        action.notification_content = {}
        action.execute()
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()

import unittest
import copy

import healthmonitoring.framework
from healthmonitoring.framework import config
from healthmonitoring.framework.store import Item, Trigger
from healthmonitoring.framework.analyzer import Event
from healthmonitoring.framework.specification.defs import Severity
from healthmonitoring.framework.actors.traps.scheduler_jobs_failure import \
    SchedulerJobsFailure
from healthmonitoring.framework.actors import trap
from healthmonitoring.framework.actors.trap import TrapActor

from tests.healthmonitoring.framework.utils import TestUtil


class TestSubprocess:

    PIPE = 'pipe'

    @staticmethod
    def Popen(*args, stdout):
        return TestPopen(0)


class TestPopen:

    def __init__(self, returncode):
        self._returncode = returncode

    @property
    def returncode(self):
        return self._returncode


class TestSchedulerJobsFailure(unittest.TestCase):

    PARAMS = {'aggregation_job_status': [{
                    'job_name': 'ps_cei2_o_index_2_1_day_aggregateJob',
                    'status': 'E'},
                {
                    'job_name': 'ps_cei_index_2_1_day_aggregateJob',
                    'status': 'E'},
                {
                    'job_name': 'ps_sms_index_2_1_day_aggregateJob',
                    'status': 'R'}]}

    EXPECTED_FAILED_JOBS = ", ".join(['ps_cei2_o_index_2_1_day_aggregateJob',
                                      'ps_cei_index_2_1_day_aggregateJob'])

    EXPECTED_JOB_TYPE = 'DAY'

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        healthmonitoring.framework.actors.trap.hosts_spec = \
            config.hosts_spec

    def _get_trigger(self, params, state):
        item = Item({}, **params)
        trigger = Trigger({})
        trigger.state = state
        trigger.item = item
        return trigger

    def _get_notification_content(self):
        triggers = {
            "aggregation_job_failure_0": self._get_trigger(
                TestSchedulerJobsFailure.PARAMS, True),
            "aggregation_job_failure_1": self._get_trigger(
                TestSchedulerJobsFailure.PARAMS, False),
            "aggregation_job_failure": self._get_trigger(
                TestSchedulerJobsFailure.PARAMS, True)}
        notification_content = {
            'trigger_name': 'aggregation_job_failure',
            'module_name': 'scheduler_jobs_failure',
            'event': Event({}, Severity.CRITICAL, triggers)}
        return notification_content

    def test_get_failed_job_names(self):
        action = SchedulerJobsFailure()
        action.notification_content = self._get_notification_content()
        failed_job_names = action._get_failed_job_names()
        self.assertEqual(failed_job_names,
                         TestSchedulerJobsFailure.EXPECTED_FAILED_JOBS)

    def test_get_failed_job_names_as_empty(self):
        triggers = {
            "aggregation_job_failure_0": self._get_trigger(
                {}, True)}
        notification_content = {
            'trigger_name': 'aggregation_job_failure',
            'event': Event({}, Severity.CRITICAL, triggers)}
        action = SchedulerJobsFailure()
        action.notification_content = notification_content
        failed_job_names = action._get_failed_job_names()
        self.assertEqual(failed_job_names, '')

    def test_get_none_job_name(self):
        action = SchedulerJobsFailure()
        action.notification_content = {
            'trigger_name': 'aggregation_job_failure',
            'event': Event({}, Severity.CRITICAL, {})}
        failed_job_names = action._get_job_name()
        self.assertIsNone(failed_job_names, None)

    def test_get_job_name_as_empty(self):
        action = SchedulerJobsFailure()
        updated_params = copy.deepcopy(TestSchedulerJobsFailure.PARAMS)
        updated_params.update(
            {'aggregation_job_value': [
                {'job_name': 'ps_cei2_o_index_2_1_day_aggregateJob',
                 'value': 'true'}]})
        triggers = {
            "aggregation_job_failure_0": self._get_trigger(
                updated_params, True)
            }
        action.notification_content = {
            'trigger_name': 'aggregation_job_failure',
            'event': Event({}, Severity.CRITICAL, triggers)}
        failed_job_names = action._get_job_name()
        self.assertIsNone(failed_job_names, None)

    def test_get_job_type(self):
        action = SchedulerJobsFailure()
        action.notification_content = self._get_notification_content()
        job_type = action._get_job_type()
        self.assertEqual(job_type, TestSchedulerJobsFailure.EXPECTED_JOB_TYPE)

    def _get_none_job_type_notification_content(self):
        params = {'aggregation_job_status': [{
                    'job_name': 'ps_cei2_o_index_2_1_aggregateJob',
                    'status': 'E'}]}
        triggers = {
            "aggregation_job_failure_0": self._get_trigger(
                params, True)}
        notification_content = {
            'trigger_name': 'aggregation_job_failure',
            'event': Event({}, Severity.CRITICAL, triggers)}
        return notification_content

    def test_get_none_job_type(self):
        action = SchedulerJobsFailure()
        action.notification_content = self.\
            _get_none_job_type_notification_content()
        job_type = action._get_job_type()
        self.assertIsNone(job_type)

    def test_get_action_specific_conf(self):
        expected_action_specific_conf = 'SAI-MIB::schedulerJobsFailure SAI-MIB::jobNames s "{}" SAI-MIB::type s "{}"'.format(TestSchedulerJobsFailure.EXPECTED_FAILED_JOBS, TestSchedulerJobsFailure.EXPECTED_JOB_TYPE)  # noqa
        action = SchedulerJobsFailure()
        action.notification_content = self._get_notification_content()
        action_specific_conf = action.get_action_specific_conf()
        self.assertEqual(action_specific_conf, expected_action_specific_conf)

    def test_execute(self):
        notification_content = self._get_notification_content()
        trap.subprocess = TestSubprocess
        trap_actor = TrapActor()
        trap_actor.act(notification_content)
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()

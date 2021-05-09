from builtins import staticmethod

from logger import Logger

from healthmonitoring.framework import config
from healthmonitoring.framework.actors.trap import TrapAction
from healthmonitoring.framework.util.trigger_util import TriggerUtil
from healthmonitoring.framework.util.collection_util import \
    get_lone_value

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class SchedulerJobsFailure(TrapAction):
    EVENT_KEY = 'event'
    JOB_NAME_KEY = 'job_name'
    TYPE_DICT = {
        '15MIN': ['15MIN_AGGREGATEJOB', '15MIN_EXPORTJOB'],
        'HOUR': ['HOUR_AGGREGATEJOB', 'HOUR_EXPORTJOB'],
        'DAY': ['DAY_AGGREGATEJOB', 'DAY_EXPORTJOB'],
        'WEEK': ['WEEK_AGGREGATEJOB', 'WEEK_EXPORTJOB'],
        'MONTH': ['MONTH_AGGREGATEJOB', 'MONTH_EXPORTJOB'],
        'USAGE': ['_LOADJOB'],
        'REAGG': ['REAGGREGATEJOB']
    }
    TRIGGER_NAME_KEY = 'trigger_name'

    def get_action_specific_conf(self):
        return (
            'SAI-MIB::schedulerJobsFailure SAI-MIB::jobNames s "{}" SAI-MIB::type s "{}"'  # noqa: E501
            .format(self._get_failed_job_names(), self._get_job_type()))

    def _get_job_type(self):
        job_name = self._get_job_name()
        for key, values in SchedulerJobsFailure.TYPE_DICT.items():
            if SchedulerJobsFailure._job_name_exists(job_name, values):
                return key

    @staticmethod
    def _job_name_exists(job_name, values):
        for value in values:
            if job_name.endswith(value):
                return True
        return False

    @staticmethod
    def _get_job_detail(triggers, trigger_name_specific_list):
        for trigger_name in trigger_name_specific_list:
            params = triggers[trigger_name].item.params
            return SchedulerJobsFailure._get_job_detail_from_params(params)

    @staticmethod
    def _get_job_detail_from_params(params):
        try:
            job_details = get_lone_value(params)
            for job_detail in job_details:
                return job_detail
        except ValueError as e:
            logger.error(e)

    def _get_job_name(self):
        triggers = self.notification_content[
            SchedulerJobsFailure.EVENT_KEY].triggers
        trigger_name_specific_list = self._get_trigger_name_specific_list(
            triggers)
        job_detail = SchedulerJobsFailure._get_job_detail(
            triggers, trigger_name_specific_list)
        if job_detail:
            return job_detail[SchedulerJobsFailure.JOB_NAME_KEY].upper()

    def _get_failed_job_names(self):
        triggers = self.notification_content[
            SchedulerJobsFailure.EVENT_KEY].triggers
        trigger_name_specific_list = self._get_trigger_name_specific_list(
            triggers)
        return ", ".join(
            self._get_failed_jobs(triggers, trigger_name_specific_list))

    def _get_trigger_name_specific_list(self, triggers):
        trigger_name = self.notification_content[
            SchedulerJobsFailure.TRIGGER_NAME_KEY]
        trigger_name_specific_list = TriggerUtil.get_specific_trigger_list(
            trigger_name, triggers.keys())
        if trigger_name in trigger_name_specific_list:
            trigger_name_specific_list.remove(trigger_name)
        return trigger_name_specific_list

    def _get_failed_jobs(self, triggers, trigger_name_specific_list):
        failed_jobs = []
        for trigger_name in trigger_name_specific_list:
            trigger = triggers[trigger_name]
            params = trigger.item.params
            failed_job_index = TriggerUtil.get_index_of_trigger(trigger_name)
            self._update_failed_jobs(failed_jobs, params, failed_job_index)
        return failed_jobs

    def _update_failed_jobs(self, failed_jobs, params, failed_job_index):
        try:
            job_details = get_lone_value(params)
            failed_jobs.append(job_details[failed_job_index][
                SchedulerJobsFailure.JOB_NAME_KEY])
        except ValueError as e:
            logger.error(e)

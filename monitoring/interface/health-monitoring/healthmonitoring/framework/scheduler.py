import calendar
import re
import threading

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from logger import Logger

from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class _Scheduler:

    def __init__(self, store):
        self._store = store
        self._schedule = None
        self._function = None

    @property
    def schedule(self):
        return self._schedule

    def execute_job(self, obj, function):
        logger.info("Executing item '%s'", obj)
        for host, item in function(obj).items():
            item.params["host"] = host
            self._store.add_item(item)


class NowScheduler(_Scheduler):

    def __init__(self, store):
        super().__init__(store)
        self.jobs = []

    def add_job(self, obj, function):
        self.jobs.append((obj, function))

    def run(self):
        for job, function in self.jobs:
            self.execute_job(job, function)


class CronScheduler(_Scheduler):
    _launched_threads, _terminated_threads = set(), set()
    _lock = threading.Lock()

    def __init__(self, store):
        super().__init__(store)
        self._blocking_scheduler = BlockingScheduler()

    def add_job(self, obj, function):
        cron_expr = AmacronConverter.convert(obj.schedule)
        logger.debug("Cron Expression is '%s' and name of the item is '%s' ",
                     cron_expr, obj)
        self._blocking_scheduler.add_job(self.execute_job,
                                         CronTrigger.from_crontab(cron_expr),
                                         kwargs={
                                             'obj': obj,
                                             'function': function
                                         })

    def run(self):
        try:
            self._blocking_scheduler.start()
        except KeyboardInterrupt:
            logger.info("Key-board Interrupt Occurred")
        finally:
            self._blocking_scheduler.shutdown()

    def execute_job(self, obj, function):
        if config.properties["SchedulerSection"]["Multithreading"] == "yes":
            self._execute_job_using_thread(obj, function)
        else:
            _Scheduler.execute_job(self, obj, function)

    def _execute_job_using_thread(self, obj, function):
        thread = threading.Thread(target=self._execute_job_inside_thread,
                                  name=str(obj),
                                  kwargs={
                                      'obj': obj,
                                      'function': function
                                  },
                                  daemon=True)
        CronScheduler._add_thread(thread)
        thread.start()
        thread_count = threading.active_count()
        CronScheduler._join_terminated_threads()
        logger.info("Converted %d active threads to %s by joining",
                    thread_count, threading.active_count())

    def _execute_job_inside_thread(self, obj, function):
        logger.info("Launched thread for '%s'", obj)
        _Scheduler.execute_job(self, obj, function)
        CronScheduler._terminate_thread(threading.current_thread())
        logger.info("Terminated thread for '%s'", obj)

    @staticmethod
    def _add_thread(thread):
        with CronScheduler._lock:
            CronScheduler._launched_threads.add(thread)

    @staticmethod
    def _terminate_thread(thread):
        with CronScheduler._lock:
            CronScheduler._terminated_threads.add(thread)
            CronScheduler._launched_threads.remove(thread)

    @staticmethod
    def _join_terminated_threads():
        with CronScheduler._lock:
            for thread in CronScheduler._terminated_threads:
                thread.join()
            CronScheduler._terminated_threads = set()


class AmacronConverter:

    @staticmethod
    def convert(amacron):
        classes = [
            '_EveryRegex', '_EveryFromRegex', '_FirstRegex', '_FirstFromRegex',
            '_MinuteAtRegex', '_HourAtRegex', '_DayAtRegex', '_MonthAtRegex'
        ]
        cron = amacron

        for class_name in classes:
            instance = eval('%s()' % class_name)
            if instance.extract_groups(amacron):
                cron = instance.process_condition()
                break

        return cron


class _CronParameters:

    def __init__(self):
        self.minute = self.hour = self.day = self.month = \
            self.day_of_week = '*'
        self.days_name = {
            v.lower(): (k + 1)
            for k, v in enumerate(calendar.day_name)
        }
        self._create_regex_params()

    def extract_groups(self, str_cron):
        match = self.regex.search(str_cron)
        if match:
            self.groups = match.groups()

        return match

    def get_crontab(self):
        return ' '.join(
            [self.minute, self.hour, self.day, self.month, self.day_of_week])

    def _create_regex_params(self):
        self.time_repr = r'([01]?[0-9]|2[0-3]):([0-5][0-9])'
        self.val_repr = r'(month|day|minute|min|hour)'
        self.num_repr = r'([0-9]+)(?:st|nd|rd|th)?'


class _EveryRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(r'^every\s+((\w+)?\s*{0})s?$'.format(
            self.val_repr))

    def process_condition(self):
        for func in ['minute', 'hour', 'day_of_week', 'day', 'month']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        greater_intervals = {'hour', 'day', 'month'}
        if self.groups[2] in greater_intervals:
            self.minute = '0'
        elif 'min' in self.groups[2] and self.groups[1]:
            self.minute = '*/%s' % (self.groups[1])

    def set_hour(self):
        greater_intervals = {'day', 'month'}
        if self.groups[2] in greater_intervals:
            self.hour = '0'
        elif self.groups[2] == 'hour' and self.groups[1]:
            self.hour = '*/%s' % (self.groups[1])
        elif self.groups[2] == 'hour':
            self.hour = '*/1'

    def set_day(self):
        if self.groups[2] == 'month':
            self.day = '1'
        elif self.groups[2] == 'day':
            if self.groups[1] and self.groups[0] not in self.days_name.keys():
                self.day = '*/%s' % (self.groups[1])
            else:
                self.day = '*/1'

    def set_month(self):
        if self.groups[2] == 'month' and self.groups[1]:
            self.month = '*/%s' % (self.groups[1])
        elif self.groups[2] == 'month':
            self.month = '*/1'

    def set_day_of_week(self):
        if self.groups[2] == 'day' and self.groups[0] in self.days_name.keys():
            self.day_of_week = '%s' % (self.days_name[self.groups[0].lower()])


class _EveryFromRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^every\s+((\w+)?\s*{0})s?\s+(from|at)?\s*{1}+(\s+to+\s+{1})?$'.
            format(self.val_repr, self.time_repr))

    def process_condition(self):
        for func in [
                'from_and_to', 'minute', 'hour', 'day_of_week', 'day', 'month'
        ]:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_from_and_to(self):
        if self.groups[6]:
            self.hour = '%s-%s' % (self.groups[4], self.groups[7])
        else:
            self.hour = '%s' % (self.groups[4])

    def set_minute(self):
        self.minute = '%s' % (self.groups[5])
        if 'min' in self.groups[2] and self.groups[1]:
            self.minute += '/%s' % self.groups[1]
        elif 'min' in self.groups[2]:
            self.minute += '/1'

    def set_hour(self):
        if self.groups[2] == 'hour' and self.groups[1]:
            self.hour += '/%s' % (self.groups[1])
        elif self.groups[2] == 'hour':
            self.hour += '/1'

    def set_day(self):
        if self.groups[2] == 'day' and self.groups[1] and self.groups[0].lower(
        ) not in self.days_name.keys():
            self.day = '*/%s' % (self.groups[1])
        elif self.groups[2] == 'day':
            self.day = '*/1'

    def set_month(self):
        if self.groups[2] == 'month' and self.groups[1]:
            self.month = '*/%s' % (self.groups[1])
        elif self.groups[2] == 'month':
            self.month = '*/1'

    def set_day_of_week(self):
        if self.groups[2] == 'day' and self.groups[0].lower(
        ) in self.days_name.keys():
            self.day_of_week = '%s' % (self.days_name[self.groups[0].lower()])


class _FirstRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^first\s+of\s+every\s+((\w+)?\s*(month|week))s?$')

    def process_condition(self):
        for func in ['minute', 'hour', 'day_of_week', 'day', 'month']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        self.minute = '0'

    def set_hour(self):
        self.hour = '0'

    def set_day(self):
        if self.groups[2] == 'month':
            self.day = '1'

    def set_month(self):
        if self.groups[2] == 'month' and self.groups[1]:
            self.month = '*/%s' % (self.groups[1])

    def set_day_of_week(self):
        if self.groups[2] == 'week':
            self.day_of_week = '1'


class _FirstFromRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^first\s+of\s+every\s+((\w+)?\s*(month|week))s?\s+(from|at)?\s*{0}+(\s+to+\s+{0})?$'  # noqa:E501
            .format(self.time_repr))

    def process_condition(self):
        for func in ['minute', 'hour', 'day_of_week', 'day', 'month']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        self.minute = '%s' % (self.groups[5])

    def set_hour(self):
        if self.groups[6]:
            self.hour = '%s-%s' % (self.groups[4], self.groups[7])
        else:
            self.hour = '%s' % (self.groups[4])

    def set_day(self):
        if self.groups[2] == 'month':
            self.day = '1'

    def set_month(self):
        if self.groups[2] == 'month' and self.groups[1]:
            self.month = '*/%s' % (self.groups[1])

    def set_day_of_week(self):
        if self.groups[2] == 'week':
            self.day_of_week = '1'


class _MinuteAtRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(r'^at\s+{0}\s+(min|minute)s?$'.format(
            self.num_repr))

    def process_condition(self):
        for func in ['minute']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        self.minute = '%s' % self.groups[0]


class _HourAtRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^every\s+((\w+)?\s*hour)s?\s+at\s+{0}\s+(minute|min)s?$'.format(
                self.num_repr))

    def process_condition(self):
        for func in ['minute', 'hour']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        self.minute = '%s' % (self.groups[2])

    def set_hour(self):
        if self.groups[1]:
            self.hour = '*/%s' % (self.groups[1])
        else:
            self.hour = '*/1'


class _DayAtRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^every\s+((\w+)?\s*day)s?\s+at\s+{0}\s+(hour|minute|min)s?$'  # noqa:E501
            .format(self.num_repr))

    def process_condition(self):
        for func in ['minute', 'hour', 'day_of_week', 'day']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        if self.groups[3] == 'hour':
            self.minute = '0'
        else:
            self.minute = '%s' % (self.groups[2])

    def set_hour(self):
        if self.groups[3] == 'hour':
            self.hour = '%s' % (self.groups[2])

    def set_day(self):
        if self.groups[1] and self.groups[0].lower(
        ) not in self.days_name.keys():
            self.day = '*/%s' % (self.groups[1])
        elif self.groups[0].lower() not in self.days_name.keys():
            self.day = '*/1'

    def set_day_of_week(self):
        if self.groups[0].lower() in self.days_name.keys():
            self.day_of_week = '%s' % (self.days_name[self.groups[0].lower()])


class _MonthAtRegex(_CronParameters):

    def __init__(self):
        super().__init__()
        self.regex = re.compile(
            r'^every\s+((\w+)?\s*month)s?\s+at\s+{0}\s+{1}s?$'.format(
                self.num_repr, self.val_repr))

    def process_condition(self):
        for func in ['minute', 'hour', 'day', 'month']:
            eval('self.set_%s()' % func)
        return super().get_crontab()

    def set_minute(self):
        greater_intervals = {'hour', 'day', 'month'}
        if self.groups[3] in greater_intervals:
            self.minute = '0'
        else:
            self.minute = '%s' % (self.groups[2])

    def set_hour(self):
        if self.groups[3] == 'hour':
            self.hour = '%s' % (self.groups[2])
        elif self.groups[3] == 'day':
            self.hour = '0'

    def set_day(self):
        if self.groups[3] == 'day':
            self.day = '%s' % (self.groups[2])

    def set_month(self):
        if self.groups[1]:
            self.month = '*/%s' % (self.groups[1])
        else:
            self.month = '*/1'

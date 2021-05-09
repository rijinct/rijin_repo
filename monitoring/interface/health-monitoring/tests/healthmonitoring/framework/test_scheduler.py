'''
Created on 31-Jan-2020

@author: nageb
'''
import time
import unittest

from healthmonitoring.framework import config
from healthmonitoring.framework.scheduler import AmacronConverter, \
    NowScheduler, CronScheduler, _Scheduler

from tests.healthmonitoring.framework.utils import TestUtil


class TestAmacronConverter(unittest.TestCase):

    def test_amacron_to_cron(self):
        test_cases = {
            'every minute': '* * * * *',
            'every 15 minute': '*/15 * * * *',
            'every hour': '0 */1 * * *',
            'every 5 hours': '0 */5 * * *',
            'every 3 days': '0 0 */3 * *',
            'every month': '0 0 1 */1 *',
            'every 6 months': '0 0 1 */6 *',
            'every min from 10:00': '00/1 10 * * *',
            'every 10 mins at 15:00': '00/10 15 * * *',
            'every hour from 10:00': '00 10/1 * * *',
            'every 3 hours from 10:00 to 12:00': '00 10-12/3 * * *',
            'every 5 days at 06:00': '00 06 */5 * *',
            'every month from 12:00': '00 12 * */1 *',
            'every 6 months from 10:00': '00 10 * */6 *',
            'first of every week': '0 0 * * 1',
            'first of every week at 12:00': '00 12 * * 1',
            'first of every month': '0 0 1 * *',
            'first of every 2 months': '0 0 1 */2 *',
            'first of every month from 10:30 to 12:00': '30 10-12 1 * *',
            'first of every 2 month at 06:30': '30 06 1 */2 *',
            'at 23rd minute': '23 * * * *',
            'every hour at 45th minute': '45 */1 * * *',
            'every 5 hours at 22nd min': '22 */5 * * *',
            'every day at 2nd hour': '0 2 */1 * *',
            'every 5 days at 45th minute': '45 * */5 * *',
            'every 2 months at 12th min': '12 * * */2 *',
            'every month at 12th minute': '12 * * */1 *',
            'every month at 9th hour': '0 9 * */1 *',
            'every month at 6th day': '0 0 6 */1 *',
            'every monday': '0 0 */1 * 1',
            'every thursday at 12th min': '12 * * * 4',
            'every tuesday from 12:00': '00 12 */1 * 2',
            'every day from 04:30': '30 04 */1 * *'
        }

        for amacron, expected in test_cases.items():
            actual = AmacronConverter().convert(amacron)
            self.assertEqual(actual, expected, amacron)

    def test_amacron_to_cron_for_failing_cases(self):
        test_cases = {
            'every 5 days from 13:00 12:00': 'every 5 days from 13:00 12:00',
            'every 11 hours at 21st hour': 'every 11 hours at 21st hour',
        }

        for amacron, expected in test_cases.items():
            actual = AmacronConverter().convert(amacron)
            self.assertEqual(actual, expected)


class TestScheduler(unittest.TestCase):

    def test__get_schedule(self):
        SCHEDULE = "every hour"
        scheduler = _Scheduler(store=None)
        scheduler._schedule = SCHEDULE
        self.assertEqual(scheduler.schedule, SCHEDULE)


class TestNowScheduler(unittest.TestCase):

    store = None

    class Store:

        def __init__(self):
            self.result = ""

        def add_item(self, item):
            self.result += item.name

    @staticmethod
    def _function(name):

        class A:

            def __init__(self, name):
                self.name = name
                self.params = {}

            def items(self):
                return [("host", self)]

        return {"host": A(name)}

    def test(self):
        TestNowScheduler.store = TestNowScheduler.Store()
        scheduler = NowScheduler(TestNowScheduler.store)
        scheduler.add_job("A", TestNowScheduler._function)
        scheduler.add_job("B", TestNowScheduler._function)
        scheduler.run()
        self.assertEqual(TestNowScheduler.store.result, "AB")


class TestCronScheduler(unittest.TestCase):

    def setUp(self):
        TestUtil.load_specification()
        self.scheduler = CronScheduler(store=None)
        item = TestCronScheduler.Item()

        self.scheduler._blocking_scheduler = \
            TestCronScheduler.MockBlockingScheduler()
        self.scheduler.add_job(item, self._function)
        self.called = False

    class MockBlockingScheduler:

        def add_job(self, function, schedule, **params):
            self._function = function
            self._params = params

        def start(self):
            self._function(self._params['kwargs']['obj'],
                           self._params['kwargs']['function'])

        def shutdown(self):
            pass

    class Item:

        def __init__(self):
            self.schedule = "every minute"

        def __str__(self):
            return "Test Item"

    def _function(self, item):
        self.called = True
        return {}

    def test_unithreaded(self):
        self.scheduler.run()
        self.assertTrue(self.called)

    def test_multithreaded(self):
        config.properties["SchedulerSection"]["Multithreading"] = "yes"
        self.scheduler.run()
        while self.scheduler._launched_threads:
            time.sleep(0)
        self.assertTrue(self.called)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

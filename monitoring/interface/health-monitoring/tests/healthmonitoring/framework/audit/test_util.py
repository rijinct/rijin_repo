'''
Created on 04-May-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework.audit.report import CheckItem, \
    CategoryReport, Report
from healthmonitoring.framework.audit.util import ReportToEventsSpecConverter
from healthmonitoring.framework.specification.defs import EventsSpecification


class TestReportToEventSpecConverter(unittest.TestCase):

    def test_to_event_spec_when_empty(self):
        report = Report('standard')
        actual_event_spec = ReportToEventsSpecConverter.convert(report)
        expected_event_spec = EventsSpecification({})
        self.assertEqual(actual_event_spec, expected_event_spec)

    def test_to_event_spec_with_filled_categories(self):
        item1 = CheckItem("failure_item", "description", "recommendation",
                          "trigger1")
        category1 = CategoryReport("failure_category")
        category1.add_checklist_item(item1)

        item2 = CheckItem("ok_item", "description", "recommendation",
                          "trigger1")
        category2 = CategoryReport("ok_category")
        category2.add_checklist_item(item2)

        report = Report('standard')
        report.add_category(category1)
        report.add_category(category2)

        actual_event_spec = ReportToEventsSpecConverter.convert(report)
        expected_event_spec = EventsSpecification({
            'audit_standard_event1': {
                'start_level': 'critical',
                'escalation_freq': 99999,
                'condition': 'trigger1',
                'clear': ['audit'],
                'critical': ['audit'],
                'audit_notification': {
                    'report': 'standard',
                    'category': 'failure_category',
                    'name': 'failure_item'
                }
            },
            'audit_standard_event2': {
                'start_level': 'critical',
                'escalation_freq': 99999,
                'condition': 'trigger1',
                'clear': ['audit'],
                'critical': ['audit'],
                'audit_notification': {
                    'report': 'standard',
                    'category': 'ok_category',
                    'name': 'ok_item'
                }
            }
        })
        self.assertEqual(actual_event_spec, expected_event_spec)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

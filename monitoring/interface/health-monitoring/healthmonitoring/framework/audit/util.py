'''
Created on 04-May-2020

@author: nageb
'''
from healthmonitoring.framework.specification.defs import EventsSpecification


class ReportToEventsSpecConverter:

    def __init__(self, report):
        self._report = report
        self._ds = {}
        self._event_spec = {}
        self._count = 0

    @staticmethod
    def convert(report):
        converter = ReportToEventsSpecConverter(report)
        converter._convert()
        return converter._event_spec

    def _convert(self):

        for category_name in self._report.get_category_names():
            self._process_category(self._report.get_category(category_name))

        self._event_spec = EventsSpecification(self._ds)

    def _process_category(self, category_report):
        for item in category_report.checklist:
            event_details = {}
            ESCALATION_FREQ = 99999

            event_details['start_level'] = 'critical'
            event_details['escalation_freq'] = ESCALATION_FREQ
            event_details['condition'] = item.condition
            event_details['clear'] = ['audit']
            event_details['critical'] = ['audit']
            event_details['audit_notification'] = {
                'report': self._report.name,
                'category': category_report.name,
                'name': item.name
                }

            self._ds['audit_{}_event{:d}'.format(
                self._report.name, self._count + 1)] = event_details
            self._count += 1

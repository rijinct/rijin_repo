'''
Created on 22-Apr-2020

@author: nageb
'''

from pathlib import Path

from healthmonitoring.framework.audit.report import Report, CategoryReport, \
    CheckItem
import yaml


class YamlReader:

    def __init__(self, pathname):
        with open(pathname) as file:
            ds = yaml.full_load(file)
        self._report = Report(Path(pathname).stem)
        for category_name, category_details in ds['report'].items():
            self._process_category(category_name, category_details)

    @property
    def report(self):
        return self._report

    def _process_category(self, category_name, category_details):
        category_report = CategoryReport(category_name)
        for item_name, item_details in category_details.items():
            item = CheckItem(item_name,
                             item_details['description'],
                             item_details["recommendation"],
                             item_details["condition"])
            if 'show_columns' in item_details:
                item.show_columns = item_details['show_columns']
            category_report.add_checklist_item(item)
        self.report.add_category(category_report)

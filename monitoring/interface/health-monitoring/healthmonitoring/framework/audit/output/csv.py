'''
Created on 11-Mar-2020

@author: poojabhi
'''
import csv

COLUMN_NAMES = ("Item", "Status", "Description", "Recommendation")

STATUS = {True: "OK", False: "NOT OK"}


class CSVReport:

    def __init__(self, report):
        self._report = report

    @property
    def report(self):
        return self._report

    def generate_report(self, pathname):
        with open(pathname, "w") as file:
            file_writer = csv.writer(file, lineterminator='\n')
            self._generate_column_headers(file_writer)
            for category in self.report.category_list:
                excel_category_report = CSVCategoryReport(category)
                excel_category_report.generate_report_for_category(file_writer)

    def _generate_column_headers(self, file_writer):
        file_writer.writerow(COLUMN_NAMES)


class CSVCategoryReport:

    def __init__(self, category):
        self._category = category

    @property
    def category(self):
        return self._category

    def generate_report_for_category(self, file_writer):
        self._generate_category_row(file_writer)
        for item in self._category.checklist:
            self._generate_item_row(file_writer, item)

    def _generate_category_row(self, file_writer):
        file_writer.writerow([self.category.name,
                              STATUS[self.category.result],
                              "",
                              ""
                              ])

    def _generate_item_row(self, file_writer, item):
        file_writer.writerow([item.name,
                              STATUS[item.result],
                              item.description,
                              item.recommendation
                              ])

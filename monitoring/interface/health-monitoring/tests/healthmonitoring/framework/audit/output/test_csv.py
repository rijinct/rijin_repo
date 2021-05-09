'''
Created on 11-Mar-2020

@author: poojabhi
'''

import unittest
import csv
import os

from healthmonitoring.framework.audit.output.csv import CSVCategoryReport, \
    CSVReport
from healthmonitoring.framework.audit.report import Report
from healthmonitoring.framework.util.file_util import get_resource_path
from ..test_report import create_category_with_failure_result
from ..test_report import create_category_with_ok_result
from healthmonitoring.framework.util import file_util


class TestCSVCategoryReport(unittest.TestCase):
    category_name = "category1"

    def setUp(self):
        file_util.under_test = True
        category = create_category_with_failure_result()
        self._category_report = CSVCategoryReport(category)

    def test_category(self):
        return self._category_report

    def test_generate_report_for_category(self):
        actual_file_name = "category.csv"
        expected_file_path = "expected_category.csv"
        expected_abs_file_path = get_resource_path(expected_file_path)
        expected_report = list(csv.reader(open(expected_abs_file_path, "r")))
        with open(actual_file_name, "w") as file:
            file_writer = csv.writer(file, lineterminator='\n')
            self._category_report.generate_report_for_category(file_writer)
        actual_report = list(csv.reader(open(actual_file_name, "r")))
        self.assertEqual(actual_report, expected_report)
        os.remove(actual_file_name)


class TestCSVReport(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        report = Report()
        report.add_category(create_category_with_failure_result())
        report.add_category(create_category_with_ok_result())
        self._excel_report = CSVReport(report)

    def test_generate_report(self):
        actual_file_name = "report.csv"
        expected_file_path = "expected_report.csv"
        expected_abs_file_path = get_resource_path(expected_file_path)
        expected_report = list(csv.reader(open(expected_abs_file_path, "r")))
        self._excel_report.generate_report(actual_file_name)
        actual_report = list(csv.reader(open(actual_file_name, "r")))
        self.assertEqual(actual_report, expected_report)
        os.remove(actual_file_name)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

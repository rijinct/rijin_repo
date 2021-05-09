'''
Created on 16-Mar-2020

@author: poojabhi
'''
import unittest

from healthmonitoring.framework.audit.input.excel import ExcelReader
from healthmonitoring.framework.audit.report import Report
from healthmonitoring.framework.util import file_util
from healthmonitoring.framework.util.file_util import get_resource_path

from ..test_report import create_category_with_failure_result
from ..test_report import create_category_with_ok_result


class TestExcelReader(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        actual_file_path = "CEM_Audit_Checklist.xls"
        filepath = get_resource_path(actual_file_path)
        self._excel_reader = ExcelReader(filepath)

    def _generate_expected_report(self):
        failure_category = create_category_with_failure_result()
        ok_category = create_category_with_ok_result()
        report = Report()
        report.add_category(failure_category)
        report.add_category(ok_category)
        return report

    def test_generate_report(self):
        expected_category_list = self._generate_expected_report().category_list
        self._excel_reader.generate_report()
        actual_category_list = self._excel_reader.report.category_list
        for actual_category, expected_category in zip(actual_category_list,
                                                      expected_category_list):
            self.assertEqual(actual_category.name, expected_category.name)
            self.assertEqual(actual_category.checklist,
                             expected_category.checklist)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

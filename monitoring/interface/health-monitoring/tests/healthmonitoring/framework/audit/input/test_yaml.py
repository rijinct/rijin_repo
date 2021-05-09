'''
Created on 22-Apr-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework.util.file_util import get_resource_path
from healthmonitoring.framework.audit.input.yaml import YamlReader
from healthmonitoring.framework.audit.report import CheckItem
from healthmonitoring.framework.util import file_util


class TestYamlReader(unittest.TestCase):

    def setUp(self):
        file_util.under_test = True
        pathname = "audit/standard.yaml"
        pathname = get_resource_path(pathname)
        self.reader = YamlReader(pathname)

    def test_get_report(self):
        report = self.reader.report
        self.assertIsNotNone(report)

    def test_get_categories(self):
        report = self.reader.report
        self.assertListEqual(
            report.get_category_names(),
            [
                "Hardware, Network, OS",
                "Hadoop"
            ])

    def test_get_checklist_items(self):
        report = self.reader.report
        self.assertListEqual(
            report.category_list[0].get_checklist_item_names(),
            [
                'Check if all Data nodes are UP',
                'Check CPU utilization'
            ])

    def test_get_checklist_item_details(self):
        report = self.reader.report
        self.assertEqual(
            report.category_list[0].checklist[0],
            CheckItem(
                name="Check if all Data nodes are UP",
                description="One or more nodes are down",
                recommendation="Recommendation1",
                condition="node_down_trigger"
                )
            )


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

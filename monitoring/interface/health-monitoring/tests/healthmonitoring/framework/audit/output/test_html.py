'''
Created on 11-Mar-2020

@author: poojabhi
'''
import unittest

from healthmonitoring.framework.audit.output.html import CategoryTable, \
    SummaryTable, HtmlReport
from healthmonitoring.framework.audit.report import Report, CategoryReport, \
    CheckItem

from ..test_report import create_category_with_failure_result
from ..test_report import create_category_with_ok_result


def format_content(content):
    return content.strip().replace('\n', '').replace('\t', '').replace(' ', '')


class TestCategoryTable(unittest.TestCase):

    def setUp(self):
        category = create_category_with_failure_result()
        self.category_table = CategoryTable(category)

    def _create_category_table(self):
        return """
            <tr style="font-weight: bold; background-color: #6685ff">
                <td>failure_category</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>failure_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td>Services are not up</td>
                <td>Restart the services</td>
            </tr>
            <tr>
                <td >ok_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>failure_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td>Services are not up</td>
                <td>Restart the services</td>
            </tr>
            """  # noqa: 501

    def test_generate_html(self):
        expected_table_content = format_content(self._create_category_table())
        table_content = format_content(self.category_table.generate_html())
        self.assertEqual(table_content, expected_table_content)

    def test_generate_html_with_show_columns(self):
        category = CategoryReport("failure_category")
        check_item = CheckItem("name", "description", "recommendation",
                               "condition", "col3, col1")
        check_item.result = False
        check_item.failures = [
            {'col1': '(1,1)', 'col2': '(1,2)', 'col3': '(1,3)'},
            {'col1': '(2,1)', 'col2': '(2,2)', 'col3': '(2,3)'},
            {'col1': '(3,1)', 'col2': '(3,2)', 'col3': '(3,3)'}
            ]
        category.add_checklist_item(check_item)
        category.recompute()
        category_table = CategoryTable(category)

        actual = (category_table.generate_html())

        expected = """
        <tr style="font-weight: bold; background-color: #6685ff">
            <td>failure_category</td>
            <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
            <td></td>
            <td></td>
        </tr><tr>
            <td>name
                <table border="1" style="margin-left:40px;">
                    <tr><td>(1,3)</td><td>(1,1)</td></tr>
                    <tr><td>(2,3)</td><td>(2,1)</td></tr>
                    <tr><td>(3,3)</td><td>(3,1)</td></tr>
                </table>
            </td>
            <td style="border-bottom: 1px solid gray; background-color: #ff8566;" >NOT OK</td>
            <td>description</td>
            <td>recommendation</td>
        </tr>"""  # noqa: E501

        self.assertEqual(format_content(actual), format_content(expected))


class TestSummaryTable(unittest.TestCase):

    def setUp(self):
        self._summary_table = SummaryTable(Report())

    def _create_summary_table(self):
        return """<table style="border: 1px solid black; width: 900px;">
           <tbody>
                <tr>
                    <td>failure_category</td>
                    <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                </tr>
                <tr>
                    <td>ok_category</td>
                    <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                </tr>
           </tbody>
        </table>"""  # noqa: 501

    def _create_category_list(self):
        failure_category = create_category_with_failure_result()
        ok_category = create_category_with_ok_result()
        self._summary_table.report.add_category(failure_category)
        self._summary_table.report.add_category(ok_category)

    def test_generate_html(self):
        expected_table_content = format_content(self._create_summary_table())
        self._create_category_list()
        table_content = format_content(self._summary_table.generate_html())
        self.assertEqual(table_content, expected_table_content)


class TestHtmlReport(unittest.TestCase):

    def setUp(self):
        self._html_report = HtmlReport(Report())
        self.maxDiff = None

    def _create_html_content(self):
        return """<table style="border: 1px solid black; width: 900px;">
           <tbody>
                <tr>
                    <td>failure_category</td>
                    <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                </tr>
                <tr>
                    <td>ok_category</td>
                    <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                </tr>
           </tbody>
        </table>
        <table style="border: 1px solid black; width: 900px;">
        <thead>
            <tr style="border-bottom: 1px solid black; background-color: silver;">
                <th>Item</th>
                <th>Status</th>
                <th>Description</th>
                <th>Recommendation</th>
            </tr>
        </thead>
        <tbody>
            <tr style="font-weight: bold; background-color: #6685ff">
                <td>failure_category</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>failure_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td>Services are not up</td>
                <td>Restart the services</td>
            </tr>
            <tr >
                <td >ok_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>failure_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #ff8566;">NOT OK</td>
                <td>Services are not up</td>
                <td>Restart the services</td>
            </tr>
            <tr style="font-weight: bold; background-color: #6685ff">
                <td>ok_category</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>ok_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>ok_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td>ok_item</td>
                <td style="border-bottom: 1px solid gray; background-color: #85e085;">OK</td>
                <td></td>
                <td></td>
            </tr>
        </tbody>
        </table>"""  # noqa: 501

    def test_generate_html(self):
        expected_html_content = format_content(self._create_html_content())
        failure_category = create_category_with_failure_result()
        ok_category = create_category_with_ok_result()
        self._html_report.report.add_category(failure_category)
        self._html_report.report.add_category(ok_category)
        html_content = format_content(self._html_report.generate_html())
        self.assertEqual(html_content, expected_html_content)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

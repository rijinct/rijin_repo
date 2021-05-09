'''
Created on 11-Mar-2020

@author: poojabhi
'''
from enum import Enum

STATUS = {True: "OK", False: "NOT OK"}


class HtmlReport:

    def __init__(self, report):
        self._report = report

    @property
    def report(self):
        return self._report

    def generate_html(self):
        return self._generate_html_for_summary_table() + \
            self._generate_html_for_category_table()

    def _generate_html_for_summary_table(self):
        return SummaryTable(self.report).generate_html()

    def _generate_html_for_category_table(self):
        result_table_style = Style.RESULT_TABLE.value
        template = """<table style="{result_table_style}">
        <thead>
            <tr style="border-bottom: 1px solid black; background-color: silver;">
                <th>Item</th>
                <th>Status</th>
                <th>Description</th>
                <th>Recommendation</th>
            </tr>
        </thead>
        <tbody>{body}</tbody>
        </table>"""  # noqa: 501
        return template.format(
            result_table_style=result_table_style,
            body=self._generate_html_for_category_table_rows())

    def _generate_html_for_category_table_rows(self):
        category_html_content = ""
        for category in self.report.category_list:
            category_html_content += CategoryTable(category).generate_html()
        return category_html_content


class SummaryTable:

    def __init__(self, report):
        self._report = report

    @property
    def report(self):
        return self._report

    def _generate_row(self, category):
        style = Style.OK_CELL.value if category.result \
            else Style.FAILURE_CELL.value

        return """<tr>
            <td>{category_name}</td>
            <td style="{style}">{status}</td>
        </tr>""".format(category_name=category.name,
                        style=style,
                        status=STATUS[category.result])

    def _generate_rows(self):
        return "\n".join(map(self._generate_row, self.report.category_list))

    def generate_html(self):
        return """<table style="{result_table_style}"><tbody>{body}</tbody></table>""".format(# noqa:501
            result_table_style=Style.RESULT_TABLE.value,
            body=self._generate_rows())


class CategoryTable:

    def __init__(self, category):
        self._category = category

    @property
    def category(self):
        return self._category

    def generate_html(self):
        return self._generate_category_row() + self._generate_rows()

    def _generate_category_row(self):
        style = Style.OK_CELL.value if self.category.result \
            else Style.FAILURE_CELL.value

        return """<tr style="{category_row_style}">
            <td>{category_name}</td>
            <td style="{style}">{status}</td>
            <td></td>
            <td></td>
        </tr>""".format(
            category_row_style=Style.CATEGORY_ROW.value,
            category_name=self.category.name,
            style=style,
            status=STATUS[self.category.result])

    def _generate_rows(self):
        return "\n".join(map(self._generate_row, self.category.checklist))

    def _generate_row(self, item):
        style = Style.OK_CELL.value if item.result \
            else Style.FAILURE_CELL.value
        if item.result:
            description, recommendation = "", ""
        else:
            description, recommendation = item.description, item.recommendation

        return """<tr>
            <td>{name}{failures}</td>
            <td style="{style}" >{status}</td>
            <td>{description}</td>
            <td>{recommendation}</td>
        </tr>""".format(
            name=item.name,
            failures=self._get_failures(item),
            style=style,
            status=STATUS[item.result],
            description=description,
            recommendation=recommendation)

    def _get_failures(self, item):
        failures = ""

        if item.show_columns and item.failures:
            failures = """
            <table border="1" style="margin-left:40px;">
            {}
            </table>
            """.format(self._get_failures_rows(item))

        return failures

    def _get_failures_rows(self, item):
        row_strings = []
        for row in item.failures:
            row_strings.append("<tr>{}</tr>".format(
                self._get_failure_row(item, row)))
        return "\n".join(row_strings)

    def _get_failure_row(self, item, row):
        column_strings = []
        for column in item.show_columns.split(','):
            column_strings.append("<td>{}</td>".format(row[column.strip()]))
        return "\n".join(column_strings)


class Style(Enum):
    TABLE_TR_TD = "border-right: 1px solid; color: black"
    TABLE_TR_TH = "border-right: 1px solid; color: black"
    TR_NTH_CHILD_EVEN = "background-color: #dddddd;"
    RESULT_TABLE = "border: 1px solid black; width: 900px;"
    TABLE_HEADER = "border-bottom: 1px solid black; background-color: silver;"
    CATEGORY_ROW = "font-weight: bold; background-color: #6685ff"
    OK_CELL = "border-bottom: 1px solid gray; background-color: #85e085;"
    FAILURE_CELL = "border-bottom: 1px solid gray; background-color: #ff8566;"

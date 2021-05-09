'''
Created on 16-Mar-2020

@author: poojabhi
'''
import xlrd

from healthmonitoring.framework.audit.report import CategoryReport, \
    CheckItem, Report


class ExcelReader:

    def __init__(self, filepath):
        workbook = xlrd.open_workbook(filepath)
        self._worksheet = workbook.sheet_by_index(0)
        self._report = Report()
        self._categories = {}

    @property
    def report(self):
        return self._report

    def generate_report(self):
        self._generate_categories_with_item_list()
        for category_name, item_list in self._categories.items():
            category = CategoryReport(category_name)
            category.checklist = item_list
            category.recompute()
            self._report.add_category(category)

    def _generate_categories_with_item_list(self):
        header_row = self._worksheet.row_values(0)
        column_headers = self._get_column_header_index(header_row)
        for i in range(1, self._worksheet.nrows):
            self._add_item_in_list(self._worksheet.row_values(i),
                                   column_headers)

    def _get_column_header_index(self, column_header_row):
        column_headers = {}
        for index, header in enumerate(column_header_row):
            column_headers[header] = index
        return column_headers

    def _add_item_in_list(self, row, column_headers):
        if row[column_headers["Category"]] in self._categories:
            item = self._create_item(row, column_headers)
            item_list = self._categories[row[column_headers["Category"]]]
            item_list.append(item)
        else:
            item_list = []
            item = self._create_item(row, column_headers)
            item_list.append(item)
            self._categories[row[column_headers["Category"]]] = item_list

    def _create_item(self, row, column_headers):
        return CheckItem(row[column_headers["Item"]],
                         row[column_headers["Description"]],
                         row[column_headers["Recommendation"]],
                         row[column_headers["Condition"]])

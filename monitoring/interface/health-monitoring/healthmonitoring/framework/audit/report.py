'''
Created on 11-Mar-2020

@author: poojabhi
'''


class ReportStore:

    def __init__(self):
        self._reports = {}

    def add(self, report):
        self._reports[report.name] = report

    def get(self, name):
        return self._reports[name]

    def get_names(self):
        return self._reports.keys()


class Report:

    def __init__(self, name=""):
        self._name = name
        self._category_list = []

    def __eq__(self, report):
        return isinstance(report, Report) \
            and self.category_list == report.category_list

    @property
    def name(self):
        return self._name

    @property
    def category_list(self):
        return self._category_list

    def add_category(self, category):
        self._category_list.append(category)

    def get_category_names(self):
        return [category.name for category in self.category_list]

    def get_category(self, name):
        index = self.get_category_names().index(name)
        return self._category_list[index]


class CategoryReport:

    def __init__(self, name):
        self._name = name
        self._result = True
        self._checklist = []
        self._event = None

    def __eq__(self, category):
        return isinstance(category, CategoryReport) \
            and self.name == category.name \
            and self.checklist == category.checklist

    @property
    def name(self):
        return self._name

    @property
    def result(self):
        return self._result

    @property
    def checklist(self):
        return self._checklist

    @checklist.setter
    def checklist(self, checklist):
        self._checklist = checklist

    def get_checklist_item_names(self):
        return [item.name for item in self.checklist]

    def recompute(self):
        self._result = True
        for check in self.checklist:
            self._result = self._result and check.result

    def add_checklist_item(self, item):
        self._checklist.append(item)

    def get_checklist_item(self, name):
        index = self.get_checklist_item_names().index(name)
        return self._checklist[index]


class CheckItem:

    def __init__(self, name, description, recommendation, condition="",
                 show_columns=""):
        self._name = name
        self._result = True
        self._description = description
        self._recommendation = recommendation
        self._condition = condition
        self._show_columns = show_columns
        self._failures = []

    def __eq__(self, item):
        return isinstance(item, CheckItem) \
            and self.name == item.name \
            and self.description == item.description \
            and self.recommendation == item.recommendation \
            and self.condition == item.condition

    @property
    def name(self):
        return self._name

    @property
    def result(self):
        return self._result

    @property
    def description(self):
        return self._description

    @property
    def recommendation(self):
        return self._recommendation

    @property
    def condition(self):
        return self._condition

    @result.setter
    def result(self, result):
        self._result = result

    @property
    def show_columns(self):
        return self._show_columns

    @show_columns.setter
    def show_columns(self, show):
        self._show_columns = show

    @property
    def failures(self):
        return self._failures

    @failures.setter
    def failures(self, failures_list):
        self._failures = failures_list

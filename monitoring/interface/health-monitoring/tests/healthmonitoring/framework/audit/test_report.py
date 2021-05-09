'''
Created on 11-Mar-2020

@author: poojabhi
'''
import unittest

from healthmonitoring.framework.audit.report import CheckItem, \
    CategoryReport, Report, ReportStore


def create_item_with_failure_result():
    item = CheckItem("failure_item",
                     "Services are not up",
                     "Restart the services",
                     "trigger1")
    item.result = False
    return item


def create_item_with_ok_result():
    item = CheckItem("ok_item", "N/A", "N/A")
    item.result = True
    return item


def create_category_with_failure_result():
    category = CategoryReport("failure_category")
    category.add_checklist_item(create_item_with_failure_result())
    category.add_checklist_item(create_item_with_ok_result())
    category.add_checklist_item(create_item_with_failure_result())
    category.recompute()
    return category


def create_category_with_ok_result():
    category = CategoryReport("ok_category")
    category.add_checklist_item(create_item_with_ok_result())
    category.add_checklist_item(create_item_with_ok_result())
    category.add_checklist_item(create_item_with_ok_result())
    category.recompute()
    return category


def create_report():
    report = Report()
    report.add_category(create_category_with_failure_result())
    report.add_category(create_category_with_ok_result())
    return report


class TestCheckItem(unittest.TestCase):
    item_name = "failure_item"
    item_description = "Services are not up"
    item_recommendation = "Restart the services"
    item_condition = "trigger1"

    def setUp(self):
        self.check_item = CheckItem(TestCheckItem.item_name,
                                    TestCheckItem.item_description,
                                    TestCheckItem.item_recommendation,
                                    TestCheckItem.item_condition)

    def test_name(self):
        self.assertEqual(self.check_item.name, TestCheckItem.item_name)

    def test_description(self):
        self.assertEqual(self.check_item.description,
                         TestCheckItem.item_description)

    def test_recommendation(self):
        self.assertEqual(self.check_item.recommendation,
                         TestCheckItem.item_recommendation)

    def test_condition(self):
        self.assertEqual(self.check_item.condition,
                         TestCheckItem.item_condition)

    def test_result(self):
        self.check_item.result = True
        self.assertTrue(self.check_item.result)

    def test_show_columns(self):
        SHOW_COLUMNS = "col1, col2, col3"
        check_item = CheckItem("name", "description", "recommendation",
                               "condition", SHOW_COLUMNS)
        self.assertEqual(check_item.show_columns, SHOW_COLUMNS)

    def test_item(self):
        self.assertEqual(self.check_item, create_item_with_failure_result())


class TestCategoryReport(unittest.TestCase):
    category_name = "failure_category"
    item_name = "failure_item"
    item_description = "Services are not up"
    item_recommendation = "Restart the services"
    item_condition = "trigger1"

    def setUp(self):
        self.category_report = CategoryReport(TestCategoryReport.category_name)

    def test_name(self):
        self.assertEqual(self.category_report.name,
                         TestCategoryReport.category_name)

    def test_add_checklist(self):
        item = CheckItem(TestCategoryReport.item_name,
                         TestCategoryReport.item_description,
                         TestCategoryReport.item_recommendation,
                         TestCategoryReport.item_condition)
        self.category_report.add_checklist_item(item)
        self.assertEqual(self.category_report.checklist[0],
                         create_item_with_failure_result())

    def _create_category_report(self, k):
        self.category_report = CategoryReport(TestCategoryReport.category_name)
        for result in k:
            item = CheckItem(TestCategoryReport.item_name,
                             TestCategoryReport.item_description,
                             TestCategoryReport.item_recommendation)
            item.result = result
            self.category_report.add_checklist_item(item)

    def test_recompute(self):
        d = {(False,): False,
             (True,): True,
             (False, False, False): False,
             (True, True, False): False,
             (True, True, False, False, False): False
             }
        for k, v in d.items():
            self._create_category_report(k)
            self.category_report.recompute()
            self.assertEqual(self.category_report.result, v)

    def test_category(self):
        failure_category = create_item_with_failure_result()
        ok_category = create_item_with_ok_result()
        self.category_report.add_checklist_item(failure_category)
        self.category_report.add_checklist_item(ok_category)
        self.category_report.add_checklist_item(failure_category)
        self.assertEqual(self.category_report,
                         create_category_with_failure_result())

    def test_get_checklist_item_names(self):
        failure_category = create_item_with_failure_result()
        ok_category = create_item_with_ok_result()
        self.category_report.add_checklist_item(failure_category)
        self.category_report.add_checklist_item(ok_category)
        self.category_report.add_checklist_item(failure_category)
        self.assertEqual(self.category_report.get_checklist_item_names(),
                         ['failure_item', 'ok_item', 'failure_item'])

    def test_get_checklist_item(self):
        failure_item = create_item_with_failure_result()
        ok_item = create_item_with_ok_result()
        self.category_report.add_checklist_item(failure_item)
        self.category_report.add_checklist_item(ok_item)
        self.assertEqual(
            self.category_report.get_checklist_item("failure_item"),
            failure_item)
        self.assertEqual(
            self.category_report.get_checklist_item("ok_item"), ok_item)

    def test_get_name_when_not_set(self):
        report = Report()
        self.assertEqual(report.name, "")

    def test_get_name_when_set(self):
        name = "Report1"
        report = Report(name)
        self.assertEqual(report.name, name)


class TestReport(unittest.TestCase):
    category1_name = "failure_category"
    category2_name = "ok_category"

    def setUp(self):
        self.report = Report()

    def _create_and_add_empty_categories(self):
        category1 = CategoryReport(TestReport.category1_name)
        category2 = CategoryReport(TestReport.category2_name)
        self.report.add_category(category1)
        self.report.add_category(category2)
        return category1, category2

    def _create_and_add_filled_categories(self):
        self.report.add_category(create_category_with_failure_result())
        self.report.add_category(create_category_with_ok_result())

    def test_add_category_list(self):
        category1, category2 = self._create_and_add_empty_categories()
        dummy_category_list = [
                               category1,
                               category2
                              ]
        self.assertListEqual(self.report.category_list, dummy_category_list)

    def test_report(self):
        self._create_and_add_filled_categories()
        self.assertEqual(self.report, create_report())

    def test_get_category_names(self):
        self._create_and_add_filled_categories()
        self.assertListEqual(self.report.get_category_names(),
                             ['failure_category', 'ok_category'])

    def test_get_category(self):
        category1, category2 = self._create_and_add_empty_categories()
        self.assertEqual(
            self.report.get_category(TestReport.category1_name), category1)
        self.assertEqual(
            self.report.get_category(TestReport.category2_name), category2)


class TestReportStore(unittest.TestCase):

    def setUp(self):
        self.store = ReportStore()

    def test_add_and_get(self):
        name = "ReportA"
        report1 = Report(name)
        self.store.add(report1)
        report2 = self.store.get(name)
        self.assertEqual(report2, report1)

    def test_get_names(self):
        names = "Report1", "Report2"
        for name in names:
            self.store.add(Report(name))
        self.assertListEqual(list(self.store.get_names()), list(names))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

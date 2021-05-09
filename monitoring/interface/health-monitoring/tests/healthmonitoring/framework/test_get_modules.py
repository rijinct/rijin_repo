import unittest

import tools.packager.get_modules as get_modules
from healthmonitoring.framework.util.file_util import get_resource_path
from healthmonitoring.framework.util import file_util


class TestGetModules(unittest.TestCase):
    DIR_PATH = r"../healthmonitoring/framework/actors"

    def setUp(self):
        file_util.under_test = False

    def test_get_list_of_modules(self):
        expected = set([
            "pkg_resources.py2_warn",
            "healthmonitoring.framework.actors.aggregation",
            "healthmonitoring.framework.actors.traps.scheduler_jobs_failure"
            ])
        actual = set(get_modules.get_list_of_modules(
            get_resource_path(TestGetModules.DIR_PATH)))
        self.assertTrue(expected.issubset(actual))

    def test_get_module_name(self):
        paths = [r"..\healthmonitoring\actors\restart.py",
                 r"../healthmonitoring/actors/restart.py"]
        expected = "healthmonitoring.actors.restart"
        for path in paths:
            actual = get_modules._get_module_name(path)
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

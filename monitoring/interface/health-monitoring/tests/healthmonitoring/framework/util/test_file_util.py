'''
Created on 09-Apr-2020

@author: nageb
'''
from pathlib import Path
import unittest

from healthmonitoring.framework.util import file_util


class TestFileUtil(unittest.TestCase):

    def test_get_resource_path_given_relative_path_in_test_mode(self):
        file_util.under_test = True
        actual = str(file_util.get_resource_path("d1/d2/f"))
        expected = str(
            (Path(__file__).parent / "../../../resources/d1/d2/f").resolve())
        self.assertEqual(actual, expected)

    def test_get_resource_path_given_relative_path_in_non_test_mode(self):
        file_util.under_test = False
        actual = str(file_util.get_resource_path("d1/d2/f"))
        expected = str(
            (Path(__file__).parent / "../../../../resources/d1/d2/f").
            resolve())
        self.assertEqual(actual, expected)

    def test_get_resource_path_given_absolute_path(self):
        actual = str(file_util.get_resource_path("/d1/d2/f"))
        expected = str((Path("/d1/d2/f").resolve()))
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

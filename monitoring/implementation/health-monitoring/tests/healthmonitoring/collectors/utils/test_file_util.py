'''
Created on 28-May-2020

@author: deerakum
'''
import unittest
from time import sleep
from pathlib import Path

from healthmonitoring.collectors.utils.file_util import FileUtils


class TestFileUtil(unittest.TestCase):

    FILENAME1 = Path("test1.txt")
    FILENAME2 = Path("test2.txt")
    DIRECTORY = Path("hm_test")

    @classmethod
    def setUpClass(cls):
        if not Path.exists(TestFileUtil.DIRECTORY):
            Path.mkdir(TestFileUtil.DIRECTORY)
        pathname1 = TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1
        pathname2 = TestFileUtil.DIRECTORY / TestFileUtil.FILENAME2
        content = '''This is a test file
All tests will be validated on this'''
        pathname1.write_text(content)
        sleep(0.05)
        pathname2.write_text(content)

    def test_get_latest_file(self):
        expected = TestFileUtil.DIRECTORY / TestFileUtil.FILENAME2
        file_path = TestFileUtil.DIRECTORY
        self.assertEqual(FileUtils.get_latest_file(file_path), expected)

    def test_get_files_count_in_dir(self):
        expected = 2
        files_path = TestFileUtil.DIRECTORY
        self.assertEqual(FileUtils.get_files_count_in_dir(files_path),
                         expected)

    def test_get_files_in_dir(self):
        expected = [
            TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1,
            TestFileUtil.DIRECTORY / TestFileUtil.FILENAME2
        ]
        self.assertSetEqual(
            set(FileUtils.get_files_in_dir(TestFileUtil.DIRECTORY)),
            set(expected))

    def test_get_file_size_in_mb(self):
        file_name = TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1
        self.assertEqual(FileUtils.get_file_size_in_mb(file_name), 0.0001)

    def test_get_dir_name(self):
        self.assertEqual(
            FileUtils.get_dir_name(TestFileUtil.DIRECTORY /
                                   TestFileUtil.FILENAME1),
            TestFileUtil.DIRECTORY)

    def test_get_files_by_modtime(self):
        expected = [
            TestFileUtil.DIRECTORY / TestFileUtil.FILENAME2,
            TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1
        ]
        self.assertListEqual(
            FileUtils.get_files_by_modtime(TestFileUtil.DIRECTORY),
            expected)

    def test_exists(self):
        self.assertTrue(
            FileUtils.exists(TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1))
        self.assertFalse(FileUtils.exists('hm_test/test3.txt'))

    def test_read_file(self):
        expected = 'This is a test file\nAll tests will be validated on this'
        self.assertEqual(
            FileUtils.read_file(TestFileUtil.DIRECTORY /
                                TestFileUtil.FILENAME1), expected)

    def test_read_lines(self):
        expected = [
            'This is a test file', 'All tests will be validated on this'
        ]
        self.assertListEqual(
            FileUtils.readlines(TestFileUtil.DIRECTORY /
                                TestFileUtil.FILENAME1), expected)

    def test_get_line_count(self):
        self.assertEqual(
            FileUtils.get_line_count(TestFileUtil.DIRECTORY /
                                     TestFileUtil.FILENAME1), 2)

    @classmethod
    def tearDownClass(cls):
        Path.unlink(TestFileUtil.DIRECTORY / TestFileUtil.FILENAME1)
        Path.unlink(TestFileUtil.DIRECTORY / TestFileUtil.FILENAME2)
        Path.rmdir(TestFileUtil.DIRECTORY)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

'''
Created on 09-Jun-2020

@author: praveenD
'''
import unittest
from pathlib import Path

from healthmonitoring.collectors.utils.file_util import CsvUtils


class TestCsvUtil(unittest.TestCase):

    FILENAME = Path("test.csv")
    DIRECTORY = Path("hm_test")

    def setUp(self):
        if not Path.exists(TestCsvUtil.DIRECTORY):
            Path.mkdir(TestCsvUtil.DIRECTORY)
        path_name = TestCsvUtil.DIRECTORY / TestCsvUtil.FILENAME
        content = '''Book,Author,Released
Harry Potter,J.K.Rowling,1997
Twilight,Stephenie Meyer,2005'''
        path_name.write_text(content)

    def test_read_csv(self):
        expected = [['Book', 'Author', 'Released'],
                    ['Harry Potter', 'J.K.Rowling', '1997'],
                    ['Twilight', 'Stephenie Meyer', '2005']]
        self.assertListEqual(
            CsvUtils.read_csv(TestCsvUtil.DIRECTORY / TestCsvUtil.FILENAME),
            expected)

    def test_read_csv_as_dict_output(self):
        expected = (['Book', 'Author', 'Released'], [{
            'Book': 'Harry Potter',
            'Author': 'J.K.Rowling',
            'Released': '1997'
        }, {
            'Book': 'Twilight',
            'Author': 'Stephenie Meyer',
            'Released': '2005'
        }])
        self.assertEqual(
            CsvUtils.read_csv_as_dict_output(TestCsvUtil.DIRECTORY /
                                             TestCsvUtil.FILENAME), expected)

    def tearDown(self):
        Path.unlink(TestCsvUtil.DIRECTORY / TestCsvUtil.FILENAME)
        Path.rmdir(TestCsvUtil.DIRECTORY)


if __name__ == "__main__":
    unittest.main()

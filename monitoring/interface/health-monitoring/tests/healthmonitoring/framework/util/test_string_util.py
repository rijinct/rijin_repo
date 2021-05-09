'''
Created on 09-Apr-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework.util import string_util


class TestStringUtil(unittest.TestCase):

    def test_convert_str_to_data_structure_with_pipe_delimiter(self):
        input_string, expected = self._get_input_and_expected()
        actual = string_util.convert_str_to_data_structure(
            input_string, delimiter="|")
        self.assertEqual(actual, expected)

    def test_convert_str_to_data_structure_with_delimiter_detection(self):
        input_string, expected = self._get_input_and_expected()
        actual = string_util.convert_str_to_data_structure(input_string)
        self.assertEqual(actual, expected)

    def test_substring_after_given_single_occurrence(self):
        self.assertEqual(string_util.substring_after("/", "abc/xyz"), "xyz")

    def test_substring_after_given_multiple_occurrence(self):
        self.assertEqual(
            string_util.substring_after("/", "abc/xyz/123"), "xyz/123")

    def test_substring_after_a_number_of_occurence(self):
        self.assertEqual(
            string_util.substring_after("/", "abc/xyz/123", 2), "123")

    def test_substring_after_given_no_occurrence(self):
        self.assertEqual(string_util.substring_before("/", "abc123"), "abc123")

    def test_substring_before_given_single_occurrence(self):
        self.assertEqual(string_util.substring_before("/", "abc/xyz"), "abc")

    def test_substring_before_given_multiple_occurrence(self):
        self.assertEqual(
            string_util.substring_before("/", "abc/xyz/123"), "abc")

    def test_substring_before_a_number_of_occurence(self):
        self.assertEqual(
            string_util.substring_before("/", "abc/xyz/123", 2), "abc/xyz")

    def test_substring_before_given_no_occurrence(self):
        self.assertEqual(string_util.substring_before("/", "abc123"), "abc123")

    def test_get_index_for_first_occurence(self):
        self.assertEqual(string_util.get_index("/", "abc/xyz/123"), 3)

    def test_get_index_for_nth_occurence(self):
        self.assertEqual(string_util.get_index("/", "abc/xyz/123", 2), 7)

    def test_get_index_for_invalid_occurence(self):
        self.assertRaises(ValueError, string_util.get_index,
                          "/", "abc/xyz/123", 3)

    def test_is_quoted(self):
        self.assertTrue(string_util.is_quoted("'var1'"))
        self.assertTrue(string_util.is_quoted('"var1"'))
        self.assertFalse(string_util.is_quoted("var1'"))
        self.assertFalse(string_util.is_quoted('"var1'))

    def _get_input_and_expected(self):
        input_string = """
      time_frame    |                    job_name                    |     start_time      |      end_time       | executionduration | status
    13Hour===14Hour | Perf_VOICE_DP_CELL_1_HOUR_AggregateJob         | 2020-02-26 14:20:13 |                     |                   | R
    13Hour===14Hour | Perf_SMS_DP_CELL_1_HOUR_AggregateJob           | 2020-02-26 14:20:13 |                     |                   | R
    13Hour===14Hour | Perf_BB_OTT_FAIL_SEGG_1_HOUR_AggregateJob      | 2020-02-26 14:10:36 |                     |        60         | R
    """  # noqa:E501
        expected = [{
            "time_frame": "13Hour===14Hour",
            "job_name": "Perf_VOICE_DP_CELL_1_HOUR_AggregateJob",
            "start_time": "2020-02-26 14:20:13",
            "end_time": "",
            "executionduration": "",
            "status": "R"
        }, {
            "time_frame": "13Hour===14Hour",
            "job_name": "Perf_SMS_DP_CELL_1_HOUR_AggregateJob",
            "start_time": "2020-02-26 14:20:13",
            "end_time": "",
            "executionduration": "",
            "status": "R"
        }, {
            "time_frame": "13Hour===14Hour",
            "job_name": "Perf_BB_OTT_FAIL_SEGG_1_HOUR_AggregateJob",
            "start_time": "2020-02-26 14:10:36",
            "end_time": "",
            "executionduration": '60',
            "status": "R"
        }]
        return input_string, expected


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

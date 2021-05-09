import unittest
from healthmonitoring.collectors.utils.string import StringUtils


class TestStringUtils(unittest.TestCase):

    def test_get_first_line(self):
        self.assertEqual(
            StringUtils.get_first_line('Line1\nLine2'),
            'Line1')

    def test_get_last_line(self):
        self.assertEqual(
            StringUtils.get_last_line('Line1\nLine2\nLine3'),
            'Line3')

    def test_get_word(self):
        self.assertEqual(
            StringUtils.get_word('word1 word2'),
            'word1')

    def test_get_last_word(self):
        self.assertEqual(
            StringUtils.get_last_word('word1 word2 word3'),
            'word3')


if __name__ == "__main__":
    unittest.main()

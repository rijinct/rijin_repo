import unittest.mock as mock
import unittest
import json
from unittest.mock import patch
import os_cmd
from os_cmd import my_function2
import builtins

class Test(unittest.TestCase):
    @patch('builtins.print')
    @patch('os_cmd.test',return_value="test")
    def test_my_function(self,first_mock,sec_mock):
        _dir = r'C:\Users\rithomas\Desktop\NokiaCemod'
        #print(my_function(_dir))
        my_function2(_dir)
        #os_system.assert_called()

if __name__ == "__main__":
    unittest.main()
'''
Created on 11-Jun-2020

@author: deerakum
'''
import unittest

from healthmonitoring.collectors.utils.html_util import HtmlUtil


class TestHtmlUtil(unittest.TestCase):

    def test_generate_html_from_list(self):
        expected_html = '''<p style="font-size: large; font-style: bold">Below are the Delayed Usage Jobs</p>
<table cellpadding="10" style="font-family: arial, sans-serif; border-collapse: collapse; width: 100%;">
<tr><th style="border: 1px solid #dddddd; color: white; padding: 8px;" bgcolor="#2A1111">Delayed Usage Jobs</th></tr>
<tr><td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;" bgcolor="#FF0000"">Usage_SMS_1_LoadJob</td>
<tr><td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;" bgcolor="#FF8000">Usage_VOICE_2_LoadJob</td>
<tr><td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;">Usage_SGSN_3_LoadJob</td>
</tr>
</table>'''
        self.assertEqual(HtmlUtil().generateHtmlFromList("Below are the Delayed Usage Jobs", ['Delayed Usage Jobs', 'Usage_SMS_1_LoadJobRed', 'Usage_VOICE_2_LoadJobOrange','Usage_SGSN_3_LoadJob']), expected_html)
    
    def test_generate_html_from_dict_list(self):
        expected_html = '''<p style="font-size: large; font-style: bold">Breached Lag Threshold Connectors</p>
<table cellpadding="10" style="font-family: arial, sans-serif; border-collapse: collapse; width: 100%;">
<tr><th style="border: 1px solid #dddddd; color: white; padding: 8px;" bgcolor="#2A1111">Connector</th><th style="border: 1px solid #dddddd; color: white; padding: 8px;" bgcolor="#2A1111">lag</th></tr>
<tr><td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;">GN_1</td>
<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;">616292</td>
<tr><td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;">SMS_1</td>
<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;" bgcolor="#FF0000"">616292</td>
</tr>
</table>'''
        lag_list = [{'Connector': 'GN_1', 'lag': '616292'}, {'Connector': 'SMS_1', 'lag': '616292Red'}]
        label = "Breached Lag Threshold Connectors"
        self.assertEqual(HtmlUtil().generateHtmlFromDictList(label, lag_list), expected_html)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

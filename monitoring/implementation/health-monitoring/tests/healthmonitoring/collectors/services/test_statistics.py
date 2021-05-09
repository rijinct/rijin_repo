'''
Created on 17-Jun-2020

@author: praveenD
'''
import unittest

from mock import patch
from healthmonitoring.collectors.services.statistics import _execute
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil


class TestStatistics(unittest.TestCase):

    TODAY_DATE_TIME = '2020-06-13 16:48:00'

    @staticmethod
    def get_output(command, **args):
        output = {
            Command.JPS_CMD_FOR_PID: '986',
            Command.PS_EAF_CMD_FOR_PID: '7689',
            Command.SYSTEMCTL_CMD_FOR_PID: '6497',
            Command.CPU_IDLE_TIME:
            '%Cpu(s):  7.2 us,  3.9 sy,  0.0 ni, 88.2 id,  0.0 wa,  0.0 hi,  0.6 si,  0.0 st',  # noqa: 501
            Command.MEM_OF_JSTAT_SERVICE:
            ''' S0C    S1C    S0U    S1U      EC       EU        OC         OU       MC     MU    CCSC   CCSU   YGC     YGCT    FGC    FGCT     GCT   
50176.0 48128.0  99.560   3102.8 671232.0 215891.9 1930240.0  16694.2  111692.0 109935.4 12160.0 11751.4 333867 10615.414 2604  3311.273 13926.687''',  # noqa: 501
            Command.OPEN_FILES: '650'
        }
        return output[command]

    @patch.object(DateTimeUtil,
                  'get_current_hour_date',
                  return_value=TODAY_DATE_TIME)
    @patch.object(CommandExecutor, 'get_output', new=get_output)
    def test_get_service_stats(self, arg):
        output = _execute()[:1]
        print(output)
        self.assertListEqual(output, [{
            'Time': TestStatistics.TODAY_DATE_TIME,
            'Service': 'Scheduler',
            'Host': 'scheduler-fip.nokia.com',
            'Status': 'Running',
            'CPU_Use': 11.8,
            'Total_MEM': 235.79,
            'Open_Files': 650
        }])

    @staticmethod
    def get_command_output(command, **args):
        output = {
            Command.JPS_CMD_FOR_PID: '986',
            Command.PS_EAF_CMD_FOR_PID: '7689',
            Command.SYSTEMCTL_CMD_FOR_PID: '6497',
            Command.CPU_IDLE_TIME:
            '%Cpu(s):  0.0 us,  0.0 sy,  0.0 ni,80.08 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st',  # noqa: 501
            Command.MEM_OF_JSTAT_SERVICE: '986 not found',
            Command.MEM_OF_SERVICE:
            'KiB Mem : 29265729+total, 12682982+free, 12168128 used, 12485935+buff/cache',  # noqa: 501
            Command.OPEN_FILES: '650'
        }
        return output[command]

    @patch.object(DateTimeUtil,
                  'get_current_hour_date',
                  return_value=TODAY_DATE_TIME)
    @patch.object(CommandExecutor, 'get_output', new=get_command_output)
    def test_service_stats(self, arg):
        output = _execute()[:1]
        print(output)
        self.assertListEqual(output, [{
            'Time': TestStatistics.TODAY_DATE_TIME,
            'Service': 'Scheduler',
            'Host': 'scheduler-fip.nokia.com',
            'Status': 'Running',
            'CPU_Use': 19.92,
            'Total_MEM': 28579.81,
            'Open_Files': 650
        }])


if __name__ == '__main___':
    unittest.main()

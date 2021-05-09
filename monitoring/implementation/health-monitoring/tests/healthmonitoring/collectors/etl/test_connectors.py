import unittest
from mock import patch

from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.etl.connectors import _execute


class ProcessArg:

    def __init__(self, type):
        self._type = type

    @property
    def type(self):
        return self._type


class TestCheckConnectors(unittest.TestCase):

    @staticmethod
    def get_status_output(command, **args):
        return {
            Command.GET_CONNECTOR_STATUS: (
                0,
                '{"name":"TT_1_SOURCE_CONN","connector":{"state":"RUNNING","worker_id":"sslave1.nokia.com:8117"},"tasks":[{"state":"RUNNING","id":0,"worker_id":"sslave3.nokia.com:8117"},{"state":"RUNNING","id":1,"worker_id":"sslave2.nokia.com:8117"},{"state":"RUNNING","id":2,"worker_id":"sslave1.nokia.com:8117"}],"type":"source"}'  # noqa: 501
                )
            }[command]

    @staticmethod
    def get_output(command, **args):
        return {
            Command.GET_LAG_SUM: '''TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                      HOST            CLIENT-ID\nmgw_e.data      0          1159367         1159367         0               consumer-13-2563b554-d70a-4123-ae29-b7519877d50b /50.50.50.128   consumer-13\nmgw_e.data      1          1159371         1159371         0               consumer-13-c057b072-414d-495b-8d79-1c4104cd0189 /50.50.50.127   consumer-13\nmgw_e.data      2          1159368         1159368         0               consumer-22-498eb782-e2a0-4b32-bb48-184189fabf45 /50.50.50.172   consumer-22'''  # noqa: 501
            }[command]

    @patch.object(CommandExecutor, 'get_status_output', new=get_status_output)
    @patch('healthmonitoring.collectors.etl.connectors.ApplicationConfig')
    @patch('healthmonitoring.collectors.etl.connectors._process_args')
    @patch.object(DateTimeUtil, 'get_current_hour_date',
                  return_value='2020-05-07 19:15:32')
    def test_status(self, mock_get_current_hour_date,
                    _process_args, ApplicationConfig):
        expected = {
            "Time": "2020-05-07 19:15:32",
            "Connector_Type": "TT_1_SOURCE_CONN",
            "Connector_Status": "0",
            "Tasks_Status": "0",
            "Active_Tasks": "3",
            "Connector_Running_On_Node": "sslave1.nokia.com:8117",
            "Tasks_Running_on_Nodes": "sslave3.nokia.com:8117-1 sslave2.nokia.com:8117-1 sslave1.nokia.com:8117-1",  # noqa: 501
            "Total_Tasks": "3"
        }
        _process_args.return_value = ProcessArg('status')
        output = _execute()
        self.assertTrue(expected in output)

    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch('healthmonitoring.collectors.etl.connectors.SpecificationUtil')
    @patch('healthmonitoring.collectors.etl.connectors.ApplicationConfig')
    @patch('healthmonitoring.collectors.etl.connectors._process_args')
    @patch.object(DateTimeUtil, 'get_current_hour_date',
                  return_value='2020-05-07 19:15:32')
    def test_lag(self, mock_get_current_hour_date,
                 _process_args, ApplicationConfig,
                 MockSpecificationUtil):
        MockSpecificationUtil.get_field.return_value = 'cloudera'
        MockSpecificationUtil.get_property.return_value = ['MGW_E_1']
        expected = {
            'Time': '2020-05-07 19:15:32',
            'Connector': 'MGW_E_1_SINK_CONN',
            'LagCount(Messages)': '0',
            'LagCount(Records)': '0'
        }
        _process_args.return_value = ProcessArg('lag')
        output = _execute()
        self.assertTrue(expected in output)


if __name__ == "__main__":
    unittest.main()

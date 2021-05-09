'''
Created on 06-Jun-2020

@author: praveenD
'''
import unittest
import datetime

from mock import patch
from healthmonitoring.collectors.etl.topologies_status import _execute
from healthmonitoring.collectors.utils.command import Command, CommandExecutor
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import QueryExecutor


class Test_topologies_status(unittest.TestCase):
    TODAY_DATE_TIME = '2020-06-06 17:22:00'

    @staticmethod
    def execute(db_type, query, **args):
        return [
            ('AV_STREAMING', 'AV_STREAMING_1/AV_STREAMING_1',
             'AV_STREAMING_1/AV_STREAMING_1', 'Usage_AV_STREAMING_1_LoadJob',
             '5MIN', datetime.datetime(2020, 6, 6, 17, 10)),
            ('BCSI', 'BCSI_1/BCSI_1', 'BCSI_1/BCSI_1', 'Usage_BCSI_1_LoadJob',
             'DAY', datetime.datetime(2020, 6, 6, 0, 0)),
            ('BROADBAND_CMTS', 'FL_BROADBAND_1/BROADBAND_CMTS_1',
             'FL_BROADBAND_1/BROADBAND_1', 'Usage_BROADBAND_1_LoadJob', '5MIN',
             datetime.datetime(2020, 6, 3, 15, 25))
        ]

    @staticmethod
    def get_output(command, **args):
        output = {
            Command.NODE_PING_STATUS:
            '1 packets transmitted, 1 received, 0% packet loss, time 0ms',  # noqa : 501
            Command.ETL_INPUT_TIME:
            '2020-06-06 17:15',
            Command.ETL_OUTPUT_TIME:
            '2020-06-06 17:13',
            Command.ETL_INPUT_OUTPUT_TIME:
            '2020-06-06 17:10',
            Command.ETL_CONNECTOR_STATUS:
            """ngdb      54988  54930 22 Apr14 ?        12-03:46:56 /usr/java/latest/bin/java -DETL_HOME=//opt/nsn/ngdb/etl -DETL_SERVICE -Dsystem=ca4ci -Dcomponent=etl -Dappender=routing -DlogDirectory=/var/local/kafka/log/AV_STREAMING/source -Xmx1280m -Xms1280m -Dlogfile.name=AV_STREAMING_source.log -cp /opt/nsn/ngdb/etl/app/lib/*:/opt/nsn/ngdb/etl/app/lib/3rdparty_with_precedence/*:/opt/nsn/ngdb/etl/app/lib/3rdparty/*:/etc/hadoop/conf -Dspring.profiles.active=baremetal -DrtbInputType=csv com.nsn.ngdb.etl.service.ETLServiceStarter -adaptation AV_STREAMING -version 1 -topology AV_STREAMING -connectortype source
          root      55170      1  0 Apr14 ?        00:00:00 su - ngdb -c . ~/.bash_profile && sleep 1 && nohup /usr/java/latest/bin/java -DETL_HOME=//opt/nsn/ngdb/etl -DETL_SERVICE -Dsystem=ca4ci  -Dcomponent=etl -Dappender=routing -DlogDirectory=/var/local/kafka/log/AV_STREAMING/sink -Xmx1280m -Xms1280m  -Dlogfile.name=AV_STREAMING_sink.log -cp /opt/nsn/ngdb/etl/app/lib/*:/opt/nsn/ngdb/etl/app/lib/3rdparty_with_precedence/*:/opt/nsn/ngdb/etl/app/lib/3rdparty/*:/etc/hadoop/conf  -Dspring.profiles.active=baremetal  -DrtbInputType=csv com.nsn.ngdb.etl.service.ETLServiceStarter -adaptation  AV_STREAMING -version 1 -topology AV_STREAMING -connectortype  sink > //opt/nsn/ngdb/etl/app/log/etl-management-service_14-04-2020_135429.log 2>&1
          ngdb      55171  55170  0 Apr14 ?        00:00:00 -bash -c . ~/.bash_profile && sleep 1 && nohup /usr/java/latest/bin/java -DETL_HOME=//opt/nsn/ngdb/etl -DETL_SERVICE -Dsystem=ca4ci  -Dcomponent=etl -Dappender=routing -DlogDirectory=/var/local/kafka/log/AV_STREAMING/sink -Xmx1280m -Xms1280m  -Dlogfile.name=AV_STREAMING_sink.log -cp /opt/nsn/ngdb/etl/app/lib/*:/opt/nsn/ngdb/etl/app/lib/3rdparty_with_precedence/*:/opt/nsn/ngdb/etl/app/lib/3rdparty/*:/etc/hadoop/conf  -Dspring.profiles.active=baremetal  -DrtbInputType=csv com.nsn.ngdb.etl.service.ETLServiceStarter -adaptation  AV_STREAMING -version 1 -topology AV_STREAMING -connectortype  sink > //opt/nsn/ngdb/etl/app/log/etl-management-service_14-04-2020_135429.log 2>&1
          ngdb      55285  55171 40 Apr14 ?        21-07:34:03 /usr/java/latest/bin/java -DETL_HOME=//opt/nsn/ngdb/etl -DETL_SERVICE -Dsystem=ca4ci -Dcomponent=etl -Dappender=routing -DlogDirectory=/var/local/kafka/log/AV_STREAMING/sink -Xmx1280m -Xms1280m -Dlogfile.name=AV_STREAMING_sink.log -cp /opt/nsn/ngdb/etl/app/lib/*:/opt/nsn/ngdb/etl/app/lib/3rdparty_with_precedence/*:/opt/nsn/ngdb/etl/app/lib/3rdparty/*:/etc/hadoop/conf -Dspring.profiles.active=baremetal -DrtbInputType=csv com.nsn.ngdb.etl.service.ETLServiceStarter -adaptation AV_STREAMING -version 1 -topology AV_STREAMING -connectortype sink""" # noqa: 501
        }
        return output[command]

    @patch.object(DateTimeUtil,
                  'now',
                  return_value=datetime.datetime.strptime(
                      TODAY_DATE_TIME, '%Y-%m-%d %H:%M:%S'))
    @patch.object(CommandExecutor, 'get_output', new=get_output)
    @patch.object(QueryExecutor, 'execute', new=execute)
    def test_topologies_status(self, arg1):
        output = _execute()

        self.assertListEqual(output, [{
            'input_delay_time': 7,
            'input_time': '2020-06-06 17:15',
            'output_delay_time': 9,
            'output_time': '2020-06-06 17:13',
            'sslave1.nokia.com': 0,
            'sslave2.nokia.com': 0,
            'sslave3.nokia.com': 0,
            'usage_job': 'Usage_AV_STREAMING_1_LoadJob',
            'partition': '5MIN',
            'usage_boundary': '2020-06-06 17:10:00',
            'usage_delay': '12',
            'Topology': 'AV_STREAMING',
            'Time': '2020-06-06 17:22:00'
        }, {
            'input_delay_time': 7,
            'input_time': '2020-06-06 17:15',
            'output_delay_time': 9,
            'output_time': '2020-06-06 17:13',
            'sslave1.nokia.com': 0,
            'sslave2.nokia.com': 0,
            'sslave3.nokia.com': 0,
            'usage_job': 'Usage_BCSI_1_LoadJob',
            'partition': 'DAY',
            'usage_boundary': '2020-06-06 00:00:00',
            'usage_delay': '1042',
            'Topology': 'BCSI',
            'Time': '2020-06-06 17:22:00'
        }, {
            'input_delay_time': 7,
            'input_time': '2020-06-06 17:15',
            'output_delay_time': 9,
            'output_time': '2020-06-06 17:13',
            'sslave1.nokia.com': 0,
            'sslave2.nokia.com': 0,
            'sslave3.nokia.com': 0,
            'usage_job': 'Usage_BROADBAND_1_LoadJob',
            'partition': '5MIN',
            'usage_boundary': '2020-06-03 15:25:00',
            'usage_delay': '4437',
            'Topology': 'BROADBAND_CMTS',
            'Time': '2020-06-06 17:22:00'
        }])


if __name__ == '__main__':
    unittest.main()

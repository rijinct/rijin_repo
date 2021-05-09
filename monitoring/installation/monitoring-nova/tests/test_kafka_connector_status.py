import unittest
from xml.dom.minidom import parseString
import multiprocessing_util
import importlib
import kafka_connectors_status as kcs


class TestKafkaConnectorStatus(unittest.TestCase):

    def get_xml(self):
        return parseString('''<?xml version="1.0" encoding="UTF-8"?>
<config>
    <TOPOLOGIES>
        <property Name="SMS_1" lagThreshold="50000" />
        <property Name="VOICE_1" lagThreshold="50000"/>
    </TOPOLOGIES>
</config> ''')

    def get_status(self, topology, connector):
        return 1, ''

    def set_up_dependencies(self):
        kcs.date = ''
        kcs.get_monitoring_xml = self.get_xml
        kcs.get_topology_status = self.get_status

    def test_get_list_of_topologies(self):
        self.set_up_dependencies()
        topologies = kcs.get_list_of_topologies()
        self.assertEqual(topologies, ['SMS', 'VOICE'])

    @staticmethod
    def mock_multiprocessing(function):
        return function

    def test_get_connector_and_task_status(self):
        multiprocessing_util.multiprocess = TestKafkaConnectorStatus.\
            mock_multiprocessing
        importlib.reload(kcs)
        self.set_up_dependencies()
        results = []
        kcs.get_connector_and_task_status('SMS', results)
        self.assertEqual(results, [{
            'Date': '',
            'Name': 'SMS_SOURCE',
            'ConnectorNode': '',
            'TaskNode': '',
            'ConnectorStatus': '2',
            'TotalTasks': '0',
            'RunningTasks': '0',
            'TaskStatus': '2'
        }, {
            'Date': '',
            'Name': 'SMS_SINK',
            'ConnectorNode': '',
            'TaskNode': '',
            'ConnectorStatus': '2',
            'TotalTasks': '0',
            'RunningTasks': '0',
            'TaskStatus': '2'
        }])

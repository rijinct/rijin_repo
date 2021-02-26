'''
Created on 07-Aug-2020

@author: pratyaks
'''
import unittest
import sys
sys.path.append('framework/')

import framework.query_generator  # noqa E402
import framework.writer  # noqa E402
import framework.aggregator  # noqa E402
import framework.reader  # noqa E402
from framework.util import get_resource_path  # noqa E402
from framework.specification import SourceSpecification  # noqa E402
from framework.specification import TargetSpecification  # noqa E402
from framework.connector import SparkConnector  # noqa E402
from framework.writer import DataWriter  # noqa E402
from reader import ReaderAndViewCreator  # noqa E402
from framework.aggregator import Aggregator  # noqa E402
from framework.query_generator import QueryGenerator  # noqa E402


class MockLogger:
    @staticmethod
    def get_logger(name):
        return MockLogger()

    def info(self, msg, *args, **kwargs):
        pass


class TestAggregator(unittest.TestCase):
    hdfs_path = "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"
    _hive_schema = "project"
    extension_conf = get_resource_path("dimension_extension.yaml", True)
    YAML_PATH = [get_resource_path("dimension_extension.yaml", True)]
    a = [
        SourceSpecification(hdfs_path, _hive_schema, 1594575000000,
                            1594575900000, 'UTC', extension_conf, 15)
    ]

    b = TargetSpecification(hdfs_path, _hive_schema, 10, 100, 'UTC', 10,
                            YAML_PATH)
    d = [
        SourceSpecification(hdfs_path, _hive_schema, 1594575000000,
                            1594575900000, 'UTC', extension_conf, 15)
    ]

    def _execute_sql(self, spec):
        pass

    def write(self):
        pass

    def read_and_create_view(self):
        pass

    def set_up(self):
        framework.aggregator.SparkConnector.get_logger = MockLogger.get_logger
        framework.aggregator.DataWriter._execute_sql = \
            TestAggregator._execute_sql
        framework.aggregator.DataWriter.write = TestAggregator.write
        framework.aggregator.ReaderAndViewCreator.read_and_create_view = \
            TestAggregator.read_and_create_view

    def test_start_loading(self):
        self.set_up()
        c = Aggregator(self.a, self.b, self.d)
        self.assertEqual(None, Aggregator.start_loading(c))

    def test_read_sources(self):
        self.set_up()
        c = Aggregator(self.a, self.b, self.d)
        self.assertIsInstance(c._read_sources(self.a)[0], ReaderAndViewCreator)


if __name__ == "__main__":
    unittest.main()

'''
Created on 10 Aug, 2020

@author: amitakum
'''
import unittest
from pandas import DataFrame
import sys
sys.path.append('framework/')
import framework.reader  # noqa E402
from framework.specification import SourceSpecification, \
    TargetSpecification  # noqa E402
from framework.util import get_resource_path  # noqa E402
from framework.reader import ReaderAndViewCreator  # noqa E402


class MockPysparkSession:
    def __init__(self, app_name):
        self._session = app_name

    @property
    def session(self):
        return self._session

    def add_file(self, file):
        self._session.sparkContext.addFile(file)

    def set(self, key, value):
        self._session.conf.set(key, value)

    def get_logger(self):
        return self._session.sparkContext._jvm.org.apache.log4j

    def get_schema(self, table_name):
        return ["col1", "col2", "col3"]

    def read_schema(self, schema, *locs):
        df = DataFrame(['1', '2'])
        return MockSparkDataFrame(df)

    def createDataFrame(self, rows, schema):
        df = DataFrame()
        return MockSparkDataFrame(df)

    def sql(self, query):
        return self._session.sql(query)

    def update_metastore(self, table_name):
        self._session.catalog.recoverPartitions(table_name)

    def read_parquet(self, loc):
        df = DataFrame(['1', '2'])
        return MockSparkDataFrame(df)


class MockSparkDataFrame:
    def __init__(self, df):
        self._df = df

    @property
    def count(self):
        return self._df.count

    @property
    def schema(self):
        return ""

    def repartition_and_persist(self, size, level):
        return MockSparkDataFrame(self._df)

    def createOrReplaceTempView(self, view_name):
        pass

    def drop(self, col_name):
        return MockSparkDataFrame(self._df)


class MockLogger:
    @staticmethod
    def get_logger(name):
        return MockLogger()

    def info(self, msg, *args, **kwargs):
        pass


def mock_get_locs(obj):
    return []


def mock_is_empty_path(obj):
    return False


class TestReaderAndViewCreator(unittest.TestCase):
    LB = 1594560600000

    UB = 1594564200000

    TARGET_SPEC = TargetSpecification(
        "hdfs://projectcluster/ngdb/ps/AV_STREAMING_1/AV_STREAMING_SEGG_1_HOUR",
        "project", LB, UB, "Default", 0,
        [r'{}'.format(get_resource_path('custom_pyspark_settings.yml', True))])

    SOURCE_SPEC = SourceSpecification(
        "hdfs://projectcluster/ngdb/es/SUBS_EXTENSION_1/SUBS_EXTENSION_1",
        "project", LB, UB, "Default",
        r'{}'.format(get_resource_path('dimension_extension.yaml', True)))

    def get_locs(self, add_path):
        return [
            'hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY/dt=1594575000000/tz=Default/',  # noqa : 501
            'hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY/dt=1594575300000/tz=Default/'] # noqa : 501

    def setUp(self):
        TestReaderAndViewCreator.SOURCE_SPEC._get_all_dts = mock_get_locs
        SourceSpecification.is_empty_path = mock_is_empty_path

    def test_read_and_create_view_empty(self):
        framework.reader.SparkConnector.get_logger = MockLogger.get_logger
        rs = ReaderAndViewCreator(TestReaderAndViewCreator.SOURCE_SPEC, False)
        framework.reader.SparkConnector.spark_session = MockPysparkSession(
            "project")
        rs.read_and_create_view()
        self.assertIsInstance(rs._df, MockSparkDataFrame)

    def test_read_and_create_view_normal(self):
        framework.reader.SparkConnector.get_logger = MockLogger.get_logger
        rs = ReaderAndViewCreator(TestReaderAndViewCreator.SOURCE_SPEC, False)
        SourceSpecification.get_locs = TestReaderAndViewCreator.get_locs
        framework.reader.SparkConnector.spark_session = MockPysparkSession(
            "project")
        rs.read_and_create_view()
        self.assertIsInstance(rs._df, MockSparkDataFrame)

    def test_read_and_create_view_one_to_many(self):
        framework.reader.SparkConnector.get_logger = MockLogger.get_logger
        rs = ReaderAndViewCreator(TestReaderAndViewCreator.SOURCE_SPEC, True)
        SourceSpecification.get_locs = TestReaderAndViewCreator.get_locs
        framework.reader.SparkConnector.spark_session = MockPysparkSession(
            "project")
        rs.read_and_create_view()
        self.assertIsInstance(rs._df, MockSparkDataFrame)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

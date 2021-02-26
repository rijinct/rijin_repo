'''
Created on 30-Jul-2020

@author: a4yadav
'''
import sys
import unittest

from numpy import NaN
from pandas import DataFrame

sys.path.append('framework/')
sys.path.append('tools/')
import framework.dataframe  # noqa E402
import framework.reader  # noqa E402
import framework.query_generator  # noqa E402
from framework.reader import ReaderAndViewCreator  # noqa E402
from framework.specification import SourceSpecification  # noqa E402
from framework.specification import TargetSpecification  # noqa E402
from framework.util import get_resource_path  # noqa E402
from framework.writer import DataWriter  # noqa E402
from framework.writer import _DataFrameJoiner  # noqa E402
from framework.writer import _DynamicPartitionWriter  # noqa E402
from tools.tools_util import read_yaml  # noqa E402
#from tools_util import read_yaml  # noqa E402

LB = 1594560600000

UB = 1594564200000

TARGET_SPEC = TargetSpecification(
    "hdfs://projectcluster/ngdb/ps/AV_STREAMING_1/AV_STREAMING_SEGG_1_HOUR",
    "project",
    LB,
    UB,
    "Default",
    0,
    [
        r'{}'.format(
            get_resource_path('Perf_SMS_SEGG_1_HOUR_AggregateJob.yaml', True)),
        r'{}'.format(
            get_resource_path('Perf_VOLTE_SEGG_1_HOUR_AggregateJob.yaml',
                              True)),  # noqa: 501
    ])

SOURCE_SPEC = SourceSpecification(
    "hdfs://projectcluster/ngdb/es/SUBS_EXTENSION_1/SUBS_EXTENSION_1", "project",
    LB, UB, "Default",
    read_yaml(r'{}'.format(get_resource_path('dimension_extension.yaml',
                                             True))))

OUTPUT_DF = df = DataFrame([[1, 'dim1', 20], [2, 'dim2', 30], [3, 'NA', 40],
                            [4, 'dim4', 50], [5, 'dim5', -88]],
                           columns=['id', 'dim_1', 'dim_2'])


class MockLogger:
    @staticmethod
    def get_logger(name):
        return MockLogger()

    def info(self, msg, *args, **kwargs):
        pass


class MockPysparkSession:
    def __init__(self, app_name):
        self._session = app_name

    @property
    def session(self):
        return self._session

    def get_schema(self, table_name):
        return ["col1", "col2", "col3"]

    def read_schema(self, schema, *locs):
        df = DataFrame(
            [[1, 10, 20, 'A3', 'A4', 'A5'], [2, 20, 30, 'B3', 'B4', 'B5'],
             [3, NaN, 40, 'C3', 'C3', 'C5'], [4, 40, 50, 'D3', 'D4', NaN],
             [5, 50, NaN, 'E3', 'E5', 'E5'], [6, 60, 70, 'E3', 'E5', 'E5']],
            columns=[
                'id', 'subs_dim_1', 'subs_dim_2', 'subs_dim_3', 'subs_dim_4',
                'subs_dim_5'
            ])
        return MockSparkDataFrame(df)

    def createDataFrame(self, rows, schema):
        df = DataFrame(
            [[1, 10, 20, 'A3', 'A4', 'A5'], [2, 20, 30, 'B3', 'B4', 'B5'],
             [3, NaN, 40, 'C3', 'C3', 'C5'], [4, 40, 50, 'D3', 'D4', NaN],
             [5, 50, NaN, 'E3', 'E5', 'E5'], [6, 60, 70, 'E3', 'E5', 'E5']],
            columns=[
                'id', 'subs_dim_1', 'subs_dim_2', 'subs_dim_3', 'subs_dim_4',
                'subs_dim_5'
            ])
        return MockSparkDataFrame(df)

    def sql(self, query):
        df = DataFrame([[1, 'Data1', 10], [2, 'Data2', 20], [3, 'Data3', 30],
                        [4, 'Data4', 40], [5, 'Data5', 50]],
                       columns=['id', 'dim_1', 'dim_2'])
        return MockSparkDataFrame(df)

    def update_metastore(self, table_name):
        pass

    def read_parquet(self, schema, *locs):
        df = DataFrame(
            [[1, 10, 20, 'A3', 'A4', 'A5'], [2, 20, 30, 'B3', 'B4', 'B5'],
             [3, NaN, 40, 'C3', 'C3', 'C5'], [4, 40, 50, 'D3', 'D4', NaN],
             [5, 50, NaN, 'E3', 'E5', 'E5'], [6, 60, 70, 'E3', 'E5', 'E5']],
            columns=[
                'id', 'subs_dim_1', 'subs_dim_2', 'subs_dim_3', 'subs_dim_4',
                'subs_dim_5'
            ])
        return MockSparkDataFrame(df)

    def get_conf(self):
        return {"spark.yarn.appMasterEnv.DYNAMIC_RECORD_COUNT": 300000}

    def get_data_types(self, table_name, exclude_col):
        columns_info = [('id', 'int64'), ('dim_1', 'string'),
                        ('dim_2', 'int64'), ('dim_3', 'string'),
                        ('dim_4', 'string'), ('dim_5', 'string')]
        return columns_info


class MockSparkDataFrame:
    def __init__(self, df):
        self._df = df

    @property
    def columns(self):
        return list(self._df.columns)

    @property
    def dtypes(self):
        return [(key, str(value).replace('object', 'string'))
                for key, value in dict(self._df.dtypes).items()]

    @property
    def schema(self):
        return list(self._df.columns)

    def alias(self, alias):
        self._df.name = alias
        return MockSparkDataFrame(self._df)

    def filter(self, condition):
        if condition == 'Col="5" ':
            return MockSparkDataFrame(self._df)
        else:
            return MockSparkDataFrame(
                self._df.query(condition.replace("'", '')))

    def column_alias(self, name, alias):
        self._df.rename(columns={name: alias}, inplace=True)
        return MockSparkDataFrame(self._df)

    def select(self, cols):
        if cols == ['col3', 'col1', 'col2']:
            return MockSparkDataFrame(
                self._df.reindex(['Col3', 'Col1', 'Col2'], axis=1))
        else:
            return MockSparkDataFrame(self._df[cols])

    def drop(self, columns):
        if columns in self.columns:
            return MockSparkDataFrame(self._df.drop(columns, axis=1))
        else:
            return MockSparkDataFrame(self._df)

    def fill_na(self, value, column):
        self._df[[column]] = self._df[[column]].fillna(value)
        return MockSparkDataFrame(self._df)

    def cast(self, col, dtype):
        self._df[col] = self._df[col].astype(dtype.replace('string', 'object'))
        return MockSparkDataFrame(self._df)

    def join(self, df, condition, how):
        df = DataFrame(
            [[1, 'dim1', 20, 'A3', 'A4', 'A5', 'Data1', 'Info1', 1],
             [2, 'dim2', 30, 'B3', 'B4', 'B5', 'Data2', 'Info2', 2],
             [3, NaN, 40, 'C3', 'C3', 'C5', 'Data3', 'Info3', 3],
             [4, 'dim4', 50, 'D3', 'D4', NaN, 'Data4', 'Info4', 4],
             [5, 'dim5', NaN, 'E3', 'E5', 'E5', 'Data5', 'Info5', 5]],
            columns=[
                'id', 'dim_1', 'dim_2', 'dim_3', 'dim_4', 'dim_5', 'col1',
                'col2', 'temp_id'
            ])
        return MockSparkDataFrame(df)

    def repartition_and_persist(self, size, level):
        return MockSparkDataFrame(self._df)

    def groupBy(self, columns):
        return MockSparkDataFrame(self._df)

    def orderBy(self, columns, ascending):
        return MockSparkDataFrame(self._df)

    def collect(self):
        return [{'count': 5, 'Col': 5}]

    def unpersist(self):
        pass

    def write_data(self, files, path):
        pass

    def createOrReplaceTempView(self, view_name):
        pass

    @staticmethod
    def asDict(row):
        return row

    def count(self):
        return 3000

    def toDF(self, cols):
        return self


def set_up():
    SOURCE_SPEC.is_empty_path = lambda x: False
    SOURCE_SPEC.get_locs = lambda x, addnl_path='': []
    framework.reader.SparkConnector.spark_session = MockPysparkSession("project")
    framework.reader.SparkConnector.get_logger = MockLogger.get_logger
    reader = ReaderAndViewCreator(SOURCE_SPEC, False)
    reader.read_and_create_view()
    return reader


class TestDataWriter(unittest.TestCase):
    def setUp(self):
        self._reader = set_up()
        self._target_spec = TargetSpecification(
            "hdfs://projectcluster/ngdb/ps/AV_STREAMING_1"
            "/AV_STREAMING_SEGG_1_HOUR",
            "project",
            LB,
            UB,
            "Default",
            0,
            [
                r'{}'.format(
                    get_resource_path('Perf_SMS_SEGG_1_HOUR_AggregateJob.yaml',
                                      True))
            ])

    def test_write(self):
        data_writer = DataWriter(TARGET_SPEC, [self._reader], [])
        data_writer.write()
        df = DataFrame([[1, 'Data1', 10], [2, 'Data2', 20], [3, 'Data3', 30],
                        [4, 'Data4', 40], [5, 'Data5', 50]],
                       columns=['id', 'dim_1', 'dim_2'])
        self.assertTrue(df.equals(data_writer._df._df))

    def test_write_2(self):
        data_writer = DataWriter(self._target_spec, [self._reader], [])
        data_writer.write()
        self.assertTrue(OUTPUT_DF.equals(data_writer._df._df))


class TestDynamicPartitionWriter(unittest.TestCase):
    def setUp(self):
        framework.reader.SparkConnector.get_logger = MockLogger.get_logger
        framework.writer.LocalDataFrame.asDict = MockSparkDataFrame.asDict
        self._df = DataFrame([[1, 'a', None], [2, 'c', None]],
                             columns=['Col1', 'Col2', 'Col3'])
        self.dynamic_partition_writer = _DynamicPartitionWriter(
            MockSparkDataFrame(self._df), TARGET_SPEC.yaml_spec[0],
            TARGET_SPEC)

    def test_write(self):
        self.dynamic_partition_writer.write()
        self.assertTrue(self.dynamic_partition_writer._df._df.equals(self._df))


class TestDataFrameJoiner(unittest.TestCase):
    def setUp(self):
        self._reader = set_up()
        df = DataFrame([[1, 'Data1', 10], [2, 'Data2', 20], [3, 'Data3', 30],
                        [4, 'Data4', 40], [5, 'Data5', 50]],
                       columns=['id', 'dim_1', 'dim_2'])
        self.data_frame_joiner = _DataFrameJoiner([self._reader],
                                                  MockSparkDataFrame(df),
                                                  TARGET_SPEC,
                                                  SOURCE_SPEC.extensions)

    def test_join(self):
        output_df = self.data_frame_joiner.join()._df
        self.assertTrue(OUTPUT_DF.equals(output_df))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

import datetime
import threading
from pathlib import Path
from threading import Thread

from connector import SparkConnector
from specification import SourceSpecification


class ReaderAndViewCreator:
    def __init__(self,
                 source_spec: SourceSpecification,
                 one_to_many=False,
                 addnl_path=''):
        self._source_spec = source_spec
        self._addnl_path = addnl_path
        self._one_to_many = one_to_many
        self._df = None
        self.logger = SparkConnector.get_logger(__name__)

    @property
    def source_spec(self):
        return self._source_spec

    @property
    def df(self):
        return self._df

    def read_and_create_view(self, view_name=''):
        self._view_name = view_name
        start = datetime.datetime.now()
        locs = self.source_spec.get_locs(self._addnl_path)
        if locs:
            if self._read_locs(locs):
                schema = SparkConnector.spark_session.get_schema(
                    self.source_spec.get_table_name())
                self._df = SparkConnector.spark_session.read_schema(
                    schema, *locs)
            if self._one_to_many:
                self._create_cache()
            self._create_view()
        else:
            self._create_empty_view()
        self.logger.info("Reading time for source {} took {} s".format(
            self.source_spec.hdfs_path,
            datetime.datetime.now() - start))

    def _read_locs(self, locs):
        table_name = self.source_spec.get_table_name()
        schema = SparkConnector.spark_session.get_schema(table_name)
        self._df = SparkConnector.spark_session.createDataFrame(
            [], schema).drop('tz').drop('dt')
        output_df_list = [None for i in range(len(locs))]
        threads = []
        self._spawn_reading_threads(output_df_list, locs, threads)
        if self._schema_equals(output_df_list):
            return True
        self._df = self._merge_and_modify_schema(output_df_list)
        return False

    def _spawn_reading_threads(self, df_list, locs, threads):
        for i in range(len(locs)):
            thread = threading.Thread(target=ReaderAndViewCreator._read_loc,
                                      args=(locs[i], df_list, i))
            thread.start()
            self.logger.info("Launched thread {} ".format(i))
            threads.append(thread)
        for job in threads:
            job.join()

    @staticmethod
    def _read_loc(loc, out, i):
        out[i] = SparkConnector.spark_session.read_parquet(loc)

    def _schema_equals(self, df_list):
        for df in df_list:
            if str(df.schema) != str(self.df.schema):
                self.logger.info(
                    "schema for one of the partition doesn't match...")
                self.logger.info("not matching schema {}".format(df.schema))
                self.logger.info("latest schema {}".format(self.df.schema))
                return False
        self.logger.info("schema matching...")
        return True

    def _merge_and_modify_schema(self, output_df_list):
        for i in range(len(output_df_list)):
            df = output_df_list[i]
            if len(self._df.columns) != len(df.columns):
                df = self._add_new_columns(df)
            self._df = self._df.union(df)
        return self._df

    def _add_new_columns(self, df):
        df_columns = df.columns
        new_columns = self._df.columns
        for new_col in new_columns[len(df_columns):]:
            self.logger.info("column added in df: {}".format(new_col))
            df = df.add_column(new_col, None)
        return df

    def _create_cache(self):
        start = datetime.datetime.now()
        self._df = self._df.repartition_and_persist(
            5, "DISK_ONLY")  # repartition count
        self.logger.info("Caching of source {} took {} s".format(
            self.source_spec.hdfs_path,
            datetime.datetime.now() - start))

    def _create_view(self):
        view_name = self._get_view_name()
        self._df.createOrReplaceTempView(view_name)
        self.logger.info("View created : {}".format(view_name))

    def _create_empty_view(self):
        table_name = self.source_spec.get_table_name()
        schema = SparkConnector.spark_session.get_schema(table_name)
        self._df = SparkConnector.spark_session.createDataFrame([], schema)
        view_name = self._get_view_name()
        self._df.createOrReplaceTempView(view_name)
        self.logger.info("Empty view created: {}".format(view_name))

    def _get_view_name(self):
        spec_type = self.source_spec.spec_type.upper()
        name = Path(self.source_spec.hdfs_path).name
        if len(self._view_name) > 0:
            return self._view_name
        else:
            return "{spec_type}1_{name}".format(spec_type=spec_type, name=name)

    def _remove_cache(self):
        self.logger.info("Un-persisting cache for source: {}".format(
            self.source_spec.hdfs_path))
        self._df.unpersist()


class ReaderAndViewCreatorThread(Thread):
    def __init__(self, reader):
        Thread.__init__(self)
        self.reader = reader

    def start_reading(self):
        try:
            self.reader.read_and_create_view()
        except Exception as e:
            raise e

    def run(self):
        self.exc = None
        try:
            self.start_reading()
        except Exception as e:
            self.exc = e

    def join(self, timeout=None):
        threading.Thread.join(self)
        if self.exc:
            raise self.exc

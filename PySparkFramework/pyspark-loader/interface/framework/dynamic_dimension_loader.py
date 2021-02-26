import datetime
import time
import os
from connector import SparkConnector
from specification import SourceSpecification
from specification import TargetSpecification
from reader import ReaderAndViewCreator
from dynamic_dimension_mapper import DynamicDimensionMapper
from dynamic_dimension_mapper import DynamicDimensionConstants as const
from dimension_common import process_args, get_sources_and_target
from dimension_common import get_hdfs_corrupt_filename
from util import get_dynamic_dimension_views, get_intermediate_table_name
import pyspark.sql.functions as functions
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from util import get_job_name
from pyspark.sql.types import LongType
from pathlib import Path
from connection import LocalSession
from dataframe import LocalDataFrame
from pyspark import StorageLevel


class DynamicDimensionLoader:
    def __init__(self, source_spec_list: [SourceSpecification],
                 target_spec: TargetSpecification):
        self._source_spec_list = source_spec_list
        self._target_spec = target_spec
        self._es_dim_id_table = self._target_spec.get_table_name().replace(
            '_extension_', '_id_')
        self._logger = SparkConnector.get_logger(__name__)

    def read(self):
        self.__pre_cleanup()
        es_dim_ext_df = self.__read_es_source(self._source_spec_list,
                                              const.ADDNL_PATH)
        self.__get_dynamic_dimension_map(es_dim_ext_df.columns)
        self.__add_default_dimid()
        es_dim_id_df = self.__read_es_source(self.__get_dim_id_source())
        self._es_dim_id_count = es_dim_id_df.count()
        self.__read_ext_source()

    def __pre_cleanup(self):
        self._logger.info("Remove latest1 files pre cleanup")
        os.system(
            const.REMOVE_TEMP_LATEST_FILE.format(self._target_spec.hdfs_path))

    def __read_es_source(self, source_spec_list, addnl_path=''):
        for source_spec in source_spec_list:
            start = datetime.datetime.now()
            reader = ReaderAndViewCreator(source_spec, False, addnl_path)
            reader.read_and_create_view('{0}_{1}_VIEW'.format(
                source_spec._spec_type,
                Path(source_spec._hdfs_path).name).upper())
            end = datetime.datetime.now()
            self._logger.info("Total Reading Time {0} : {1} s".format(
                source_spec.get_table_name(), end - start))
            return reader.df

    def __get_dynamic_dimension_map(self, columns):
        views = get_dynamic_dimension_views(self._target_spec.hdfs_path)
        mapper = DynamicDimensionMapper()
        self.__dd_dict = mapper.generate(views, columns)

    def __add_default_dimid(self):
        get_query = self.__dd_dict[const.QUERY_GET_DEFAULT_DIMID]
        get_query = get_query.format(self._es_dim_id_table)
        add_query = self.__dd_dict[const.QUERY_ADD_DEFAULT_DIMID]
        add_query = add_query.format(self._es_dim_id_table)
        df = SparkConnector.spark_session.sql(get_query)
        if df.head(1):
            self._logger.info('Default dimension IDs are already added')
        else:
            self._logger.info('Adding default dimension IDs')
            df = SparkConnector.spark_session.sql(add_query)
            self.__refresh_table(self._es_dim_id_table)

    def __get_dim_id_source(self):
        id_source_spec_list = []
        id_source_spec_list.append(
            SourceSpecification(
                self._target_spec.hdfs_path.replace('_EXTENSION_', '_ID_'),
                self._target_spec.hive_schema))
        return id_source_spec_list

    def __read_ext_source(self):
        inter_tn = get_intermediate_table_name(
            self._target_spec.hdfs_path).upper()

        query = self.__prechecks(inter_tn)
        start = datetime.datetime.now()
        self.__fromdt = int(time.time()) * 1000
        dt = datetime.datetime.now()
        dt = datetime.datetime(*dt.timetuple()[:4])
        self.__current_partition = int(dt.strftime('%s')) * 1000

        self.__read_and_create_view_table_name(inter_tn, query)
        end = datetime.datetime.now()
        self._logger.info("Total Reading Time {0} : {1} s".format(
            inter_tn, end - start))

    def __prechecks(self, table_name):
        query = self.__dd_dict[const.QUERY_GET_CSV_DUPLICATE_COUNT]
        query = query.format(self._target_spec.hive_schema, table_name)
        self._logger.info('QUERY_GET_CSV_DUPLICATE_COUNT {0}'.format(query))
        df = SparkConnector.spark_session.sql(query)
        hdfs_fileloc = get_hdfs_corrupt_filename(self._target_spec.hdfs_path)
        self._logger.info('Corrupt hdfs file {0}'.format(hdfs_fileloc))
        os.system("hdfs dfs -rm {0}/*".format(hdfs_fileloc))
        if df.head(1):
            self.__write_duplicates(table_name, hdfs_fileloc)
            return self.__dd_dict[const.QUERY_GET_CSV_COLUMNS_ROWNO]
        else:
            return self.__dd_dict[const.QUERY_GET_CSV_COLUMNS]

    def __write_duplicates(self, table_name, hdfs_fileloc):
        query = self.__dd_dict[const.QUERY_GET_CSV_DUPLICATE]
        query = query.format(self._target_spec.hive_schema, table_name)
        self._logger.info('QUERY_GET_CSV_DUPLICATE {0}'.format(query))
        df = SparkConnector.spark_session.sql(query)
        df.coalesce_write_format(1, 'csv', 'header', 'overwrite', 'sep',
                                 hdfs_fileloc)

    def __read_and_create_view_table_name(self, table_name, query):
        query = query.format(self._target_spec.hive_schema, table_name)
        self._logger.info('QUERY_GET_CSV_COLUMNS {0}'.format(query))
        df = SparkConnector.spark_session.sql(query)
        df = df.add_column('from_dt',
                           self.__fromdt).cast('from_dt', LongType())
        df = df.add_column('to_dt', None).cast('to_dt', LongType())
        df = df.add_column('subs_dim_id', None).cast('subs_dim_id', LongType())
        df.createOrReplaceTempView("{}_VIEW".format(table_name))
        return df

    def generate_es_ext_dim_extension_view(self):
        start = datetime.datetime.now()
        self._es_ext_dim_extention_df = self.__get_es_ext_dim_extention_view()
        end = datetime.datetime.now()
        self._logger.info(
            "Total Generating ES_EXT_DIM_EXTENSION_VIEW Time : {0} s".format(
                end - start))

    def __get_es_ext_dim_extention_view(self):
        query = self.__dd_dict[const.QUERY_GET_ES_EXT_DIM_EXTENSION]
        self._logger.info('QUERY_GET_ES_EXT_DIM_EXTENSION {0}'.format(query))
        df = SparkConnector.spark_session.sql(query)
        self._logger.info(const.VIEW_ES_EXT_DIM_EXTENSION)
        df.createOrReplaceTempView(
            self.__dd_dict[const.VIEW_ES_EXT_DIM_EXTENSION])
        return df

    def get_es_dimid_count(self):
        return self._es_dim_id_count

    def write(self):
        self.__write_es_targets()
        self.__cleanup()

    def __write_es_targets(self):
        start = datetime.datetime.now()
        self.__write_es_dim_id()
        end = datetime.datetime.now()
        self._logger.info("Write ES_DIM_ID Time: {0} s".format(end - start))

        start = datetime.datetime.now()
        self.__write_es_dim_extension_latest()
        end = datetime.datetime.now()
        self._logger.info(
            "Write ES_DIM_EXTENSION_LATEST Time: {0} s".format(end - start))

        start = datetime.datetime.now()
        self.__write_es_dim_extension_history()
        end = datetime.datetime.now()
        self._logger.info(
            "Write ES_DIM_EXTENSION_HISTORY Time: {0} s".format(end - start))

    def __write_es_dim_id(self):
        query = self.__dd_dict[const.QUERY_GET_ES_DIM_ID_COLUMNS]
        df = SparkConnector.spark_session.sql(query)
        if df.head(1):
            self.__drop_view(self.__dd_dict[const.VIEW_ES_DIM_ID])
            new_dim_id_added_df = df.repartition_and_persist(
                1, "DISK_ONLY")  # repartition count
            new_dim_id_added_df = new_dim_id_added_df.with_column(
                self.__dd_dict["COL_DIMID"], auto_increment)
            new_dim_id_added_df.repartition_write_parquet(
                1,
                path=self._target_spec.hdfs_path.replace(
                    '_EXTENSION_', '_ID_'),
                mode='append')
            self.__refresh_table(self._es_dim_id_table)
            df.unpersist()

    def __drop_view(self, view_name):
        query = const.DROP_VIEW.format(view_name)
        self._logger.info(query)
        SparkConnector.spark_session.sql(query)

    def __refresh_table(self, table_name):
        query = const.REFRESH_TABLE.format(table_name)
        self._logger.info(query)
        SparkConnector.spark_session.sql(query)

    def __write_es_dim_extension_latest(self):
        query = self.__dd_dict[const.QUERY_GET_ES_DIM_EXTEN_LATEST_COL]
        query = query.format(self._es_dim_id_table)
        df = SparkConnector.spark_session.sql(query)
        self._logger.info(
            const.TEMP_LATEST_PATH.format(self._target_spec.hdfs_path))
        self.__new_entries_added = False
        if df.head(1):
            self.__new_entries_added = True
            df.repartition_write_parquet(2,
                                         path=const.TEMP_LATEST_PATH.format(
                                             self._target_spec.hdfs_path),
                                         mode='overwrite')
            self.__mcsk_repair_table(self._target_spec.get_table_name())

    def __mcsk_repair_table(self, table_name):
        query = const.MSCK_REPAIR_TABLE.format(table_name)
        self._logger.info(query)
        SparkConnector.spark_session.sql(query)

    def __write_es_dim_extension_history(self):
        query = self.__dd_dict[const.QUERY_GET_ES_DIM_EXTEN_HISTORY_COL]
        df = SparkConnector.spark_session.sql(query)
        self.__dim_changed = False
        if df.head(1):
            self.__dim_changed = True
            self._logger.info(
                const.HISTORY_PATH.format(self._target_spec.hdfs_path,
                                          str(self.__current_partition)))
            df.repartition_write_parquet(2,
                                         path=const.HISTORY_PATH.format(
                                             self._target_spec.hdfs_path,
                                             str(self.__current_partition)),
                                         mode='append')

    def __cleanup(self):
        self.__move_archive_files()
        self.__move_latest_temp_files()
        self.__remove_ext_dim_extension()

    def __move_archive_files(self):
        prev_partition = self.__get_previous_partition()
        self._logger.info("Previous {0} Current {1}".format(
            prev_partition, self.__current_partition))
        if (not prev_partition) or (prev_partition == str(
                self.__current_partition)) or (not self.__dim_changed):
            self._logger.info("No previous archive partition present")
        else:
            self._logger.info(
                "Previous partitions present {}".format(prev_partition))
            self.__move_previous_partition(prev_partition)
            self.__remove_previous_partition(prev_partition)

    def __get_previous_partition(self):
        partitions_query = const.SHOW_PARTITIONS.format(
            self._target_spec.get_table_name())
        partitions_df = SparkConnector.spark_session.sql(
            partitions_query).collect()

        prev_partition = ''
        for i in partitions_df:
            cpartition = i.asDict()['partition'].split('=')
            if (not cpartition[1].startswith('latest')):
                prev_partition = cpartition[1]
                break
        return prev_partition

    def __move_previous_partition(self, prev_partition):
        self._logger.info(
            "Move Data from previous {0} partition to current {1} partition".
            format(prev_partition, self.__current_partition))
        partiton_to_move = const.MOVE_HISTORY.format(
            self._target_spec.hdfs_path, prev_partition,
            self.__current_partition)
        os.system(partiton_to_move)

    def __remove_previous_partition(self, prev_partition):
        self._logger.info("Remove previous {0} entry".format(prev_partition))
        query = const.DROP_PARTITION.format(self._target_spec.get_table_name(),
                                            prev_partition)
        SparkConnector.spark_session.sql(query)

    def __move_latest_temp_files(self):
        if self.__new_entries_added:
            self._logger.info("Move latest1 to latest partition")
            os.system(
                const.REMOVE_LATEST_FILE.format(self._target_spec.hdfs_path))
            os.system(
                const.REMOVE_LATEST_DIR.format(self._target_spec.hdfs_path))
            os.system(const.MOVE_LATEST.format(self._target_spec.hdfs_path))
            self.__mcsk_repair_table(self._target_spec.get_table_name())

    def __remove_ext_dim_extension(self):
        query = const.TRUNCATE_TABLE.format(
            self._target_spec.hive_schema,
            get_intermediate_table_name(self._target_spec.hdfs_path).upper())
        self._logger.info(query)
        SparkConnector.spark_session.sql(query)
        os.system(const.REMOVE_CSV_FILE)


inargs = process_args()
jobname = get_job_name(inargs.target_path)
LocalDataFrame.expr = functions.expr
LocalDataFrame.lit = functions.lit
LocalSession.BUILDER = SparkSession.builder
LocalDataFrame.DISK_ONLY = StorageLevel.DISK_ONLY
spark_session = SparkConnector.get_spark_session(jobname)
counter = 0


@udf('long')
def auto_increment():
    global counter
    counter = counter + 1
    return counter


sources_specs, target_spec = get_sources_and_target(inargs)
dim_loader = DynamicDimensionLoader(sources_specs, target_spec)
dim_loader.read()
dim_loader.generate_es_ext_dim_extension_view()
counter = dim_loader.get_es_dimid_count()
dim_loader.write()

spark_session.stop()
SparkConnector.spark_session = None

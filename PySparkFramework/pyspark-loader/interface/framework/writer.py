import datetime
import math
import os
import re
import threading

from connector import SparkConnector
from dataframe import LocalDataFrame
from query_generator import QueryGenerator
from reader import ReaderAndViewCreator
from specification import TargetSpecification
from util import OutputfilesCounter, get_static_partition_path


class DataWriter:
    def __init__(self, target_spec: TargetSpecification,
                 reader_objs: [ReaderAndViewCreator],
                 addnl_reader_objs: [ReaderAndViewCreator]):
        self._target_spec = target_spec
        self._reader_objs = reader_objs
        self._addnl_reader_objs = addnl_reader_objs
        self._query_generator = QueryGenerator(self._target_spec,
                                               self._reader_objs,
                                               self._addnl_reader_objs)
        self._multi_load = True if len(
                self._target_spec.yaml_spec) > 1 else False
        self._df = None
        self._logger = SparkConnector.get_logger(__name__)

    def write(self):
        if self._multi_load:
            for yaml_spec in self._target_spec.yaml_spec:
                self._target_spec.hdfs_path = yaml_spec['target_path']
                self._write_table(yaml_spec)
        else:
            self._write_table(self._target_spec.yaml_spec[0])

    def _write_table(self, spec):
        start = datetime.datetime.now()
        self._execute_sql(spec)
        self._cast_columns(spec)
        if TargetSpecification.get_dynamic_partition_col(spec):
            _DynamicPartitionWriter(self._df, spec,
                                    self._target_spec).write()
        else:
            source_spec_list = [
                reader.source_spec for reader in self._reader_objs
                ]
            files = self._get_files_count(source_spec_list, spec)
            static_partitions = \
                TargetSpecification.get_static_partition_col_with_values(
                        spec)
            self._df.write_data(get_static_partition_path(
                    self._target_spec.hdfs_path, static_partitions), files)
        # SparkConnector.spark_session.update_metastore(
        #         self._target_spec.get_table_name())
        self._logger.info("Writing in {} took {} s".format(
                self._target_spec.hdfs_path,
                datetime.datetime.now() - start))

    def _execute_sql(self, spec):
        if len(self._addnl_reader_objs) > 0:
            query = self._query_generator.generate_query(spec)
            self._df = SparkConnector.spark_session.sql(query)
        else:
            self._df = SparkConnector.spark_session.sql(
                    spec['sql']['query'])
            extensions = self._reader_objs[0].source_spec.extensions
            if extensions:
                self._df = _DataFrameJoiner(self._reader_objs, self._df,
                                            self._target_spec,
                                            extensions).join()
        self._logger.info("columns in df after join: {} ".format(
                self._df.columns))

    def _cast_columns(self, spec):
        DATA_TYPE = 1
        static_partitions = \
            TargetSpecification.get_static_partition_col(spec)
        self._logger.info("static_partitions: {}".format(
                static_partitions))
        cols_info = SparkConnector.spark_session.get_data_types(
                self._target_spec.get_table_name(), static_partitions)
        cols = [col_name for col_name, data_type in cols_info]
        self._df = self._df.toDF(cols)
        print(self._df)
        df_col = self._df.columns
        for i in range(len(df_col)):
            self._df = self._df.cast(df_col[i], cols_info[i][DATA_TYPE])
            self._logger.info("casting column {} to {}".format(
                    df_col[i], cols_info[i][DATA_TYPE]))
        self._logger.info("columns and data_types in output df: {}".format(
                self._df.dtypes))

    def _get_files_count(self, source_spec_list, spec):
        output_files_counter = OutputfilesCounter(source_spec_list,
                                                  self._target_spec)
        if 'number_of_output_files' in spec['common'].keys():
            files = spec['common']['number_of_output_files']
        else:
            files = output_files_counter.calculate_files_count()
        self._logger.info(
                "Calculated number of output file {}".format(files))
        return files


class _DynamicPartitionWriter:
    def __init__(self, df, yaml_spec, target_spec):
        self._df = df
        self._hdfs_path = target_spec.hdfs_path
        self._spec = yaml_spec
        self._target_spec = target_spec
        self._logger = SparkConnector.get_logger(__name__)

    def write(self):
        threads = []
        self._create_cache()
        dynamic_partitions = self._get_dynamic_partitions()
        for dynamic_partition_dict in dynamic_partitions:
            thread = threading.Thread(target=self._write_dynamic_partition,
                                      args=[
                                          dynamic_partition_dict,
                                          ])
            threads.append(thread)
            thread.start()
        self._logger.info("Threads Spawned {}".format(len(threads)))
        for thread in threads:
            thread.join()
        self._remove_cache()

    def _create_cache(self):
        start = datetime.datetime.now()
        self._df = self._df.repartition_and_persist(
                5, "DISK_ONLY")  # repartition count
        self._logger.info("Caching for target path {}, took {} s".format(
                self._target_spec.hdfs_path,
                datetime.datetime.now() - start))

    def _get_dynamic_partitions(self):
        partition_list = []
        start = datetime.datetime.now()
        self._logger.info("df count {}".format(self._df.count()))
        partition_df = self._df.groupBy(
                TargetSpecification.get_dynamic_partition_col(self._spec))
        partition_df = partition_df.orderBy('count', False)
        self._logger.info(
                "DataFrame Operations took {}s...".format(
                        datetime.datetime.now() -
                        start))
        for row in partition_df.collect():
            partition_list.append(LocalDataFrame.asDict(row))
        return partition_list

    def _write_dynamic_partition(self, *args):
        start = datetime.datetime.now()
        partition_dict = args[0]
        num_files = 3
        dynamic_hdfs_path = get_static_partition_path(
                self._target_spec.hdfs_path,
                TargetSpecification.get_static_partition_col_with_values(
                    self._spec))
        condition = self._get_filter_condition(partition_dict)
        partitioned_df = self._df.filter(condition)
        for key, value in partition_dict.items():
            if key == 'count':
                num_files += math.ceil(
                        int(value) / int(
                                self._get_dynamic_record_count(self)))
            else:
                dyn_part = "{0}={1}".format(key, value)
                dynamic_hdfs_path = os.path.join(dynamic_hdfs_path,
                                                 dyn_part)
                partitioned_df = partitioned_df.drop(key)
        self._logger.info(
                "No.of files for dynamic partitioned {}".format(num_files))
        partitioned_df.write_data(dynamic_hdfs_path, num_files)
        self._logger.info(
                "Dynamic partitioned writing in target path {}, records "
                "{}, "
                "took {} s".format(dynamic_hdfs_path,
                                   partition_dict["count"],
                                   datetime.datetime.now() - start))

    def _get_filter_condition(self, partition_dict):
        condition = r""
        for key in partition_dict.keys():
            if not key == "count":
                condition += f'''{key}="{partition_dict[key]}" and '''
        return condition[:-4]

    def _remove_cache(self):
        self._logger.info("Un-persisting cache for target path {}".format(
                self._target_spec.hdfs_path))
        self._df.unpersist()

    @staticmethod
    def _get_dynamic_record_count(self):
        conf = dict(SparkConnector.spark_session.get_conf())
        return conf['spark.yarn.appMasterEnv.DYNAMIC_RECORD_COUNT']


class _DataFrameJoiner:
    UNKNOWN_STRING_VALUE = "NA"
    UNKNOWN_NUMERIC_VALUES = -88
    FILTER_KEY = "filter_condition"
    JOIN_CONDITION_KEY = "join_condition"
    TARGET_TABLE_KEY = "target_tables"
    OUT_COLUMN_KEY = "out_columns"

    def __init__(self, readers: [ReaderAndViewCreator], df: LocalDataFrame,
                 target_spec, extensions):
        self._readers = readers
        self._df = df
        self._target_spec = target_spec
        self._extensions = extensions
        self._logger = SparkConnector.get_logger(__name__)

    def join(self):
        column_order = self._df.columns
        data_types = self._df.dtypes
        for reader in self._readers:
            lt_tbl = self._target_spec.get_table_name().replace(
                    "{}.".format(self._target_spec.hive_schema),
                    "").lower()
            rt_tbl = reader.source_spec.get_table_name().replace(
                    "{}.".format(reader.source_spec.hive_schema),
                    "").lower()
            for key in self._extensions.keys():
                if self._is_join_required(rt_tbl, lt_tbl, key):
                    self._apply_join(column_order, data_types, lt_tbl,
                                     reader,
                                     rt_tbl)
        return self._df

    def _is_join_required(self, rt_tbl, lt_tbl, key):
        return rt_tbl == key and lt_tbl in self._extensions[key][
            _DataFrameJoiner.TARGET_TABLE_KEY].keys()

    def _apply_join(self, column_order, data_types, lt_tbl, reader,
                    rt_tbl):
        join_condition = self._process_join_condition(rt_tbl, lt_tbl)
        df = self._select_output_columns(reader, rt_tbl, lt_tbl)
        self._drop_source_table_dimensions(rt_tbl, lt_tbl)
        self._df = self._df.alias(lt_tbl).join(df.alias(rt_tbl),
                                               join_condition,
                                               "left_outer")
        self._cast_columns(rt_tbl, lt_tbl, column_order, data_types)
        self._df = self._modify_column_order(column_order)
        self._logger.info("column order after joining \
        with {} table: {}".format(rt_tbl, self._df.columns))

    def _process_join_condition(self, rt_tbl, lt_tbl):
        join_cols = self._get_table_columns_from_join_condition(rt_tbl,
                                                                lt_tbl)
        condition = \
            self._extensions[rt_tbl][_DataFrameJoiner.TARGET_TABLE_KEY][
                lt_tbl][
                _DataFrameJoiner.JOIN_CONDITION_KEY]
        for col in join_cols:
            unprocessed_col = "{}.{}".format(rt_tbl, col)
            processed_col = "{}.temp_{}".format(rt_tbl, col)
            regex = re.compile(r"{}".format(unprocessed_col),
                               re.IGNORECASE)
            condition = regex.sub(processed_col, condition)
        self._logger.info("final join condition: {}".format(condition))
        return condition

    def _select_output_columns(self, reader, rt_tbl, lt_tbl):
        join_cols = self._get_table_columns_from_join_condition(rt_tbl,
                                                                lt_tbl)
        out_col = \
            self._extensions[rt_tbl][_DataFrameJoiner.TARGET_TABLE_KEY][
                lt_tbl][_DataFrameJoiner.OUT_COLUMN_KEY]
        data_cols = list({col.lower() for col in out_col.keys()})
        df = reader.df
        if _DataFrameJoiner.FILTER_KEY in self._extensions[rt_tbl].keys():
            condition = self._process_filter_condition(rt_tbl)
            self._logger.info(
                    "final filter condition {}".format(condition))
            df = df.filter(condition)
        dimension_df = df.select(list(set(join_cols + data_cols)))
        for name, alias in out_col.items():
            dimension_df = dimension_df.column_alias(name, alias)
        for name in join_cols:
            dimension_df = dimension_df.column_alias(name,
                                                     "temp_{}".format(
                                                             name))
        return dimension_df

    def _drop_source_table_dimensions(self, rt_tbl, lt_tbl):
        column_dict = \
            self._extensions[rt_tbl][_DataFrameJoiner.TARGET_TABLE_KEY][
                lt_tbl][_DataFrameJoiner.OUT_COLUMN_KEY]
        for es_name, ps_name in column_dict.items():
            self._df = self._df.drop(ps_name)

    def _cast_columns(self, rt_tbl, lt_tbl, col_names, data_types):
        out_col = set(
                self._extensions[rt_tbl][
                    _DataFrameJoiner.TARGET_TABLE_KEY][lt_tbl]
                [_DataFrameJoiner.OUT_COLUMN_KEY].values())
        self._logger.info("data types {}".format(data_types))
        for col_name, data_type in zip(col_names, data_types):
            if col_name in out_col:
                if data_type[1].lower() == "string":
                    self._df = self._df.fill_na(
                            _DataFrameJoiner.UNKNOWN_STRING_VALUE,
                            col_name)
                self._df = self._df.fill_na(
                        _DataFrameJoiner.UNKNOWN_NUMERIC_VALUES, col_name)
            self._df = self._df.cast(col_name, data_type[1])

    def _modify_column_order(self, column_order):
        return self._df.select(column_order)

    def _get_table_columns_from_join_condition(self, rt_tbl, lt_tbl):
        cols_list = re.findall(
                r"{}.(\w+)".format(rt_tbl),
                self._extensions[rt_tbl][
                    _DataFrameJoiner.TARGET_TABLE_KEY][lt_tbl]
                [_DataFrameJoiner.JOIN_CONDITION_KEY])
        cols_set = set()
        for col in cols_list:
            name = col.lower()
            if name not in cols_set:
                cols_set.add(name)
        return list(cols_set)

    def _process_filter_condition(self, rt_tbl):
        condition = self._extensions[rt_tbl][
            _DataFrameJoiner.FILTER_KEY].replace(r"{}.".format(rt_tbl), "")
        condition = condition.replace("#LOWER_BOUND",
                                      str(self._target_spec.lower_bound))
        return condition.replace("#UPPER_BOUND",
                                 str(self._target_spec.upper_bound))

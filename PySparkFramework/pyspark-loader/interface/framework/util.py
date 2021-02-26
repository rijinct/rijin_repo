import datetime
import math
import os
import time
from pathlib import Path
from subprocess import Popen, PIPE

from connector import SparkConnector
from reader import ReaderAndViewCreator
from specification import TargetSpecification, SourceSpecification

NO_OF_DAYS_IN_WEEK = 7

ONE_DAY_IN_MILLI_SECONDS = 86400000

ONE_MB_IN_BYTES = 1048576

MIN_FILES = 2


def execute_command(command):
    process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    std_out, std_err = process.communicate()
    return std_out


def source_definition(def_file_path):
    def_file_path = open(def_file_path)
    exec(def_file_path.read().replace("(", "(\"").replace(")", "\")"),
         globals())
    def_file_path.close()


def get_application_host(name):
    statement = "out = {}.split()".format(name)
    exec(statement, globals(), locals())
    return locals()['out']


def get_spec_type(hdfs_path):
    spec_type = hdfs_path.rsplit(r"/ngdb/")[1].split("/")[0]
    if spec_type.lower() == "es":
        spec_type_name = "Entity"
        intermediate_spec_type = 'ext'
    elif spec_type.lower() == "ps":
        spec_type_name = "Perf"
        intermediate_spec_type = 'int'
    else:
        spec_type_name = "Usage"
        intermediate_spec_type = ''
    return spec_type.lower(), spec_type_name, intermediate_spec_type


def get_job_name(hdfs_path):
    spec_type, spec_type_name, intermediate_spec_type = get_spec_type(
            hdfs_path)
    path = Path(hdfs_path)
    return "{}_{}".format(spec_type_name, path.stem)


def get_intermediate_table_name(hdfs_path):
    spec_type, spec_type_name, intermediate_spec_type = get_spec_type(
            hdfs_path)
    path = Path(hdfs_path)
    return "{}_{}".format(intermediate_spec_type, path.stem)


def get_dynamic_dimension_views(hdfs_path):
    views = {}
    spec_type, spec_type_name, intermediate_spec_type = get_spec_type(
            hdfs_path)
    path = Path(hdfs_path)
    views["VIEW_EXT_DIM_EXTENSION"] = "{}_{}_VIEW".format(
            intermediate_spec_type, path.stem).upper()
    views["VIEW_ES_DIM_EXTENSION"] = "{}_{}_VIEW".format(spec_type,
                                                         path.stem).upper()
    views["VIEW_ES_DIM_ID"] = "{}_{}_VIEW".format(spec_type,
                                                  path.stem).upper(

            ).replace(
            'EXTENSION', 'ID')
    return views


def get_day_one_of_last_month(epoch_time):
    date = datetime.datetime.strptime(
            time.strftime('%Y-%m-%d %H:%M:%S',
                          time.localtime(epoch_time / 1000)),
            '%Y-%m-%d %H:%M:%S')
    last_month = date.month - 1 if date.month > 1 else 12
    last_year = date.year - 1 if date.month == 1 else date.year
    new_date = "{year}-{month}-01 00:00:00".format(year=last_year,
                                                   month=last_month)
    new_obj = datetime.datetime.strptime(new_date, '%Y-%m-%d %H:%M:%S')
    return int(new_obj.timestamp()) * 1000


def get_resource_path(relative_path_to_resources, under_test):
    file_util_pathname = Path(__file__)
    util_pathname = file_util_pathname.parent
    framework_pathname = util_pathname.parent
    pysparkloader_pathname = framework_pathname.parent
    project_pathname = pysparkloader_pathname.parent
    if under_test:
        resources_pathname = project_pathname / "pyspark-loader" \
                             / "interface" \
                             / "tests" / "conf"
    else:
        resources_pathname = project_pathname / "conf"
    pathname = resources_pathname / relative_path_to_resources
    return pathname.resolve()


def get_static_partition_path(hdfs_path, static_partitions):
    custom_path = ""
    for partition in static_partitions:
        custom_path = os.path.join(custom_path, partition)
    return os.path.join(hdfs_path, custom_path)


class OutputfilesCounter:
    def __init__(self, source_spec_list: [SourceSpecification],
                 target_spec: TargetSpecification):
        self._source_spec_list = source_spec_list
        self._target_spec = target_spec
        self._one_to_many = True if len(
                self._target_spec.yaml_spec) > 1 else False
        self._static_partitions = self._get_non_dt_static_partitions(
                target_spec.yaml_spec[0])
        self._logger = SparkConnector.get_logger(__name__)

    def calculate_files_count(self):
        table_name = self._target_spec.get_table_name()
        previous_dt = self._get_previous_dt(table_name)
        static_path = self._get_static_path(previous_dt,
                                            self._static_partitions)
        command = "hdfs dfs -du -s {file_path}/{static_path}".format(
                file_path=self._target_spec.hdfs_path,
                static_path=static_path)
        target_file_size = self._get_file_size(command)
        self._logger.info("Target_file_size : {}".format(target_file_size))
        file_size_threshold = self._check_file_size(target_file_size)
        num_files = math.ceil(file_size_threshold /
                              int(self.get_partition_size(self)))
        return max(num_files, MIN_FILES)

    @staticmethod
    def _get_non_dt_static_partitions(spec):
        static_partitions = \
            TargetSpecification.get_static_partition_col_with_values(
                spec)
        return [x for x in static_partitions if 'dt' not
                in x]

    def _get_previous_dt(self, table_name):
        self._logger.info("Table name : {}".format(table_name))
        if any([x in table_name.lower() for x in
                ("15min", "hour", "day")]):
            previous_dt = self._target_spec.lower_bound - \
                          ONE_DAY_IN_MILLI_SECONDS
        elif table_name.lower() in 'week':
            previous_dt = self._target_spec.lower_bound - \
                          ONE_DAY_IN_MILLI_SECONDS * NO_OF_DAYS_IN_WEEK
        else:
            previous_dt = self._get_day_one_of_last_month(
                    self._target_spec.lower_bound)
        return previous_dt

    @staticmethod
    def _get_day_one_of_last_month(target_lb):
        date = datetime.datetime.strptime(
                time.strftime('%Y-%m-%d %H:%M:%S',
                              time.localtime(target_lb / 1000)),
                '%Y-%m-%d %H:%M:%S')
        last_month = date.month - 1 if date.month > 1 else 12
        last_year = date.year - 1 if date.month == 1 else date.year
        new_date = "{year}/{month}/01 00:00:00".format(year=last_year,
                                                       month=last_month)
        new_obj = datetime.datetime.strptime(new_date, '%Y/%m/%d %H:%M:%S')
        return int(new_obj.timestamp()) * 1000

    def _check_file_size(self, file_size):
        source_file_size = 0
        if file_size < ONE_MB_IN_BYTES:
            for source_spec in self._source_spec_list:
                source_file_size = self._get_source_file_size(
                        source_file_size, source_spec)
        else:
            source_file_size = file_size
        self._logger.info(
                "Source_file_size final : {}".format(source_file_size))
        return source_file_size

    def _get_source_file_size(self, source_file_size, source_spec):
        reader = ReaderAndViewCreator(source_spec, self._one_to_many)
        command = "hdfs dfs -du -s {file_path}/dt={dt}/tz={tz}".format(
            file_path=reader.source_spec.hdfs_path,
            dt=reader.source_spec.lower_bound,
            tz=reader.source_spec.timezone)
        source_file_size_temp = self._get_file_size(command)
        source_file_size = source_file_size + source_file_size_temp
        return source_file_size

    @staticmethod
    def _get_file_size(command):
        file_size = execute_command(command).split()
        file_size = int(file_size[0].decode('UTF-8')) if file_size else 0
        return file_size

    @staticmethod
    def get_partition_size(self):
        conf = dict(SparkConnector.spark_session.get_conf())
        return conf['spark.yarn.appMasterEnv.STATIC_PARTITION_SIZE']

    @staticmethod
    def _get_static_path(dt, non_dt_static_partitions):
        custom_path = "dt={}".format(dt)
        for partition in non_dt_static_partitions:
            custom_path = os.path.join(custom_path, partition)
        return custom_path

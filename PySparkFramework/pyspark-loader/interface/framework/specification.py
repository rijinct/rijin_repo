import os
import re
from pathlib import Path
from subprocess import Popen, PIPE

import yaml


class Specification:
    def __init__(self,
                 hdfs_path,
                 hive_schema,
                 lower_bound=0,
                 upper_bound=0,
                 timezone='Default',
                 days=0):
        self._hdfs_path = hdfs_path
        self._hive_schema = hive_schema
        self._spec_type = self.hdfs_path.rsplit("/ngdb/")[1].split('/')[0]
        self._lower_bound = lower_bound - (days * 86400000)
        self._upper_bound = upper_bound
        self._timezone = timezone
        self._days = days

    @property
    def hdfs_path(self):
        return self._hdfs_path

    @hdfs_path.setter
    def hdfs_path(self, hdfs_path):
        self._hdfs_path = hdfs_path

    @property
    def hive_schema(self):
        return self._hive_schema

    @property
    def spec_type(self):
        return self._spec_type

    @property
    def lower_bound(self):
        return self._lower_bound

    @property
    def upper_bound(self):
        return self._upper_bound

    @property
    def timezone(self):
        return self._timezone

    @property
    def days(self):
        return self._days

    @staticmethod
    def set_yaml_spec(pathname):
        if os.path.isfile(pathname):
            with open(pathname) as file:
                return yaml.full_load(file)


class SourceSpecification(Specification):
    def __init__(self,
                 hdfs_path,
                 hive_schema,
                 lower_bound=0,
                 upper_bound=0,
                 timezone='Default',
                 extensions='',
                 days=0):
        super().__init__(hdfs_path, hive_schema, lower_bound, upper_bound,
                         timezone, days)
        self._extensions = extensions

    @property
    def extensions(self):
        return self._extensions

    @staticmethod
    def represents_int(a):
        try:
            int(a)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_empty_path(dir_path):
        command = 'hdfs dfs -ls ' + dir_path
        process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        std_out, err = process.communicate()
        return_code = process.returncode
        if return_code != 0:
            raise RuntimeError(err)
        return len(std_out) == 0

    # get_all_dates
    def _get_all_dts(self):
        command = 'hdfs dfs -ls -r {}'.format(self.hdfs_path)
        process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, err = process.communicate()
        return_code = process.returncode
        if return_code != 0:
            raise RuntimeError(err)
        dts = re.findall(r"dt=(\d+)", str(output))
        return dts

    def create_static_path(self, int_dt):
        return "{}/dt={}/tz={}/".format(self.hdfs_path, int_dt,
                                        self.timezone.replace(' ', r'\ '))

    def get_table_name(self):
        return "{0}.{1}_{2}".format(self._hive_schema, self._spec_type,
                                    Path(self.hdfs_path).name.lower())

    # Condition Removed: "not SourceSpecification.is_empty_path(part_info)"
    # Reason: hive/spark creates an empty parquet file inside static
    # parition
    # even when data is not present
    def _get_ps_partition_paths(self):
        static_paths = []
        dts = self._get_all_dts()
        for dt in dts:
            if SourceSpecification.represents_int(dt):
                int_dt = int(dt)
                path = self.create_static_path(int_dt)
                if self.lower_bound <= int_dt < self.upper_bound and not \
                        SourceSpecification.is_empty_path(path):
                    static_paths.append(path)
                elif int_dt < self.lower_bound:
                    break
        return static_paths

    def get_dimension_tables(self, table_name):
        es_tables = []
        for es_table in self.extensions.keys():
            if table_name in self.extensions[es_table][
                    'target_tables'].keys():
                es_tables.append(es_table)
        return es_tables

    def get_locs(self, addnl_path=''):
        locs = []
        path = "{0}{1}".format(self.hdfs_path, addnl_path)
        if self.spec_type.upper() == "ES" and \
                not SourceSpecification.is_empty_path(path):
            locs.append(path)
        elif self.spec_type.upper() == "PS" or self.spec_type.upper() == \
                "US":
            locs = self._get_ps_partition_paths()
        return locs


class TargetSpecification(Specification):
    def __init__(self,
                 hdfs_path,
                 hive_schema,
                 lower_bound=0,
                 upper_bound=0,
                 timezone='Default',
                 days=0,
                 yaml_files=[],
                 table_name=""):
        super().__init__(hdfs_path, hive_schema, lower_bound, upper_bound,
                         timezone, days)
        self._yaml_spec = [
            Specification.set_yaml_spec(file) for file in yaml_files
            ]
        self._table_name = table_name

    @property
    def yaml_spec(self):
        return self._yaml_spec

    @staticmethod
    def get_dynamic_partition_col(spec):
        partition = spec.get('partition')
        return [x for x in partition['columns_with_values'] if '=' not
                in x]

    @staticmethod
    def get_static_partition_col(spec):
        partition = spec.get('partition')
        return [x.split('=')[0] for x in partition['columns_with_values']
                if '=' in x]

    @staticmethod
    def get_static_partition_col_with_values(spec):
        partition = spec.get('partition')
        return [x for x in partition['columns_with_values'] if '=' in x]

    def get_table_schema(self, spark_session):
        return spark_session.table(self.get_table_name()).schema

    def get_table_name(self):
        if self._table_name:
            table_name = self._table_name
        else:
            table_name = "{}_{}".format(self._spec_type,
                                        Path(self.hdfs_path).name.lower())
        return "{0}.{1}".format(self._hive_schema, table_name)

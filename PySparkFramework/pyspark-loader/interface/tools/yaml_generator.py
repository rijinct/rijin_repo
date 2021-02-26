import re

import sqlparse

import tools_util as util
from constants import FileConstants as FC
from constants import YamlConstants as YC
from query_parser import QueryParser
from subpartition_retriever import SubpartitionRetriever


class YamlGenerator:
    def __init__(self, args, yaml_file):
        self._input_yaml = yaml_file
        conf_files = True if yaml_file else False
        sql_query = self._get_sql_query(conf_files, args.query_file)
        query = self._format_query(sql_query)
        self._query = self._parsed_query(query)
        self._target_table_name = ""
        self._args = args

    def generate_yaml(self, job_name, sql_path):
        output = YamlGenerator._create_base_yaml_struct()
        output[YC.COMMON.value][YC.DESCRIPTION.value] = job_name
        self._get_subpartitions_tag(output)
        output[YC.SQL.value][YC.QUERY.value] = QueryParser(
                self._query).query
        output[YC.TARGET_TABLE_NAME.value] = self._target_table_name
        return output

    def get_updated_yaml_with_mandatory_fields(self, job_name,
                                               query_file):
        input_yaml = self._input_yaml
        main_fields = [YC.COMMON.value, YC.SQL.value]
        for field in main_fields:
            if not input_yaml.get(field):
                input_yaml[field] = {}
        if not input_yaml[YC.COMMON.value].get(YC.DESCRIPTION.value, ''):
            input_yaml[YC.COMMON.value][YC.DESCRIPTION.value] = job_name
        if not input_yaml.get(YC.SUBPARTITION_COLUMNS.value, ''):
            self._get_subpartitions_tag(
                    input_yaml)
        if not input_yaml.get(YC.SQL.value).get(YC.QUERY.value, ''):
            self._get_sql_from_file(input_yaml, query_file)
        input_yaml[YC.TARGET_TABLE_NAME.value] = self._target_table_name
        return input_yaml

    def _get_subpartitions_tag(self, input_yaml):
        input_yaml[
            YC.SUBPARTITION.value] = {}
        subpartition_list = self._get_subpartition_columns(
                self._query)
        if not subpartition_list:
            subpartition_list = self._get_default_list()
        input_yaml[
            YC.SUBPARTITION.value][YC.COLUMNS_WITH_VALUES.value] = \
            self._get_subpartition_list_with_values(subpartition_list)
        input_yaml[
            YC.SUBPARTITION.value][YC.COLUMNS.value] = \
            [x.split('=')[0].strip() for x in subpartition_list]

    def _get_sql_from_file(self, input_yaml, query_file):
        if query_file:
            qp = QueryParser(self._query)
            input_yaml[YC.SQL.value][
                YC.QUERY.value] = YamlGenerator._get_agg_query(
                    qp.query)
        else:
            raise FileNotFoundError("Missing argument!! -q query_file")

    @staticmethod
    def _format_query(query):
        query = YamlGenerator._replace_string("(decode\(|decode \()",
                                                "custom_decode(",query)
        query = YamlGenerator._replace_string("LEAST\(","custom_least(",query)
        query = re.sub('\s+',' ',query)
        query = util.get_new_lines_replaced_query(query)
        return query.replace("''", "'")

    @staticmethod
    def _replace_string(string_present,
                        string_to_be_replaced,
                        input_string):
        replacement_string = re.compile(
            string_present,
            re.IGNORECASE)
        input_string = replacement_string.sub(
            string_to_be_replaced, input_string)
        return input_string
        
    @staticmethod
    def _parsed_query(query):
        return sqlparse.parse(query)[0]

    def _get_sql_query(self, conf_file, sql_path):
        query = ""
        if not conf_file or not YC.SQL.value in self._input_yaml.keys():
            query = util.read_sql_file(sql_path)
        else:
            if self._input_yaml.get(YC.SQL.value).get(YC.QUERY.value, ''):
                query = self._input_yaml.get(YC.SQL.value).get(
                        YC.QUERY.value)
        return query

    @staticmethod
    def _create_base_yaml_struct():
        base_yaml = {}
        sample_yaml = util.read_yaml(FC.TEMPLATE_YAML.value)
        for k, v in sample_yaml.items():
            base_yaml[k] = {}
            if isinstance(v, dict):
                for key in v.keys():
                    base_yaml[k][key] = ""
            elif isinstance(v, list):
                base_yaml[k] = []
            else:
                base_yaml[k] = ""
        return base_yaml

    def _get_subpartition_columns(self, query):
        pr = SubpartitionRetriever()
        partitions_list = pr.get_subpartition(query)
        self._target_table_name = pr.target_table_name
        return partitions_list

    @staticmethod
    def _get_agg_query(input_sql):
        return input_sql

    def _get_default_list(self):
        default_list = ["dt={}".format(self._args.lower_bound),
                        "tz={}".format(self._args.timezone)]
        return default_list

    def _get_subpartition_list_with_values(self, subpartition_list):
        dt = "dt={}".format(self._args.lower_bound)
        tz = "tz={}".format(self._args.timezone)
        for partition in subpartition_list:
            if "dt" in partition:
                YamlGenerator._update_subpartition_list(partition,
                                                        subpartition_list, dt)
            elif "tz" in partition:
                YamlGenerator._update_subpartition_list(partition,
                                                        subpartition_list, tz)
        return subpartition_list

    @staticmethod
    def _update_subpartition_list(partition, subpartition_list,
                                  new_partition):
        index = subpartition_list.index(partition)
        subpartition_list.remove(partition)
        subpartition_list.insert(index, new_partition)

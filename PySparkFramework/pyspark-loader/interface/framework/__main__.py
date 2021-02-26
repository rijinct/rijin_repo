import argparse as ap
import re
from pathlib import Path

import pyspark.sql.functions as functions
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from aggregator import Aggregator
from connection import LocalSession
from connector import SparkConnector
from dataframe import LocalDataFrame
from specification import SourceSpecification
from specification import Specification
from specification import TargetSpecification
from util import get_job_name

TARGET_TABLE_RELATIONSHIP_EXISTS = True


def process_args():
    p = ap.ArgumentParser(description="Usage: __main__.py")
    p.add_argument("-s",
                   "--source_paths",
                   required=True,
                   help="source location(s)")
    p.add_argument("-t",
                   "--target_path",
                   required=True,
                   help="target location")
    p.add_argument("-l", "--lower_bound", required=True, help="lower bound")
    p.add_argument("-u", "--upper_bound", required=True, help="upper bound")
    p.add_argument("-tz",
                   "--timezone",
                   nargs='+',
                   help="time zone",
                   default=['Default'])
    p.add_argument("-d", "--days", help="days to consider", default=0)
    p.add_argument("-c", "--conf_files", help="YAML configuration location")
    args = p.parse_args()
    args.conf_files = ",".join(
        Path(f).name for f in args.conf_files.split(','))
    args.timezone = " ".join(args.timezone)
    return args


def get_paths_of_dynamic_extensions(extensions, table_name):
    hdfs_paths = []
    if extensions:
        for es_table in extensions:
            if 'target_tables' in extensions[
                    es_table] and table_name in extensions[es_table][
                        'target_tables'].keys():
                hdfs_paths.append(extensions[es_table]['hdfs_path'])
        return hdfs_paths
    return ""


def get_hive_schema():
    conf = dict(SparkConnector.spark_session.get_conf())
    return conf['spark.yarn.appMasterEnv.HIVESCHEMA']


def get_sources_and_target(inargs):
    global TARGET_TABLE_RELATIONSHIP_EXISTS
    hive_schema = get_hive_schema()
    source_specs = []
    addnl_source_specs = []
    source_paths = inargs.source_paths.split(',')
    conf_file_dict = Specification.set_yaml_spec(
            inargs.conf_files.split(',')[0])
    target_table_name = conf_file_dict['target_table_name'] if \
        'target_table_name' in conf_file_dict.keys() else ""
    target_spec = TargetSpecification(inargs.target_path, hive_schema,
                                      int(inargs.lower_bound),
                                      int(inargs.upper_bound), inargs.timezone,
                                      int(inargs.days),
                                      inargs.conf_files.split(','),
                                      target_table_name)
    extensions = conf_file_dict[
        'dimension_column_mapping'] if 'dimension_column_mapping' in \
                                       conf_file_dict.keys() else ''
    addnl_source_specs = get_additional_sources(
        target_spec.get_table_name().replace('{}.'.format(hive_schema), ''),
        extensions, conf_file_dict)
    if len(addnl_source_specs) == 0:
        TARGET_TABLE_RELATIONSHIP_EXISTS = False
        source_paths += get_paths_of_dynamic_extensions(
            extensions,
            target_spec.get_table_name().replace(
                "{}.".format(target_spec.hive_schema), "").lower())

    for source_path in source_paths:
        spec = SourceSpecification(source_path, hive_schema,
                                   int(inargs.lower_bound),
                                   int(inargs.upper_bound),
                                   inargs.timezone,
                                   extensions, int(inargs.days))
        source_specs.append(spec)
        if not TARGET_TABLE_RELATIONSHIP_EXISTS:
            asource_specs = get_additional_sources(
                spec.get_table_name().replace('{}.'.format(hive_schema), ''),
                extensions, conf_file_dict)
            addnl_source_specs = addnl_source_specs + asource_specs
    return source_specs, target_spec, addnl_source_specs


def is_additional_source_column_present(es_table, extensions, conf_file_dict):
    query = conf_file_dict['sql']['query']
    is_present = False
    for jointype in extensions[es_table]:
        if jointype.startswith(
                'jointype_'
        ) and 'columns_needed_in_query' in extensions[es_table][jointype]:
            for col in extensions[es_table][jointype][
                    'columns_needed_in_query'].keys():
                string_to_search = r"\b{}\b".format(col)
                if re.search(string_to_search, query, re.IGNORECASE):  # noqa: 605
                    is_present = True
                    break
    return is_present


def get_additional_sources(table_name, extensions, conf_file_dict):
    hive_schema = get_hive_schema()
    addnl_source_specs = []
    if 'table_relationship' in conf_file_dict and table_name in conf_file_dict[
            'table_relationship'] and extensions:
        for es_table in conf_file_dict['table_relationship'][table_name]:
            if (es_table in extensions and is_additional_source_column_present(
                    es_table, extensions, conf_file_dict)):
                addnl_source_specs.append(
                    SourceSpecification(extensions[es_table]['hdfs_path'],
                                        hive_schema))
    return addnl_source_specs


def main():
    inargs = process_args()
    LocalDataFrame.expr = functions.expr
    LocalDataFrame.lit = functions.lit
    LocalSession.BUILDER = SparkSession.builder
    LocalDataFrame.DISK_ONLY = StorageLevel.DISK_ONLY
    SparkConnector(get_job_name(inargs.target_path))
    SparkConnector.add_files(inargs.conf_files.split(','))
    SparkConnector.spark_session.set_current_database(get_hive_schema())
    sources_specs, target_spec, addnl_source_specs = get_sources_and_target(
        inargs)
    aggregator = Aggregator(sources_specs, target_spec, addnl_source_specs)
    aggregator.start_loading()


if __name__ == '__main__':
    main()

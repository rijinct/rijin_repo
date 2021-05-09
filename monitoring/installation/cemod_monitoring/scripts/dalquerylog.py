#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author: shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose:To capture the Report from dal log
#
# Date:    19-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

from datetime import datetime, timedelta
import commands, os, subprocess
import json
import csv
import re
import sys
import copy
sys.path.append("../utils/")
from postgres_connection import PostgresConnection
from loggerUtil import loggerUtil
from date_time_util import DateTimeUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).\
    replace("(","(\"").replace(")","\")")

### Variables assignment ###
dal_server_log_path = "/var/local/dal-server/logs"
dal_server_output_log_path = "/var/local/monitoring/output/dalServer"
utility_type = 'dalQueryAnalysis'
KEY_VALUE_SEPARATOR = ' : '
QUERY = 'Query'
APPLICATION_INFO = 'Application info'
MAPPER = 'Mapper'
REDUCER = 'Reducer'
EXECUTOR = 'Executor'
UNIQUE_ID_REGEX = '(DALQuery_[0-9]*)'
SPARK_QUERY_KEY = "sparkQueries"
HIVE_QUERY_KEY = "hiveQueryStats"
date_time_to_process = datetime.now().replace(microsecond=0, second=0,
                                              minute=0)-timedelta(hours=1)
file_name_suffix = '{0}-{1}-{2}_HH{3}'.format(date_time_to_process.strftime('%d'),
                        date_time_to_process.strftime('%m'),
                        date_time_to_process.strftime('%Y'),
                        date_time_to_process.strftime('%H'))
log_file_name = 'dal_query_analysis_{}'.format(file_name_suffix.replace(' ', '_'))
LOGGER = loggerUtil.__call__().get_logger(log_file_name)


def update_query_dict(query_dict, query):
    previous_query_object = copy.deepcopy(query)
    if previous_query_object:
        query_dict.append(previous_query_object)



def update_query_object_and_get_key(query, line):
    line_array = line.split(' : ', 1)
    key = line_array[0].strip().capitalize()
    value = line_array[1].lstrip()
    query[key] = value
    return key

def process_each_line_for_query_details(lines, query_dict):
    query = dict({})
    previous_key = None
    for line in lines.split('\n'):
        if QUERY == line.split(' : ')[0].capitalize().strip():
            update_query_dict(query_dict, query)
            query = dict({})
            previous_key = update_query_object_and_get_key(query, line)
        elif KEY_VALUE_SEPARATOR in line:
            previous_key = update_query_object_and_get_key(query, line)
        else:
            query[previous_key] += line
    
    update_query_dict(query_dict, query)


def get_query_info_dictionary():
    LOGGER.info('Getting information of all the dal queries')
    sed_cmd_to_get_information_of_all_queries = 'sed -n "/QUERY                   :/,/EXECUTION ENGINE        :/p" {}/dal-server.log'.format(
        dal_server_log_path)
    lines = subprocess.Popen("""ssh {} '{}'""".
                             format(cemod_dalserver_active_fip_host,
                                    sed_cmd_to_get_information_of_all_queries),
                             stdout=subprocess.PIPE, shell=True).communicate()[0]
    query_dict = []
    if lines:
        process_each_line_for_query_details(lines, query_dict)
    return query_dict


def update_required_queries(date_time_to_process,
                            required_queries_for_given_time, query_info_dict):
    next_hour_date_time = date_time_to_process + timedelta(hours=1)
    LOGGER.info('Getting queries for the time interval {} - {}'.format(str(
        date_time_to_process), str(next_hour_date_time)))
    for query_info in query_info_dict:
        query_date_time = DateTimeUtil.parse_date(query_info['End time'])
        if date_time_to_process <= query_date_time < next_hour_date_time:
            required_queries_for_given_time.append(query_info)


def get_all_queries_for_given_time(date_time_to_process):
    required_queries_for_given_time = []
    query_info_dict = get_query_info_dictionary()
    LOGGER.info('Number of distinct dal queries found are {}'.format(len(
        query_info_dict)))
    if query_info_dict:
        update_required_queries(date_time_to_process,
                                required_queries_for_given_time,
                                query_info_dict)
    LOGGER.info('Number of distinct dal queries found for the input time {} are {}'.
                format(str(date_time_to_process),
                       len(required_queries_for_given_time)))
    return required_queries_for_given_time

def get_query_stats(hm_type, date_time_to_process):
    next_hour_date_time = date_time_to_process + timedelta(hours=1)
    postgres_connection = PostgresConnection.get_instance()
    sql = """select hm_json from sairepo.hm_stats where hm_type='{}' and hm_date between '{}' and '{}'""".\
       format(hm_type, date_time_to_process, next_hour_date_time)
    return postgres_connection.fetch_records(sql)

def get_application_json_ascii_encode(hive_query, hive_query_info):
    application = {}
    for key in hive_query[APPLICATION_INFO][hive_query_info].keys():
        application[str(key)] = str(hive_query[APPLICATION_INFO][hive_query_info][key])
    return application


def get_query(record_list, dal_query_id):
    query_from_db = None
    for query in record_list:
        is_dal_query = re.search(UNIQUE_ID_REGEX, query[QUERY])
        query_id = re.search(UNIQUE_ID_REGEX, query[QUERY]).group()
        if is_dal_query and dal_query_id == query_id:
            query_from_db = query
            break;
    return query_from_db


def update_additional_info_for_query(query_info, application_info, mapper_count,
                                     reducer_count, executor_count):
    query_info[APPLICATION_INFO] = application_info
    query_info[MAPPER] = mapper_count
    query_info[REDUCER] = reducer_count
    query_info[EXECUTOR] = executor_count


def get_application_info_for_hive_query(hive_query):
    application_info = {}
    for hive_query_info in hive_query[APPLICATION_INFO]:
        application_info[str(hive_query_info)] = get_application_json_ascii_encode(
            hive_query, hive_query_info)
    
    return application_info

def update_dal_query_info_for_hive(query_info, dal_query_id, record_list):
    hive_query = get_query(record_list, dal_query_id)
    if hive_query is not None:
        application_info = get_application_info_for_hive_query(hive_query)
        update_additional_info_for_query(query_info, application_info, 
                                         hive_query[MAPPER], hive_query[REDUCER], 0)
    else:
        update_additional_info_for_query(query_info, {}, 0, 0, 0)

def update_dal_query_info_for_spark(query_info, dal_query_id, record_list):
    spark_query = get_query(record_list, dal_query_id)
    if spark_query is not None:
        update_additional_info_for_query(query_info, spark_query['Application Id'], 
                                         0, 0, spark_query[EXECUTOR])
    else:
        update_additional_info_for_query(query_info, {}, 0, 0, 0)


def get_record_list(hive_query_stats):
    record_list = []
    if hive_query_stats:
        record_list = list(hive_query_stats[0])[0]
    return record_list


def process_hive_dal_query(hive_query_stats, query_info):
    dal_query_id = re.search(UNIQUE_ID_REGEX, query_info[QUERY]).group()
    record_list = get_record_list(hive_query_stats)
    update_dal_query_info_for_hive(query_info, dal_query_id, record_list)


def process_spark_dal_query(spark_query_stats, query_info):
    dal_query_id = re.search(UNIQUE_ID_REGEX, query_info[QUERY]).group()
    record_list = get_record_list(spark_query_stats)
    update_dal_query_info_for_spark(query_info, dal_query_id, record_list)


def update_query_info_for_hive_and_spark(hive_query_stats, spark_query_stats, query_info):
    if query_info['Execution engine'] == 'HIVESERVER':
        LOGGER.debug('Updating additional details for hive query')
        process_hive_dal_query(hive_query_stats, query_info)
    else:
        LOGGER.debug('Updating additional details for spark query')
        process_spark_dal_query(spark_query_stats, query_info)


def get_time_taken_for_query(query_info):
    start_time = DateTimeUtil.parse_date(query_info['Start time'])
    end_time = DateTimeUtil.parse_date(query_info['End time'])
    return DateTimeUtil.get_difference_in_minutes(start_time, end_time)


def get_hive_query_stats(date_time_to_process):
    LOGGER.info('Getting hive query information for time {}'.format(date_time_to_process))
    return get_query_stats(HIVE_QUERY_KEY, date_time_to_process)


def get_spark_query_stats(date_time_to_process):
    LOGGER.info('Getting spark query information for time {}'.format(date_time_to_process))
    return get_query_stats(SPARK_QUERY_KEY, date_time_to_process)


def update_required_information_for_queries(queries_information, date_time_to_process):
    LOGGER.info('Updating additional information for each query')
    hive_query_stats = get_hive_query_stats(date_time_to_process)
    spark_query_stats = get_spark_query_stats(date_time_to_process)
    for query_info in queries_information:
        query_info['Time taken'] = str(get_time_taken_for_query(query_info))
        update_query_info_for_hive_and_spark(hive_query_stats, spark_query_stats, query_info)


def write_csv(query_information, csv_file_path):
    LOGGER.info('Writing dal query information to csv in path {}'.format(csv_file_path))
    keys = query_information[0].keys()
    with open(csv_file_path, 'w') as output_csv:
        dict_writer = csv.DictWriter(output_csv, keys)
        dict_writer.writeheader()
        dict_writer.writerows(query_information)


def push_content_to_db(queries_information):
    LOGGER.info('Pushing dal query information inside db')
    postgres_connection = PostgresConnection.get_instance()
    for query_info in queries_information:
        sql_query = 'insert into sairepo.hm_stats values (%s, %s, %s)'
        end_time = DateTimeUtil.to_date_string(
            DateTimeUtil.parse_date(query_info['End time']),
            '%Y-%m-%d %H:%M:%S')
        record = (end_time, utility_type, json.dumps(query_info))
        postgres_connection.execute_query_with_record(sql_query, record)


def main():
    LOGGER.info('Logs available at /var/local/monitoring/log/{}.log'.format(log_file_name))
    csv_file_path = '{0}/dalReportlog_{1}.csv'.format(dal_server_output_log_path, file_name_suffix)
    queries_information = get_all_queries_for_given_time(date_time_to_process)
    if queries_information:
        update_required_information_for_queries(queries_information, date_time_to_process)
        write_csv(queries_information, csv_file_path)
        push_content_to_db(queries_information)


if __name__ == "__main__":
    main()
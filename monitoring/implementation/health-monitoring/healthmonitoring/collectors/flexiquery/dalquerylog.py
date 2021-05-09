from datetime import datetime, timedelta
import subprocess
import re
import copy

from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.connection import ConnectionFactory
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)

dal_server_log_path = "/var/local/dal-server/logs"
KEY_VALUE_SEPARATOR = ' : '
QUERY = 'Query'
APPLICATION_INFO = 'Application info'
MAPPER = 'Mapper'
REDUCER = 'Reducer'
EXECUTOR = 'Executor'
UNIQUE_ID_REGEX = '(DALQuery_[0-9]*)'
SPARK_QUERY_KEY = "sparkQueries"
HIVE_QUERY_KEY = "hiveQueryStats"
date_time_to_process = datetime.now().replace(
    microsecond=0, second=0, minute=0) - timedelta(hours=1)


def main():
    queries_information = get_all_queries_for_given_time(date_time_to_process)
    if queries_information:
        update_required_information_for_queries(queries_information,
                                                date_time_to_process)
    print(queries_information)


def get_all_queries_for_given_time(date_time_to_process):
    required_queries_for_given_time = []
    query_info_dict = get_query_info_dictionary()
    logger.info('Number of distinct dal queries found are {}'.format(
        len(query_info_dict)))
    if query_info_dict:
        update_required_queries(date_time_to_process,
                                required_queries_for_given_time,
                                query_info_dict)
    logger.info(
        'Number of distinct dal queries found for the input time {} are {}'.
        format(str(date_time_to_process),
               len(required_queries_for_given_time)))
    return required_queries_for_given_time


def get_query_info_dictionary():
    logger.info('Getting information of all the dal queries')
    sed_cmd_to_get_information_of_all_queries = (
        'sed -n "/QUERY                   :/,/EXECUTION ENGINE        :/p" {}/dal-server.log'.  # noqa: 501
        format(dal_server_log_path))
    host_ip = SpecificationUtil.get_host('dalserver').address
    lines = subprocess.Popen("""ssh {} '{}'""".format(
        host_ip, sed_cmd_to_get_information_of_all_queries),
                             stdout=subprocess.PIPE,
                             shell=True).communicate()[0].decode('utf-8')
    query_dict = []
    if lines:
        process_each_line_for_query_details(lines, query_dict)
    return query_dict


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


def update_required_queries(date_time_to_process,
                            required_queries_for_given_time, query_info_dict):
    next_hour_date_time = date_time_to_process + timedelta(hours=1)
    logger.info('Getting queries for the time interval {} - {}'.format(
        str(date_time_to_process), str(next_hour_date_time)))
    for query_info in query_info_dict:
        query_date_time = DateTimeUtil.parse_date(query_info['End time'])
        if date_time_to_process <= query_date_time < next_hour_date_time:
            required_queries_for_given_time.append(query_info)


def update_required_information_for_queries(queries_information,
                                            date_time_to_process):
    logger.info('Updating additional information for each query')
    hive_query_stats = get_hive_query_stats(date_time_to_process)
    spark_query_stats = get_spark_query_stats(date_time_to_process)
    for query_info in queries_information:
        query_info['Time taken'] = str(get_time_taken_for_query(query_info))
        update_query_info_for_hive_and_spark(hive_query_stats,
                                             spark_query_stats, query_info)


def get_hive_query_stats(date_time_to_process):
    logger.info('Getting hive query information for time {}'.format(
        date_time_to_process))
    return get_query_stats(HIVE_QUERY_KEY, date_time_to_process)


def get_spark_query_stats(date_time_to_process):
    logger.info('Getting spark query information for time {}'.format(
        date_time_to_process))
    return get_query_stats(SPARK_QUERY_KEY, date_time_to_process)


def get_query_stats(hm_type, date_time_to_process):
    next_hour_date_time = date_time_to_process + timedelta(hours=1)
    postgres_connection = ConnectionFactory.get_connection(DBType.POSTGRES_SDK)
    sql = (
        """select hm_json from sairepo.hm_stats where hm_type='{}' and hm_date between '{}' and '{}'""".  # noqa: 501
        format(hm_type, date_time_to_process, next_hour_date_time))
    return postgres_connection.fetch_records(sql)


def get_time_taken_for_query(query_info):
    start_time = DateTimeUtil.parse_date(query_info['Start time'])
    end_time = DateTimeUtil.parse_date(query_info['End time'])
    return DateTimeUtil.get_difference_in_minutes(start_time, end_time)


def update_query_info_for_hive_and_spark(hive_query_stats, spark_query_stats,
                                         query_info):
    if query_info['Execution engine'] == 'HIVESERVER':
        logger.debug('Updating additional details for hive query')
        process_hive_dal_query(hive_query_stats, query_info)
    else:
        logger.debug('Updating additional details for spark query')
        process_spark_dal_query(spark_query_stats, query_info)


def process_hive_dal_query(hive_query_stats, query_info):
    dal_query_id = re.search(UNIQUE_ID_REGEX, query_info[QUERY]).group()
    record_list = get_record_list(hive_query_stats)
    update_dal_query_info_for_hive(query_info, dal_query_id, record_list)


def get_record_list(hive_query_stats):
    record_list = []
    if hive_query_stats:
        record_list = list(hive_query_stats[0])[0]
    return record_list


def process_spark_dal_query(spark_query_stats, query_info):
    dal_query_id = re.search(UNIQUE_ID_REGEX, query_info[QUERY]).group()
    record_list = get_record_list(spark_query_stats)
    update_dal_query_info_for_spark(query_info, dal_query_id, record_list)


def update_dal_query_info_for_hive(query_info, dal_query_id, record_list):
    hive_query = get_query(record_list, dal_query_id)
    if hive_query is not None:
        application_info = get_application_info_for_hive_query(hive_query)
        update_additional_info_for_query(query_info, application_info,
                                         hive_query[MAPPER],
                                         hive_query[REDUCER], 0)
    else:
        update_additional_info_for_query(query_info, {}, 0, 0, 0)


def get_application_info_for_hive_query(hive_query):
    application_info = {}
    for hive_query_info in hive_query[APPLICATION_INFO]:
        application_info[str(hive_query_info)] = \
            get_application_json_ascii_encode(hive_query, hive_query_info)
    return application_info


def get_application_json_ascii_encode(hive_query, hive_query_info):
    application = {}
    for key in list(hive_query[APPLICATION_INFO][hive_query_info].keys()):
        application[str(key)] = str(
            hive_query[APPLICATION_INFO][hive_query_info][key])
    return application


def update_dal_query_info_for_spark(query_info, dal_query_id, record_list):
    spark_query = get_query(record_list, dal_query_id)
    if spark_query is not None:
        update_additional_info_for_query(query_info,
                                         spark_query['Application Id'], 0, 0,
                                         spark_query[EXECUTOR])
    else:
        update_additional_info_for_query(query_info, {}, 0, 0, 0)


def update_additional_info_for_query(query_info, application_info,
                                     mapper_count, reducer_count,
                                     executor_count):
    query_info[APPLICATION_INFO] = application_info
    query_info[MAPPER] = mapper_count
    query_info[REDUCER] = reducer_count
    query_info[EXECUTOR] = executor_count


def get_query(record_list, dal_query_id):
    query_from_db = None
    for query in record_list:
        is_dal_query = re.search(UNIQUE_ID_REGEX, query[QUERY])
        query_id = re.search(UNIQUE_ID_REGEX, query[QUERY]).group()
        if is_dal_query and dal_query_id == query_id:
            query_from_db = query
            break
    return query_from_db


if __name__ == "__main__":
    main()

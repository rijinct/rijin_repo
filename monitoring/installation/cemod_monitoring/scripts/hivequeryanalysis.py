import requests
import json
import os, subprocess, errno
import re
import csv
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import concurrent.futures
import sys
import copy
sys.path.append("../utils/")
from postgres_connection import PostgresConnection
from loggerUtil import loggerUtil
from date_time_util import DateTimeUtil

REDUCER = 'Reducer'

MAPPER = 'Mapper'

APPLICATION_ID = 'Application id'

APPLICATION_INFO = 'Application info'

QUERY = 'Query'

QUERY_ID = 'Query id'

NUMBER_OF_MAPPERS_REDUCERS_REGEX = '(Number of maps: [0-9]+)|(Number of reduces: [0-9]+)'

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).\
    replace("(","(\"").replace(")","\")")

application_ids = ""
utility_type = 'hiveQueryStats'
hive_directory_path = '/var/local/monitoring/output/hive'
previous_date_time = datetime.now().replace(microsecond=0, second=0, minute=0) - timedelta(hours=1)
time_to_manipulate = '{0}-{1}-{2} {3}'.format(previous_date_time.strftime('%d'),
                                                  previous_date_time.strftime('%m'),
                                                  previous_date_time.strftime('%Y'),
                                                  previous_date_time.strftime('%H'))
logFile = 'hive_query_analysis_{}'.format(time_to_manipulate.replace(' ', '_'))
LOGGER = loggerUtil.__call__().get_logger(logFile)


def get_yarn_rm_ids(yarn_site_root):
    for each_property in yarn_site_root.findall('property'):
        if each_property.find('name').text == 'yarn.resourcemanager.ha.rm-ids':
            yarn_rm_ids = each_property.find('value').text.split(',')
            break
    return yarn_rm_ids


def get_yarn_rm_text_to_search(yarn_rm_ids):
    yarn_rm_key_template = 'yarn.resourcemanager.webapp.address'
    yarn_rm_text_to_search = []
    if tls_enabled.lower() == "yes":
        yarn_rm_key_template = 'yarn.resourcemanager.webapp.https.address'
    
    for yarn_rm_id in yarn_rm_ids:
        yarn_rm_text_to_search.append('{}.{}'.
                                      format(yarn_rm_key_template, yarn_rm_id))
    return yarn_rm_text_to_search


def get_yarn_rm_id_to_host_info(yarn_rm_ids, yarn_site_root):
    yarn_rm_id_to_host = dict({})
    yarn_rm_text_to_search = get_yarn_rm_text_to_search(yarn_rm_ids)
    for each_property in yarn_site_root.findall('property'):
        if each_property.find('name').text in yarn_rm_text_to_search:
            name = each_property.find('name').text
            yarn_rm_id = name.split('.')[len(name.split('.')) - 1]
            value = each_property.find('value').text
            yarn_rm_id_to_host[yarn_rm_id] = value
        if len(yarn_rm_id_to_host) > 1:
            break
    return yarn_rm_id_to_host


def get_resource_manager_active_host():
    resource_manager_active_host = ''
    yarn_site_root = ET.parse(r'/etc/hive/conf.cloudera.HIVE/yarn-site.xml').getroot()
    yarn_rm_ids = get_yarn_rm_ids(yarn_site_root)
    yarn_information = get_yarn_rm_id_to_host_info(yarn_rm_ids, yarn_site_root)
    for rm_id in yarn_information:
        status = subprocess.Popen(['yarn', 'rmadmin', '-getServiceState', rm_id],
                                  stdout=subprocess.PIPE).communicate()[0].strip()
        if status == 'active':
            resource_manager_active_host = yarn_information[rm_id]
            break
    return resource_manager_active_host


def get_all_application_ids(finish_time_begin, finish_time_end):
    resource_manager_active_host = get_resource_manager_active_host()
    LOGGER.debug('Resource manager active host is {}'.format(
        resource_manager_active_host))
    application_protocol = 'http'
    if tls_enabled.lower() == "yes":
        application_protocol = 'https'
    rest_url = '{}://{}/ws/v1/cluster/apps?finishedTimeBegin={}&finishedTimeEnd={}' \
               '&states=FINISHED,FAILED&user=hive'.format(application_protocol, resource_manager_active_host,
                                                  finish_time_begin, finish_time_end)
    try:
        response = requests.get(rest_url, verify=False)
    except requests.exceptions.RequestException as e:
        LOGGER.error('Failed connecting to yarn service api', exc_info=True)
        return None
    response = json.dumps(response.json())
    return json.loads(response, 'ascii')


def get_job_configuration(mapred_job_info):
    return re.search('Job File:([a-zA-Z0-9: /_.])*',
        mapred_job_info).group()


def get_query_information(job_conf_root):
    query_details = dict({})
    for property_value in job_conf_root.findall('property'):
        if property_value.find('name').text == 'hive.query.string':
            query_details[QUERY] = str(property_value.find('value').text)
        if property_value.find('name').text == 'hive.query.id':
            query_details[QUERY_ID] = str(property_value.find('value').text)
        if len(query_details) > 1:
            break
    return query_details


def delete_file(pathname):
    try:
        os.remove(pathname)
    except OSError:
        pass


def get_query_details(mapred_job_info):
    job_conf_info = get_job_configuration(mapred_job_info)
    job_conf_file_path = str(job_conf_info.split(':', 1)[1]).strip()
    xml_data = subprocess.Popen(['hadoop', 'dfs', '-cat', job_conf_file_path],
                      stdout=subprocess.PIPE).communicate()[0]
    job_conf_root = ET.fromstring(xml_data)
    return get_query_information(job_conf_root)


def update_mapper_info(mapred_job_info, mapper_reducer_info):
    number_of_mappers_regex = '(Number of maps: [0-9]+)'
    number_of_mappers_matched_line = re.compile(number_of_mappers_regex).search(mapred_job_info).group().strip()
    number_of_mappers_matched_line_array = number_of_mappers_matched_line.split(':')
    mapper_reducer_info[MAPPER] = int(number_of_mappers_matched_line_array[
        len(number_of_mappers_matched_line_array) - 1].strip())


def update_reducer_info(mapred_job_info, mapper_reducer_info):
    number_of_reducers_regex = '(Number of reduces: [0-9]+)'
    number_of_reducers_matched_line = re.compile(number_of_reducers_regex).search(mapred_job_info).group().strip()
    number_of_reducers_matched_line_array = number_of_reducers_matched_line.split(':')
    mapper_reducer_info[REDUCER] = int(number_of_reducers_matched_line_array[
        len(number_of_reducers_matched_line_array) - 1].strip())

def get_mapper_reducer_info(mapred_job_info):
    mapper_reducer_info = dict({})
    update_mapper_info(mapred_job_info, mapper_reducer_info)
    update_reducer_info(mapred_job_info, mapper_reducer_info)
    return mapper_reducer_info


def get_query_information_object(mapper_reducer_information, query_info, application_id):
    LOGGER.debug('Creating query information object for application id {}'.
                 format(application_id))
    query_information = dict({})
    query_information.update(query_info)
    query_information[APPLICATION_INFO] = {application_id: mapper_reducer_information}
    query_information[MAPPER] = mapper_reducer_information[MAPPER]
    query_information[REDUCER] = mapper_reducer_information[REDUCER]
    return query_information


def get_application_id_information(index):
    query_information_object = None
    application_id = str(application_ids['apps']['app'][index]['id'])
    job_id = re.sub('application', 'job', application_id)
    mapred_job_info = subprocess.Popen(['mapred', 'job', '-status', job_id], stdout=subprocess.PIPE).communicate()[0]
    LOGGER.debug('Getting query info for application id {}'.format(application_id))
    query_info = get_query_details(mapred_job_info)
    if re.search('(DALQuery_[0-9]*)', query_info[QUERY]):
        LOGGER.debug('Getting mapper and reducer information for application id {}'.format(
            application_id))
        mapper_reducer_information = get_mapper_reducer_info(mapred_job_info)
        query_information_object = get_query_information_object(mapper_reducer_information, 
                                    query_info, application_id)
    return query_information_object



def merge_query_info(merged_query_info, query_info):
    if query_info[QUERY_ID] in merged_query_info.keys():
        merged_query_info.get(query_info[QUERY_ID])[APPLICATION_INFO].update(query_info[APPLICATION_INFO])
        merged_query_info.get(query_info[QUERY_ID])[MAPPER] += query_info[MAPPER]
        merged_query_info.get(query_info[QUERY_ID])[REDUCER] += query_info[REDUCER]
    else:
        merged_query_info[query_info[QUERY_ID]] = query_info

def merge(results):
    merged_query_info = dict({})
    for query_info in results:
        if query_info is not None:
            merge_query_info(merged_query_info, query_info)
    return merged_query_info


def create_output_directories(directory_path):
    try:
        os.makedirs(directory_path)
    except OSError as e:
        pass


def write_rows_to_csv(queries_information, dict_writer):
    for query_information in queries_information:
        query_information_copy = copy.deepcopy(query_information)
        dict_writer.writerow(query_information_copy)


def write_csv(queries_information, formatted_start_time):
    file_name_suffix = '{0}-{1}-{2}_HH{3}'.format(formatted_start_time.strftime('%d'),
                             formatted_start_time.strftime('%m'),
                             formatted_start_time.strftime('%Y'),
                             formatted_start_time.strftime('%H'))
    keys = queries_information[0].keys()
    csv_file_path = '{}/hiveQueryAnalysis_{}.csv'.format(
        hive_directory_path, file_name_suffix)
    LOGGER.info("Writing csv in file path : {}".format(csv_file_path))
    with open(csv_file_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        write_rows_to_csv(queries_information, dict_writer)
    LOGGER.info("Csv file writing done successfully")


def push_data_to_db(json_content, start_time):
    LOGGER.info("Pushing data to db")
    postgres_connection = PostgresConnection.get_instance()
    sql_query = 'insert into sairepo.hm_stats values (%s, %s, %s)'
    record = (start_time, utility_type, json_content)
    postgres_connection.execute_query_with_record(sql_query, record)
    LOGGER.info("Data pushed successfully to db")


def write_csv_and_push_data_to_db(finish_time_begin, query_id_information):
    if len(query_id_information.keys()) > 0:
        formatted_start_time = DateTimeUtil.get_date_from_epoch(
            finish_time_begin, '%Y-%m-%d %H:%M:%S')
        write_csv(query_id_information.values(), formatted_start_time)
        push_data_to_db(json.dumps(query_id_information.values()),
                        formatted_start_time)

def spawn_thread(results, array_of_threads_to_be_spawned, executor):
    for result in executor.map(get_application_id_information, 
        array_of_threads_to_be_spawned):
        if result is not None:
            results.append(result)

def get_details_for_application_id(application_ids):
    results = []
    number_of_app_ids = len(application_ids['apps']['app'])
    LOGGER.info("Total number of ids found : {}".format(number_of_app_ids))
    array_of_threads_to_be_spawned = range(0, number_of_app_ids)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        spawn_thread(results, array_of_threads_to_be_spawned, executor)
    return results

def process_all_hive_application_id(finish_time_begin, finish_time_end):
    LOGGER.info("Getting all application id's spawned in time interval {} - {}".
                format(finish_time_begin, finish_time_end))
    global application_ids
    application_ids = get_all_application_ids(finish_time_begin, finish_time_end)
    if application_ids is not None:
        results = get_details_for_application_id(application_ids)
        query_id_information = merge(results)
        write_csv_and_push_data_to_db(finish_time_begin, query_id_information)


def main():
    LOGGER.info('Logs available at /var/local/monitoring/log/{}.log'.format(logFile))
    finish_time_begin = DateTimeUtil.parse_date('{}:00:00'.format(time_to_manipulate))
    finish_time_end = DateTimeUtil.parse_date('{}:59:59'.format(time_to_manipulate))
    epoch_finish_time_begin = int(finish_time_begin.strftime('%s'))*1000
    epoch_finish_time_end = int(finish_time_end.strftime('%s'))*1000
    create_output_directories(hive_directory_path)
    process_all_hive_application_id(epoch_finish_time_begin, epoch_finish_time_end)


if __name__ == '__main__':
    main()
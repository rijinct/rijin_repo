import sys
import os
import csv
import datetime
from commands import getoutput
from concurrent import futures
from dateutil.parser import parse

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from jsonUtils import JsonUtils
from dbUtils import DBUtil
from enum_util import ETLTopologyStatus

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh"
          ).read()).replace("(", "(\"").replace(")", "\")")

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)

SOURCE_AND_SINK_UP, SOURCE_UP, SINK_UP, SOURCE_AND_SINK_DOWN = range(4)
NO_DELAY_TIME, ALL_NODES_DOWN = [-1, -2]
SOURCE, SINK = 'source', 'sink'


def get_ping_status(node):
    command_str = 'ping \\-c 1 %s | grep "packet loss" |\
 gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' ' % (node, "%")
    return getoutput(command_str)


def get_topologies():
    sql_query = PropertyFileUtil('etl_topologies', 'ColumnSection'
                                 ).getValueForKey().split(';')[0]
    command_str = 'psql -h %s -d %s -t -U sairepo -c "%s;"' % (
        cemod_postgres_sdk_fip_active_host, cemod_sdk_db_name, sql_query)
    output_data = getoutput(command_str).strip().splitlines()
    list_of_topologies = []
    for line in output_data:
        topology_info = [
            date_to_fetch.strip() for date_to_fetch in line.split('|')]
        list_of_topologies.append(topology_info)
    return list_of_topologies


def get_input_output_delay_time(input1, input2, dir_path):
    if dir_path == '/mnt/staging/import':
        command_string = 'hdfs dfs -ls  %s/%s/ | grep "^d" | sort -k6,7 |\
 grep -w %s | tail -1 | tr -s \' \' | cut -d\' \' -f6,7' % (
            dir_path, input1, input2)
    else:
        command_string = 'hdfs dfs -ls  %s/%s/%s/ | grep "^d" |\
 sort -k6,7 | tail -1 | tr -s \' \' | cut -d\' \' -f6,7' % (
            dir_path, input1, input2)
    out_put_date_time = getoutput(command_string)
    if not out_put_date_time:
        out_put_date_time = get_output_date_time(dir_path, input1)
        return out_put_date_time, get_delay_time(out_put_date_time)
    else:
        return out_put_date_time, get_delay_time(out_put_date_time)


def get_output_date_time(dir_path, input1):
    command_string = 'hdfs dfs -ls  %s/%s/ | grep "^d" | sort -k6,7 | tail -1 |\
 tr -s \' \' | cut -d\' \' -f6,7' % (dir_path, input1)
    out_put_date_time = getoutput(command_string)
    return out_put_date_time


def get_delay_time(out_put_date_time):
    try:
        delay_time = datetime.datetime.now() - datetime.datetime.strptime(
            out_put_date_time, '%Y-%m-%d %H:%M')
        if delay_time.days:
            return int(
                round(delay_time.days * (24 * 60) + (delay_time.seconds / 60)))
        else:
            return int(round(delay_time.seconds / 60))
    except ValueError as error:
        LOGGER.info(error)


def get_source_and_sink_status(toplology, hostname):
    command_str = 'ssh %s ps -eaf | grep -i ETLServiceStarter | grep %s' % (
        hostname, toplology)
    output_str = getoutput(command_str)
    if output_str:
        return get_source_and_sink_pid(output_str)


def get_source_and_sink_pid(output_str):
    connectortype_data_list = list(
        filter(lambda x: True if 'nohup' not in x else False,
               output_str.strip().splitlines()))
    source_and_sink_pid_list = []
    try:
        for connectortype_data in connectortype_data_list:
            connectortype_data = connectortype_data.split()
            add_source_and_sink_pid(connectortype_data,
                                    source_and_sink_pid_list)
    except Exception as e:
        LOGGER.info(e)
    return source_and_sink_pid_list


def add_source_and_sink_pid(connectortype_data, source_and_sink_pid_list):
    for index, item in enumerate(connectortype_data):
        if 'connectortype' in item:
            source_and_sink_pid_list.append((connectortype_data[1],
                                             connectortype_data[index + 1]))


def fetch_and_write_topology_status(node, topology, status_to_write):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_to_write['Time'] = current_time
    topology_pid_list = get_source_and_sink_status(topology, node)
    fetch_topology_status_on_nodes(node, status_to_write, topology_pid_list)


def write_delay_time(output_and_delay_time, delay_type, status_to_write):
    delay_time = output_and_delay_time[1]
    if delay_time is not None:
        time = delay_time
    else:
        time = NO_DELAY_TIME
    status_to_write[delay_type] = time
    status_to_write['_'.join((delay_type).split('_')[0::2])] = \
        output_and_delay_time[0]


def fetch_topology_status_on_nodes(node, status_to_write, topology_pid_list):
    if topology_pid_list:
        connector_types_list = [data_item[1] for data_item in topology_pid_list
                                if int(data_item[0]) and len(data_item) > 1]
        write_source_and_sink_status(connector_types_list,
                                     node, status_to_write)
    else:
        status_to_write[node] = SOURCE_AND_SINK_DOWN


def write_source_and_sink_status(connector_types_list, node, status_to_write):
    if SOURCE in connector_types_list and SINK in connector_types_list:
        status_to_write[node] = SOURCE_AND_SINK_UP
    elif SOURCE in connector_types_list or SINK in connector_types_list:
        for connector in connector_types_list:
            write_source_or_sink_status_up(connector, node, status_to_write)
    else:
        status_to_write[node] = SOURCE_AND_SINK_DOWN


def write_source_or_sink_status_up(connector, node, status_to_write):
    if connector == SOURCE:
        status = SOURCE_UP
    else:
        status = SINK_UP
    status_to_write[node] = status


def fetch_topology_status_info_on_nodes(topology_data):
    count_of_nodes_down = 0
    status_to_write = {'Topology': topology_data[0]}
    write_delay_time(
        get_input_output_delay_time(topology_data[1].split('/')[0],
                                    topology_data[1].split('/')[1],
                                    '/mnt/staging/import'), 'input_delay_time',
        status_to_write)
    write_delay_time(
        get_input_output_delay_time(topology_data[2].split('/')[0],
                                    topology_data[2].split('/')[1],
                                    '/ngdb/us/import'), 'output_delay_time',
        status_to_write)
    write_topology_status_on_nodes(count_of_nodes_down, status_to_write,
                                   topology_data)
    write_topology_partition_level_and_usage_stats(status_to_write,
                                                   topology_data)
    return status_to_write


def write_topology_partition_level_and_usage_stats(status_to_write,
                                                   topology_data):
    status_to_write['usage_job'] = topology_data[3]
    status_to_write['partition'] = topology_data[4]
    status_to_write['usage_boundary'] = topology_data[5]
    status_to_write['usage_delay'] = get_usage_delay(topology_data[5])


def get_usage_delay(boundary):
    if boundary != '':
        return '%.f' % ((datetime.datetime.now() - parse(boundary)
                         ).total_seconds() / 60)
    else:
        return str(ETLTopologyStatus.DELAY_WHEN_BOUNDARY_NULL.value)


def write_topology_status_on_nodes(count_of_nodes_down, status_to_write,
                                   topology_data):
    for node in cemod_kafka_app_hosts.split():
        if get_ping_status(node):
            fetch_and_write_topology_status(node, topology_data[0],
                                            status_to_write)
        else:
            count_of_nodes_down += 1
            status_to_write[node] = SOURCE_AND_SINK_DOWN
    if count_of_nodes_down == len(cemod_kafka_app_hosts.split()):
        status_to_write['delay_time'] = ALL_NODES_DOWN


def write_topology_status_on_nodes_to_csv(topology_status):
    global file_name
    UTIL_TYPE = 'TopologiesStatusOnNodes'
    FIELD_NAMES = ['Time', 'Topology'] + cemod_kafka_app_hosts.split() + [
        'input_time', 'input_delay_time', 'output_time', 'output_delay_time',
        'partition', 'usage_job', 'usage_boundary', 'usage_delay'
    ]
    out_put_directory = PropertyFileUtil(UTIL_TYPE,
                                         'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % (
        UTIL_TYPE, datetime.datetime.now().strftime("%Y-%m-%d_%H-%M"))
    file_exist = os.path.exists(os.path.join(out_put_directory, file_name))
    LOGGER.info('writing %s status on %s and delay_time to csv' % 
                (topology_status['Topology'], ', '.join(
                    cemod_kafka_app_hosts.split())))
    with open(os.path.join(out_put_directory, file_name), 'a') as file_object:
        writer = csv.DictWriter(file_object, fieldnames=FIELD_NAMES)
        if not file_exist:
            writer.writeheader()
        writer.writerow(topology_status)


def push_to_postgres():
    UTIL_TYPE = 'TopologiesStatusOnNodes'
    outPutDirectory = PropertyFileUtil(UTIL_TYPE, 'DirectorySection'
                                       ).getValueForKey()
    jsonFileName = JsonUtils().convertCsvToJson(os.path.join(outPutDirectory,
                                                             file_name))
    LOGGER.info('Pushing all topologies status and delay_times to Postgres DB')
    DBUtil().pushDataToPostgresDB(jsonFileName, UTIL_TYPE)


def main():
    with futures.ThreadPoolExecutor() as executor:
        list_of_all_topologies_status_data = list(executor.map(
            fetch_topology_status_info_on_nodes, get_topologies()))
        for topology_status in list_of_all_topologies_status_data:
            write_topology_status_on_nodes_to_csv(topology_status)
    push_to_postgres()


if __name__ == '__main__':
    main()

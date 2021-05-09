import sys
import os
import json
import datetime
import pexpect
from commands import getoutput
from concurrent import futures
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from csvUtil import CsvUtil
from dbUtils import DBUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
     read()).replace("(", "(\"").replace(")", "\")")

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(
    scriptName=script_name, currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)

list_of_topologies_time_spans = []


def get_topologies_configured():
    topologies = []
    xmlparser = minidom.parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentTag = xmlparser.getElementsByTagName('TOPOLOGIES')[0]
    propertyTag = parentTag.getElementsByTagName('property')
    for property in propertyTag:
        try:
            topologies.append(property.attributes['Name'].value.split('_1')[0])
        except Exception as e:
            LOGGER.info(e)
    return topologies


def get_list_of_topologies_dirs():
    sql_query = PropertyFileUtil('etl_topologies', 'ColumnSection'). \
        getValueForKey().split(';')[0]
    command_str = 'psql -h %s -d %s -t -U sairepo -c "%s;"' % (
        cemod_postgres_sdk_fip_active_host, cemod_sdk_db_name, sql_query)
    topologies_data_from_postgres = getoutput(command_str).strip().splitlines()
    list_of_topologies_and_dirs = []
    topologies_configured = get_topologies_configured()
    add_topology_and_dir_to_list(list_of_topologies_and_dirs,
                                 topologies_configured,
                                 topologies_data_from_postgres)
    return list_of_topologies_and_dirs


def add_topology_and_dir_to_list(list_of_topologies_and_dirs,
                                 topologies_configured,
                                 topologies_data_from_postgres):
    for topology_data in topologies_data_from_postgres:
        list_of_topology_name_and_dir = [data.strip() for data in
                                         topology_data.split('|')]
        topology = list_of_topology_name_and_dir[0]
        topology_input_dir = list_of_topology_name_and_dir[1]
        if topology in topologies_configured:
            list_of_topologies_and_dirs.append((topology, topology_input_dir))


def fetch_time_spans_in_files_of_topology(topology_data):
    temp_directory = 'temp_1_{}'.format(topology_data[0])
    path_for_temp_dir = '/var/local/monitoring/log/%s' % temp_directory
    check_dir_if_exists_and_remove(path_for_temp_dir)
    getoutput('mkdir %s' % path_for_temp_dir)
    child = pexpect.spawn('watch hadoop fs -copyToLocal '
                          '/mnt/staging/import/{0}/*dat {1}/'.
                          format(topology_data[1], path_for_temp_dir))
    child.expect([pexpect.TIMEOUT], timeout=180)
    return fetch_and_write_time_spans_in_files(path_for_temp_dir,
                                               topology_data)


def fetch_and_write_time_spans_in_files(path_for_temp_dir, topology_data):
    number_of_files_in_temp_dir = get_total_files_exists_in_temp_dir(
        path_for_temp_dir)
    if number_of_files_in_temp_dir:
        return get_time_spans_in_files(path_for_temp_dir,
                                       topology_data,
                                       number_of_files_in_temp_dir)
    else:
        return fetch_and_write_no_files_to_scan(topology_data)


def check_dir_if_exists_and_remove(path_for_temp_dir):
    if os.path.exists(path_for_temp_dir):
        cmd_to_remove_dir = 'rm -rf %s' % path_for_temp_dir
        getoutput(cmd_to_remove_dir)
        LOGGER.info('successfully removed already existing {} directory'.
                    format(path_for_temp_dir))


def get_time_spans_in_files(path_for_temp_dir, topology_data, total_files):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cmd_str = '''awk -F\',\' 'string_to_replace' temp_dir/*.dat | awk '
        {{ print substr($0,1,16) }}'  | awk '{{ split($2,a,":");
        printf "%s %s:%02d \\n",$1,a[1],int(a[2]/5)*5}}' | sort -u'''

    cmd_str = get_cmd_str_to_fetch_time_spans(cmd_str,
                                              path_for_temp_dir, topology_data)
    time_spans = getoutput(cmd_str).split('\n')
    LOGGER.info('time spans in files of dir %s writing to csv' %
                topology_data[1])
    data_to_write = {'Time': current_time, 'Topology': topology_data[0],
                     'Topology_dir': topology_data[1], 'Total_files_fetched':
                         str(total_files), 'Time_spans_in_file':
                         str(len(time_spans)), 'min_time': time_spans[0],
                     'max_time': time_spans[-1]}
    return data_to_write


def get_cmd_str_to_fetch_time_spans(cmd_str, path_for_temp_dir, topology_data):
    if topology_data[0] == 'BCSI':
        cmd_str = cmd_str.replace('string_to_replace', '{{ print $5 }}'). \
            replace('temp_dir', path_for_temp_dir)
    elif topology_data[0] == 'TT':
        cmd_str = cmd_str.replace('string_to_replace', '{{ print $3 }}'). \
            replace('temp_dir', path_for_temp_dir)
    else:
        cmd_str = cmd_str.replace('string_to_replace', '{{ print $1 }}'). \
            replace('temp_dir', path_for_temp_dir)
    return cmd_str


def fetch_and_write_no_files_to_scan(topology_data):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOGGER.info('No files to scan in dir %s' % topology_data[1])
    data_to_write = {'Time': current_time, 'Topology': topology_data[0],
                     'Topology_dir': topology_data[1],
                     'Total_files_fetched': '0', 'Time_spans_in_file': '-1',
                     'min_time': '-1', 'max_time': '-1'}
    return data_to_write


def get_total_files_exists_in_temp_dir(temp_directory):
    cmd_string = 'ls -lrt %s' % temp_directory
    output = getoutput(cmd_string)
    if output:
        if 'No such file or directory' not in output and \
                '.dat' in output:
            list_of_output_data = output.strip().splitlines()
            files = [file for file in list_of_output_data
                     if file.endswith('.dat')]
            return len(files)


def remove_temp_directories():
    cmd_string = 'rm -rf /var/local/monitoring/log/temp_1_*'
    try:
        getoutput(cmd_string)
    except Exception as e:
        LOGGER.info(e)
    else:
        LOGGER.info('successfully removed all temp files')


def push_time_stamps_to_postgres():
    HM_TYPE = 'CheckTimeSpansInFile'
    json_data = '[%s]' % (','.join(data_to_push))
    DBUtil().jsonPushToPostgresDB(json_data, HM_TYPE)


def main():
    global data_to_push
    data_to_push = []
    with futures.ThreadPoolExecutor() as executor:
        list_of_topologies_time_spans = list(
            executor.map(fetch_time_spans_in_files_of_topology,
                         get_list_of_topologies_dirs()))
    for data in list_of_topologies_time_spans:
        CsvUtil().writeDictToCsv('CheckTimeSpansInFile', data)
        data_to_push.append(json.dumps(data))
    push_time_stamps_to_postgres()
    remove_temp_directories()


if __name__ == '__main__':
    main()

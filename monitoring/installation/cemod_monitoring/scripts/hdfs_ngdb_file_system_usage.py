import datetime
import sys
import os
import json
from commands import getstatusoutput
from hdfs_hbase_file_system_usage import get_usage_in_gb

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from loggerUtil import loggerUtil

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
current_time = datetime.datetime.now().strftime(TIME_FORMAT)
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)


def get_file_system_disk_usage_data(cmd_str):
    list_of_dir_usage_data = []
    status, output_data = getstatusoutput(cmd_str)
    if not status:
        list_of_output_data = output_data.strip().splitlines()
        for data in list_of_output_data:
            usage1, usage2, directory = data.split()
            list_of_dir_usage_data.append(
                (get_usage_in_gb(usage1), get_usage_in_gb(usage2), directory))
    return list_of_dir_usage_data


def write_file_systems_usage_to_csv(util_name, file_systems_usages_list):
    data_to_push_to_db = []
    current_time = datetime.datetime.now().strftime(TIME_FORMAT)
    for file_system_usage in file_systems_usages_list:
        output_to_write = {
            'Time': current_time,
            'Directory': file_system_usage[2],
            'Usage_with_replication(GB)': str(file_system_usage[1]),
            'Usage_without_replication(GB)': str(file_system_usage[0])
        }
        CsvUtil().writeDictToCsv(util_name, output_to_write)
        data_to_push_to_db.append(json.dumps(output_to_write))
    LOGGER.info('Pushing {} data to DB'.format(util_name))
    push_data_to_postgres(data_to_push_to_db, util_name)

def fetch_and_write_hdfs_disk_usage_data(util_name, cmd_str):
    status, output_data = getstatusoutput(cmd_str)
    if not status:
        list_of_output_data = output_data.strip().splitlines()
        data = list_of_output_data[1].split(' ')
        data = [i for i in data if i]
        output_to_write = {
            'Time': current_time,
            'FileSystem' : data[0],
            'Size': data[1] + data[2],
            'Used': data[3]+data[4],
            'Available': data[5]+data[6],
            'Use(%)': data[7]
        }
        CsvUtil().writeDictToCsv(util_name, output_to_write)
        LOGGER.info('Pushing {} data to DB'.format(util_name))
        push_data_to_postgres([json.dumps(output_to_write)], util_name)

def write_ngdb_file_systems_data_to_csv(util_name, directory):
    if util_name =='HdfsUsage':
        fetch_and_write_hdfs_disk_usage_data(util_name, directory)
    else:
        list_of_ngdb_file_systems_usages = []
        fetch_and_write_ngdb_file_sys_usage(directory, list_of_ngdb_file_systems_usages)
        write_file_systems_usage_to_csv(util_name, list_of_ngdb_file_systems_usages)
    
def fetch_and_write_ngdb_file_sys_usage(directory, list_of_ngdb_file_systems_usages):
    cmd_str = 'hadoop fs -du {}'.format(directory)
    list_of_ngdb_file_systems_usages.extend(get_file_system_disk_usage_data(cmd_str))

def push_data_to_postgres(data_to_push, hm_type):
    json_data = '[%s]' % (','.join(data_to_push))
    DBUtil().jsonPushToPostgresDB(json_data, hm_type)

def fetch_and_write_file_system_data():
    util,directory,info = '','',''
    data = {"USAGE":['Getting Usage File system','NgdbFileSystemUsage', '/ngdb/us/*_1/'],"DIMENSION":['Getting ES File system','NgdbEsFileSystemUsage', '/ngdb/es/*_1/'],"NGDB":['Getting ngdb File system',  'NgdbFileSystem', '/ngdb/'],"TOTAL":['Getting Total File system', 'TotalFileSystemUsage', '/'],"HDFS":['Getting HDFS File system',  'HdfsUsage', 'hdfs dfs -df -h']}
    
    if sys.argv[1] in ("15MIN","HOUR","DAY","WEEK","MONTH"):
        info = 'Getting PS File system'
        util = 'NgdbPsFileSystemUsage_{}'.format(sys.argv[1])
        directory = '-s /ngdb/ps/*/*{}*'.format(sys.argv[1])
    elif sys.argv[1] in data.keys():
        info,util,directory = data[sys.argv[1]]
    
    LOGGER.info(info)
    write_ngdb_file_systems_data_to_csv(util, directory)

def main():
    fetch_and_write_file_system_data()
    
if __name__ == '__main__':
    main()

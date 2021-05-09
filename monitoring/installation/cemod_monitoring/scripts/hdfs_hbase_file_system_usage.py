import datetime
import sys
import os
import json
from commands import getstatusoutput

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


def get_usage_in_gb(usage):
    return round(float(usage) / (1024 * 1024 * 1024), 3)


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


def write_hbase_wals_and_hbase_staging_file_system_usage_to_csv():
    for directory in ['/hbase/WALs/', '/hbase/hbase-staging/']:
        util_name = 'Hbase{}FileSystemUsage'.format(directory.split('/')[2])
        cmd_str = 'hadoop fs -du {}'.format(directory)
        list_of_usage_data = get_file_system_disk_usage_data(cmd_str)
        LOGGER.info('writing {} data to csv'.format(util_name))
        write_file_systems_usage_to_csv(util_name, list_of_usage_data)


def write_hbase_file_systems_data_to_csv():
    list_of_hbase_file_systems_usages = []

    UTIL_NAME = 'HbaseFileSystemUsage'
    for directory in [
            '/hbase/', '/hbase/archive/', '/hbase/corrupt/', '/hbase/data/',
            '/hbase/hbase.id/', '/hbase/hbase.version/'
    ]:
        fetch_and_write_hbase_file_sys_usage(
            directory, list_of_hbase_file_systems_usages)
    write_file_systems_usage_to_csv(UTIL_NAME,
                                    list_of_hbase_file_systems_usages)


def fetch_and_write_hbase_file_sys_usage(directory,
                                         list_of_hbase_file_systems_usages):
    cmd_str = 'hadoop fs -du {}'.format(directory)
    list_of_hbase_file_systems_usages.extend(
        get_file_system_disk_usage_data(cmd_str))


def push_data_to_postgres(data_to_push, hm_type):
    json_data = '[%s]' % (','.join(data_to_push))
    DBUtil().jsonPushToPostgresDB(json_data, hm_type)


def main():
    write_hbase_file_systems_data_to_csv()
    write_hbase_wals_and_hbase_staging_file_system_usage_to_csv()


if __name__ == '__main__':
    main()

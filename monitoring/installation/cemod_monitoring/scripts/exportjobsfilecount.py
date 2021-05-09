import sys
import os
import json
from commands import getoutput
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from linuxutils import LinuxUtility, OutputUtility
from loggerUtil import loggerUtil
from jsonUtils import JsonUtils
from postgres_connection import PostgresConnection

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")


def main():
    global postgres_conn
    postgres_conn = PostgresConnection().get_instance()
    create_logger()
    collect_export_jobs_stats()


def create_logger():
    global logger, date
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(log_file_name, date))


def collect_export_jobs_stats():
    try:
        if sys.argv[1] == 'sdk':
            collect_sdk_export_job_stats()
        elif sys.argv[1] == 'cct':
            collect_cct_export_job_stats()
        else:
            print_error_message()
    except IndexError:
        print_error_message()
    finally:
        postgres_conn.close_connection()


def print_error_message():
    logger.info('Incorrect Script Usage at %s. Usage of the script: python %s (sdk/cct)' % (
        date, os.path.basename(sys.argv[0])))


def collect_sdk_export_job_stats():
    global file_name
    output_directory = PropertyFileUtil('sdkExportJobStats', 'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % ('sdkExportJobStats', datetime.today().strftime('%Y-%m-%d-%H-%M-%S'))
    export_jobs = get_sdk_export_jobs_location(export_jobs={})
    write_sdk_file_count_and_memory(export_jobs)
    push_to_postgres('sdkExportJobStats')


def collect_cct_export_job_stats():
    global file_name
    output_directory = PropertyFileUtil('cctExportJobStats', 'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % ('cctExportJobStats', datetime.today().strftime('%Y-%m-%d-%H-%M-%S'))
    files_dict = process_cct_export_files('/mnt/cctexport/')
    write_cct_export_stats('/mnt/cctexport/', files_dict)
    push_to_postgres('cctExportJobStats')


def get_sdk_export_jobs_location(export_jobs):
    query = PropertyFileUtil('exportJobLocationQuery', 'PostgresSqlSection').getValueForKey()
    output = postgres_conn.fetch_records_as_list(query)
    for job in output:
        export_job, location = job.split(',')
        export_jobs[export_job] = location
    return export_jobs


def write_sdk_file_count_and_memory(export_jobs):
    for export_job in export_jobs:
        total_files = LinuxUtility('str').get_count_of_files(export_jobs[export_job])
        size = LinuxUtility('str').get_size_of_dir(export_jobs[export_job])
        write_to_csv(export_job, total_files, size, 'sdkExportJobStats')


def write_to_csv(export_job, total_files, size, csv_type):
    output_string = ','.join([date, export_job, total_files, size])
    logger.info('Job: %s, No. of files: %s, Size: %s' % (export_job, total_files, size))
    CsvUtil().writeToCsv(file_name, csv_type, output_string)


def process_cct_export_files(path):
    files = LinuxUtility('str_list').get_list_of_files(path, '.zip')
    files = [i[1 + i.index('_'):i.rindex('_', 0, i.rindex('_'))] for i in files]
    files_dict = {i: files.count(i) for i in set(files)}
    return files_dict


def write_cct_export_stats(path, files_dict):
    for file_name in files_dict:
        size = LinuxUtility('float').get_size_of_files(path, file_name) / 1024
        write_to_csv(file_name.replace('_', ' '), str(files_dict[file_name]), str(int(size)), 'cctExportJobStats')


def push_to_postgres(util_type):
    output_directory = PropertyFileUtil(util_type, 'DirectorySection').getValueForKey()
    if os.path.exists(output_directory+file_name):
        json_file = JsonUtils().convertCsvToJson(os.path.join(output_directory, file_name))
        DBUtil().pushDataToPostgresDB(json_file, util_type)


if __name__ == '__main__':
    main()

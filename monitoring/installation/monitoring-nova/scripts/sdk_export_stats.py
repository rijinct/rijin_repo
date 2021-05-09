import sys
import os
import json
from subprocess import getoutput
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from dbConnectionManager import DbConnection
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from linuxutils import LinuxUtility, OutputUtility
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

def main():
    collect_sdk_export_job_stats()


def collect_sdk_export_job_stats():
    global file_name,output_directory
    output_directory = PropertyFileUtil('sdkExportJobStats', 'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % ('sdkExportJobStats', datetime.today().strftime('%Y-%m-%d-%H-%M-%S'))
    export_jobs = get_sdk_export_jobs_location(export_jobs={})
    write_sdk_file_count_and_memory(export_jobs)
    push_to_postgres(file_name)


def get_sdk_export_jobs_location(export_jobs):
    outputsql = PropertyFileUtil('exportJobLocationQuery', 'PostgresSqlSection').getValueForKey()
    output = DbConnection().getConnectionAndExecuteSql(outputsql, 'postgres')
    for job in output.split("\n"):
        export_job, location = job.split(',')
        export_jobs[export_job] = location
    return export_jobs


def write_sdk_file_count_and_memory(export_jobs):
    for export_job in export_jobs:
        total_files = LinuxUtility('str').get_count_of_files(export_jobs[export_job])
        size = LinuxUtility('str').get_size_of_dir(export_jobs[export_job])
        write_to_csv(export_job, total_files, size, 'sdkExportJobStats')


def write_to_csv(export_job, total_files, size, csv_type):
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    output_string = ','.join([date, export_job, total_files, size])
    logger.info("Job: '%s' , No. of files: '%s' , Size: '%s' ", export_job, total_files, size)
    CsvUtil().writeToCsv(csv_type, output_string, file_name)


def push_to_postgres(file_name):
    if os.path.exists(output_directory+file_name):
        json_file = JsonUtils().convertCsvToJson(os.path.join(output_directory, file_name))
        DBUtil().pushToMariaDB(json_file, 'sdkExportJobStats')



if __name__ == '__main__':
    main()

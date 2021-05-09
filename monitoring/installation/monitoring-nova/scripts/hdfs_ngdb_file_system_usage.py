#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 1
# Purpose: This script ngdb file system details to csv and mariadb.
#
# Date:  03-08-2020
#############################################################################
#############################################################################
from datetime import datetime
import json
import sys
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from jsonUtils import JsonUtils
from logger_util import *
from py4j.java_gateway import Py4JError, traceback
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

ALARM_KEY = AlarmKeys.HDFS_USED_PERC_ALARM.value


def main():
    try:
        global system_usage
        gateway_obj = DbConnection().initialiseGateway()
        system_usage = gateway_obj.jvm.com.nokia.monitoring.hive.HdfsFileSystemUsageManager("/etc/hadoop/conf/")
        fetch_and_write_file_system_data()
    except Py4JError:
        traceback.print_exc()
    finally:
        gateway_obj.shutdown()


def fetch_and_write_file_system_data():
    if sys.argv[1] == "USAGE":
        logger.info('Getting Usage File system')
        fetch_and_write_usage_file_system_data()
    elif sys.argv[1] == "DIMENSION":
        logger.info('Getting ES File system')
        fetch_and_write_es_file_system_data()
    elif sys.argv[1] in ("15MIN","HOUR","DAY","WEEK","MONTH"):
        logger.info('Getting PS File system')
        fetch_and_write_ps_file_system_data(sys.argv[1])
    elif sys.argv[1] == "NGDB":
        logger.info('Getting ngdb File system')
        fetch_and_write_ngdb_file_system_data()
    elif sys.argv[1] == "TOTAL":
        logger.info('Getting Total File system')
        fetch_and_write_total_file_system_data()
    elif sys.argv[1] == "HDFS":
        logger.info('Getting Total File system')
        fetch_and_write_hdfs_file_system_data()


def fetch_and_write_total_file_system_data():
    util = "hdfsUsedPerc"
    json_list = []
    file_sys_map = system_usage.getNgdbTotalSummary()
    ngdb_dict_in_gb = { item: round(file_sys_map[item] / (1000 * 1000 * 1000), 3) for item in file_sys_map.keys()}
    total_capacity = round((ngdb_dict_in_gb['Total Size']),2)
    used_capacity = round((ngdb_dict_in_gb['Total Used']),2)
    remaining_capacity = round((ngdb_dict_in_gb['Remaining']),2)
    hdfs_perc_occupied = round((ngdb_dict_in_gb['Total Used'] / ngdb_dict_in_gb['Total Size']) * 100, 2)
    hdfs_perc_dict = {'Time': datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,'Total Capacity(Gb)':total_capacity,'Used Capacity(Gb)':used_capacity ,'Available Capacity(Gb)':remaining_capacity ,'HDFS Used(%)':hdfs_perc_occupied}
    json_list.append(hdfs_perc_dict)
    file_name = '{0}_{1}.csv'.format(util, datetime.now().strftime("%Y-%m-%d_%H_%M"))
    CsvUtil().writeDictToCsvDictWriter("hdfsUsedPerc",hdfs_perc_dict, file_name)
    DBUtil().jsonPushToMariaDB(json.dumps(json_list),util)
    send_alert(hdfs_perc_occupied, json_list)


def send_alert(hdfs_perc_occupied, json_list):
    final_json_list = []
    threshold_value = configured_value()
    major_threshold, critical_threshold = threshold_value['MajorThreshold'],threshold_value['CriticalThreshold']
    final_json = json_list[0]
    final_json_list.append({ key: str(final_json[key]) for key in final_json.keys()})
    if hdfs_perc_occupied > float(major_threshold) and hdfs_perc_occupied < float(critical_threshold):
        severity = "MAJOR"
        threshold_value = major_threshold
        frame_html_and_send(final_json_list, severity, hdfs_perc_occupied, threshold_value)
    elif hdfs_perc_occupied > float(critical_threshold):
        severity = "CRITICAL"
        threshold_value = critical_threshold
        frame_html_and_send(final_json_list, severity, hdfs_perc_occupied, threshold_value)

def frame_html_and_send(final_json_list, severity, hdfs_perc_occupied, threshold_value):
    html = HtmlUtil().generateHtmlFromDictList("Total File System Usage", final_json_list)
    EmailUtils().frameEmailAndSend("[{0} ALERT]: HDFS Usage is {1}% which is greater than the threshold {2}%".format(severity, hdfs_perc_occupied, threshold_value), html, ALARM_KEY)
    SnmpAlarm.send_alarm(ALARM_KEY, final_json_list, severity)


def configured_value():
    xmlParser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentFileSystemTag = xmlParser.getElementsByTagName("HdfsFileSystem")[0]
    propertyTag=parentFileSystemTag.getElementsByTagName('totalUsedPerc')
    return {'MajorThreshold': propertyTag[0].attributes['MajorThreshold'].value,'CriticalThreshold': propertyTag[0].attributes['CriticalThreshold'].value}


def fetch_and_write_hdfs_file_system_data():
    hdfs_map = system_usage.getFileStatus('/', "ngdb")
    logger.debug("Getting info for total file system '%s'",hdfs_map)
    total_dict_in_gb = get_quota_in_gb(hdfs_map)
    write_data_to_csv_and_db('totalFileSystem', total_dict_in_gb)


def fetch_and_write_ngdb_file_system_data():
    ngdb_map = system_usage.getFileStatus('/ngdb/', "ngdb")
    logger.debug("Getting info for ngdb file system '%s'",ngdb_map)
    ngdb_dict_in_gb = get_quota_in_gb(ngdb_map)
    write_data_to_csv_and_db('ngdbFileSystem', ngdb_dict_in_gb)

def fetch_and_write_usage_file_system_data():
    usage_map = system_usage.getFileStatus('/ngdb/us/', "usage")
    logger.debug("Getting info for usage file system '%s'",usage_map)
    usage_dict_in_gb = get_quota_in_gb(usage_map)
    write_data_to_csv_and_db('fileSystemUsage', usage_dict_in_gb)


def fetch_and_write_es_file_system_data():
    quota = system_usage.getFileStatus('/ngdb/es/', "dimension")
    logger.debug("Getting info for es file system '%s'",quota)
    es_usage_dict_in_gb = get_quota_in_gb(quota)
    write_data_to_csv_and_db('fileSystemDim', es_usage_dict_in_gb)


def fetch_and_write_ps_file_system_data(duration):
    util = 'fileSystemAgg_{}'.format(duration)
    quota = system_usage.getFileStatus("/ngdb/ps", duration)
    logger.debug("Getting info for ps file system '%s'",quota)
    quota = get_quota_in_gb(quota)
    write_data_to_csv_and_db(util, quota)


def get_quota_in_gb(usage_dict):
    return { item: [round(usage_dict[item][0] / (1000 * 1000 * 1000), 3) ,round(usage_dict[item][1] / (1000 * 1000 * 1000),3)] for item in usage_dict.keys()}

def write_data_to_csv_and_db(util, usage_dict):
    json_list = []
    file_name = "{0}_{1}.csv".format(util,datetime.now().strftime("%Y-%m-%d_%H"))
    for key in usage_dict.keys():
        temp_dict = {}
        temp_dict['Time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        temp_dict['Directory'] = key
        temp_dict['Usage_with_replication(GB)'] = usage_dict[key][0]
        temp_dict['Usage_without_replication(GB)'] = usage_dict[key][1]
        CsvUtil().writeDictToCsvDictWriter(util, temp_dict, file_name)
        json_list.append(temp_dict)
    DBUtil().jsonPushToMariaDB(json.dumps(json_list),util)

if __name__ == '__main__':
    main()

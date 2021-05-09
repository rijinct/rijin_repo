'''
Created on 01-Sep-2020

@author: deerakum
'''
from collections import Counter
from concurrent import futures
from datetime import datetime
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from dateTimeUtil import DateTimeUtil
from dateutil import parser
from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils
from py4j.java_gateway import Py4JError
from py4j.java_gateway import traceback
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

IMP_PATH = '/mnt/staging/import/'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TABLE_LIMIT = 10
LARGE_FILES_ALARM_KEY = AlarmKeys.ETL_INPUT_FILE_SIZE_LARGE.value
SEVERITY = SnmpAlarm.get_severity_for_email(LARGE_FILES_ALARM_KEY)


def main():
    try:
        global file_manager
        logger.info("Started Finding out Large Files for configured Topologies")
        gateway_obj = DbConnection().initialiseGateway()
        file_manager = get_file_manager_object(gateway_obj)
        files_data = get_all_files_data()
        dat_ctr_list = get_list_without_processing_files(files_data)
        matched_dat_ctr_list = get_dat_files_list_matching_ctr(dat_ctr_list)
        final_list = get_old_files(matched_dat_ctr_list)
        take_action(final_list)
    except Py4JError:
        logger.exception(traceback.print_exc())
    finally:
        gateway_obj.shutdown()


def get_file_manager_object(gateway_obj):
    return gateway_obj.jvm.com.nokia.monitoring.hive.\
                    HdfsFilesManager("/etc/hadoop/conf/")


def get_all_files_data():
    topologies = get_list_of_topologies()
    with futures.ThreadPoolExecutor() as executor:
        return list(executor.map(get_files_and_sizes, topologies))


def get_list_of_topologies():
    global topologies
    logger.debug("Getting Topologies Info")
    topologies = []
    data = XmlParserUtils.get_topologies_from_xml()
    for name, info in data.items():
        topologies.append({
            'name':
            name,
            'hdfs_dir':
            '%s%s' % (IMP_PATH, info['Input']),
            'size_threshold':
            int(info['LargeSizeFileThresholdInMB'])
        })
    return topologies


def get_files_and_sizes(topology):
    logger.debug('Parsing topologies info for : %s' % topology['name'])
    data = get_files_and_sizes_in_dir(**topology)
    return data


def get_files_and_sizes_in_dir(name, hdfs_dir, size_threshold):
    result = []
    files = file_manager.getFilesWithSizes(hdfs_dir, "")
    for file, file_info in files.items():
        size = str(round(int(file_info[0]) / (1000 * 1000), 2))
        data = {
            'Topology': name,
            'Path': hdfs_dir,
            'File': file,
            'Modification Time': file_info[1],
            'Size (In MB)': size,
            'Size Threshold (In MB)': str(size_threshold)
        }
        result.append(data)
    return result


def get_list_without_processing_files(data):
    final_list = []
    for topology_data in data:
        for topo_dict in topology_data:
            if "processing" not in topo_dict['File']:
                final_list.append(topo_dict)
    return final_list


def get_dat_files_list_matching_ctr(dat_ctr_list):
    final_list = []
    file_names_list = get_all_file_names_from_dict(dat_ctr_list)
    for topo_dict in dat_ctr_list:
        file_name = topo_dict['File']
        if file_name.replace(".dat", "").replace(
                ".ctr", "") in file_names_list and "dat" in file_name:
            final_list.append(topo_dict)
    return final_list


def get_all_file_names_from_dict(dat_ctr_list):
    return [
        topo_info['File'].replace(".dat", "").replace(".ctr", "")
        for topo_info in dat_ctr_list
    ]


def get_old_files(matched_dat_ctr_list):
    final_list = []
    for topology_data in matched_dat_ctr_list:
        if check_file_timestamp(topology_data['Modification Time'], 60):
            final_list.append(topology_data)
    return final_list


def check_file_timestamp(time, delay_threshold):
    time = get_time_in_local_tz(time)
    delay = (datetime.now() - parser.parse(time))
    return int(delay.total_seconds() / 60) >= delay_threshold


def get_time_in_local_tz(time):
    return DateTimeUtil().convertUtcToLocalTime(parser.parse(time))


def take_action(data):
    breached_result = get_breached_limit_list(data)
    summary_list = prepare_summary_info(breached_result)
    final_summary_list = process_summary_list(summary_list)
    if breached_result and final_summary_list:
        send_email_alert(breached_result, final_summary_list)
        send_snmp_alarm(breached_data[:TABLE_LIMIT], summary_list)
        logger.info("Found Large Files, Sent Alert & Alarm")

def process_summary_list(summary_list):
    final_summary_list = []
    for summary_item in summary_list:
        temp_dict = {}
        for key in summary_item.keys():
            temp_dict['Topology'] = summary_item['Topology']
            temp_dict['Path'] = summary_item['Path']
            temp_dict['No. Of Large Files'] = str(summary_item['No. Of Large Files'])
        final_summary_list.append(temp_dict)
    return final_summary_list
            

def prepare_summary_info(final_list):
    summary_list = []
    topo_count = Counter(topo_dict['Topology'] for topo_dict in final_list)
    for key in topo_count.keys():
        temp_dict = {}
        temp_dict['Topology'] = key
        for topology_dict in topologies:
            if key == topology_dict['name']:
                temp_dict['Path'] = topology_dict['hdfs_dir']
        temp_dict['No. Of Large Files'] = topo_count[key]
        summary_list.append(temp_dict)
    return summary_list


def get_breached_limit_list(files_data):
    result = []
    for topology_data in files_data:
        if float(topology_data['Size (In MB)']) >= float(
                topology_data['Size Threshold (In MB)']):
            result.append(topology_data)
    return result


def send_email_alert(complete_data, summary_data):
    global breached_data
    breached_data = sorted(complete_data,
                           key=lambda k: float(k['Size (In MB)']),
                           reverse=True)
    subject = "ETL - Large Files @/mnt/staging/import"
    html = str(HtmlUtil().generateHtmlFromDictList(subject, summary_data))
    subject = "Top 10 Largest Files at ETL"
    html += str(HtmlUtil().generateHtmlFromDictList(subject, breached_data[:TABLE_LIMIT]))
    EmailUtils().frameEmailAndSend(
        "[{0} ALERT]: ETL - Large Files @/mnt/staging/import {1}".format(
            SEVERITY,
            datetime.now().strftime(TIME_FORMAT)), html, LARGE_FILES_ALARM_KEY)


def send_snmp_alarm(breached_data, summary_data):
    SnmpAlarm.send_alarm(LARGE_FILES_ALARM_KEY, summary_data + breached_data)


if __name__ == '__main__':
    main()

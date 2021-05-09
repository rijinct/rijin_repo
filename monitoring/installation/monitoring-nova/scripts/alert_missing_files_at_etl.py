'''
Created on 20-Aug-2020

@author: a4yadav
'''
from concurrent import futures
from datetime import datetime
import sys
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dateTimeUtil import DateTimeUtil
from dateutil import parser
from dbConnectionManager import DbConnection
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
CTR_THRESHOLD = 1
DAT_THRESHOLD = 1
HTML_TABLE_LIMIT = 10
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MISSING_DAT_AND_CTR_ALARM = AlarmKeys.MISSING_DAT_AND_CTR_ALARM.value
SEVERITY = SnmpAlarm.get_severity_for_email(MISSING_DAT_AND_CTR_ALARM)


def main():
    try:
        global file_manager, ref_db
        ref_db = {}
        gateway_obj = DbConnection().initialiseGateway()
        file_manager = get_file_manager_object(gateway_obj)
        missing_files_data = get_missing_files_data()
        take_action(missing_files_data)
    except Py4JError:
        logger.exception(traceback.print_exc())
    finally:
        gateway_obj.shutdown()


def get_file_manager_object(gateway_obj):
    return gateway_obj.jvm.com.nokia.monitoring.hive.\
                    HdfsFilesManager("/etc/hadoop/conf/")


def get_missing_files_data():
    topologies = get_list_of_topologies()
    with futures.ThreadPoolExecutor() as executor:
        result = list(executor.map(get_missing_files, topologies))
    return list(filter(None, result))


def get_list_of_topologies():
    logger.debug("Getting Topologies Info")
    topologies = []
    data = XmlParserUtils.get_topologies_from_xml()
    for name, info in data.items():
        topologies.append({
            'name': name,
            'hdfs_dir': '%s%s' % (IMP_PATH, info['Input']),
            'delay': int(info['DelayBeyondWhichToConsiderMissingDatCtrFilesInMINs'])
        })
    return topologies


def get_missing_files(topology):
    logger.info('Parsing topogies info for : %s' % topology['name'])
    data = get_missing_files_in_dir(**topology)
    if data:
        final = dict(zip(['Name', 'Path', 'Ctr', 'Dat'], list(map(str, data))))
        return final


def get_missing_files_in_dir(name, hdfs_dir, delay):
    files = file_manager.getCtrAndDatFiles(hdfs_dir)
    files = [dict(data) for data in files]
    if files == [{hdfs_dir: "Invalid Path"}, {hdfs_dir: "Invalid Path"}]:
        logger.debug('%s has invalid path: %s' % (name, hdfs_dir))
        return []
    else:
        missed_ctr, missed_dat = process_missing_files(name, delay, *files)
        return [name, hdfs_dir, len(missed_ctr), len(missed_dat)]


def process_missing_files(name, delay, ctr_files, dat_files):
    ctr_files = filter_files(name, ctr_files, delay)
    dat_files = filter_files(name, dat_files, delay)
    return (dat_files - ctr_files), (ctr_files - dat_files)


def filter_files(topology, files, delay):
    filtered, ref_data = [], {}
    for file_name, utc_time in files.items():
        if check_file_timestamp(utc_time, delay):
            name = file_name.replace('.ctr', '').replace('.dat', '')
            ref_data[file_name] = get_time_in_local_tz(utc_time)
            filtered.append(name)
    add_data_to_ref_db(topology, ref_data)
    return set(filtered)


def check_file_timestamp(time, delay_threshold):
    time = get_time_in_local_tz(time)
    delay = (datetime.now() - parser.parse(time))
    return int(delay.total_seconds() / 60) >= delay_threshold


def get_time_in_local_tz(time):
    return DateTimeUtil().convertUtcToLocalTime(parser.parse(time))


def add_data_to_ref_db(topology, ref_data):
    if topology in ref_db:
        ref_db[topology].update(ref_data)
        ref_db[topology] = dict(
            sorted(ref_db[topology].items(),
                   key=lambda x: datetime.strptime(x[1], TIME_FORMAT)))
    else:
        ref_db[topology] = ref_data


def take_action(data):
    breached_result = get_breached_limit_list(data)
    send_email_alert(*breached_result)
    send_snmp_alarm(*breached_result)


def get_breached_limit_list(files_data):
    ctr_result, dat_result = [], []
    keys = ['Topology', 'Path', 'Type', 'Count of Missing files']
    for data in files_data:
        if int(data['Ctr']) >= CTR_THRESHOLD:
            res = zip(keys, [data['Name'], data['Path'], 'CTR', data['Ctr']])
            ctr_result.append(dict(res))
        elif int(data['Dat']) >= DAT_THRESHOLD:
            res = zip(keys, [data['Name'], data['Path'], 'DAT', data['Dat']])
            dat_result.append(dict(res))
    return [ctr_result, dat_result]


def send_email_alert(ctr_data, dat_data):
    breached_data = sorted(ctr_data + dat_data,
                           key=lambda k: int(k['Count of Missing files']),
                           reverse=True)
    if breached_data:
        subject = "Missing CTR files and DAT files @/mnt/staging/import"
        html = str(HtmlUtil().generateHtmlFromDictList(subject, breached_data))
        html += get_html_body_for_topology(ctr_data, dat_data)
        EmailUtils().frameEmailAndSend(
            "[{0} ALERT]: Missing CTR and DAT files at {1}".format(
                SEVERITY,
                datetime.now().strftime(TIME_FORMAT)), html)


def get_html_body_for_topology(ctr_data, dat_data):
    table = get_html_table_for_file_type(ctr_data)
    table.extend(get_html_table_for_file_type(dat_data))
    subject = "Oldest %d files for each topology" % HTML_TABLE_LIMIT
    return str(HtmlUtil().generateHtmlFromDictList(subject, table))


def get_html_table_for_file_type(data):
    table = []
    data = sorted(data,
                  key=lambda k: int(k['Count of Missing files']),
                  reverse=True)
    topologies = list([data['Topology'] for data in data])
    for topology in topologies:
        table.extend(get_html_table_for_topology(topology))
    return table


def get_html_table_for_topology(name):
    result = []
    data = ref_db[name]
    for file in list(data.keys())[:HTML_TABLE_LIMIT]:
        res = {'Topology': name, 'File Name': file, 'Time Stamp': data[file]}
        result.append(res)
    return result


def send_snmp_alarm(ctr_data, dat_data):
    if ctr_data or dat_data:
        SnmpAlarm.send_alarm(MISSING_DAT_AND_CTR_ALARM, ctr_data + dat_data)


if __name__ == '__main__':
    main()

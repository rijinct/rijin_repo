import sys
from subprocess import getoutput
from dateutil import parser
from py4j.java_gateway import Py4JError, traceback

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from dbUtils import DBUtil
from logger_util import *
from dbConnectionManager import DbConnection
from dateTimeUtil import DateTimeUtil
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from enum_util import AlarmKeys
from monitoring_utils import XmlParserUtils
from htmlUtil import HtmlUtil
from datetime import datetime

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

ALARM_KEY_WRITE = AlarmKeys.HDFS_WRITE_ALARM.value
ALARM_KEY_READ = AlarmKeys.HDFS_READ_ALARM.value



def create_backup_file():
    getoutput('sudo dd if=/dev/zero of=/mnt/hadoop_monitoring.test\
 bs=1M count=5000')
    getoutput('sudo mkdir -p /mnt/hadoop.delete ;sudo  chown -R ngdb:ninstall\
 /mnt/hadoop.delete ; sudo chmod -R 777 /mnt/hadoop.delete')


def deleting_temp_files():
    getoutput('sudo rm -rf /mnt/hadoop_monitoring.test')
    getoutput('sudo rm -rf /mnt/hadoop.delete')


def get_hdfs_perfromance_stats():
    stats = ""
    try:
        gateway_obj = DbConnection().initialiseGateway()
        hdfs_perf_manager = get_perf_manager_object(gateway_obj)
        stats = hdfs_perf_manager.getPerfStats("/mnt/hadoop_monitoring.test",
                                               "/mnt/hadoop.delete",
                                               "/mnt/hadoop.delete")
    except Py4JError:
        traceback.print_exc()
    finally:
        execute_finally(gateway_obj)
    return process_stats(stats)


def process_stats(stats):
    keys = ['Date', 'hdfsreadtime', 'hdfswritetime']
    time_utc, read_time, write_time = stats.split(',')
    local_time = DateTimeUtil().convertUtcToLocalTime(parser.parse(time_utc))
    return dict(zip(keys, [local_time, read_time, write_time]))


def get_perf_manager_object(gateway_obj):
    print(('Connecting to {0}'.format("HDFS")))
    return gateway_obj.jvm.com.nokia.monitoring.hive.HdfsPerfCheckManager(
        "/etc/hadoop/conf/")


def execute_finally(gateway_obj):
    gateway_obj.shutdown()
    deleting_temp_files()


def write_stats_to_csv(stats):
    print("Performance Statistic: %s" % str(stats))
    CsvUtil().writeDictToCsv(identifier, stats)


def push_data_to_maria_db(stats):
    json = ('[%s]' % stats).replace("'", '"')
    DBUtil().jsonPushToMariaDB(json, identifier)

def get_breached_info(stats):
    breached_info_write = []
    breached_info_read = []
    write_threshold = 0
    hadooop_threshold = XmlParserUtils.get_hadoop_threshold()
    for item in hadooop_threshold:
        for keys in item.keys():
            if keys == 'name' and item[keys]  == 'write_threshold':
                write_threshold = int(item['value'])
            if keys == 'name' and item[keys]  == 'read_threshold':
                read_threshold = int(item['value'])
    for keys in stats.keys():
        if keys == 'hdfswritetime':
            actual_write_took = int(stats[keys])
        if keys == 'hdfsreadtime':
            actual_read_took = int(stats[keys])
    if actual_write_took > write_threshold:
        info = {'hadoop_write_took in seconds': str(actual_write_took),'Hadoop_Write_Threshold in seconds': str(write_threshold)}
        breached_info_write.append(info)
    if actual_read_took > read_threshold:
        info = {'hadoop_read_took in seconds': str(actual_read_took),'Hadoop_Read_Threshold in seconds': str(read_threshold)}
        breached_info_read.append(info)
    return breached_info_write,breached_info_read

def send_alert(breached_data,ALARM_KEY,type):
    if breached_data:
        severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
        html = str(HtmlUtil().generateHtmlFromDictList(
            "Hdfs Took more time for " + str(type), breached_data))
        EmailUtils().frameEmailAndSend("[{0} ALERT]:Hdfs  Taking more time for {1} at {2}".format(# noqa: 501
            severity, type, datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, ALARM_KEY)
        SnmpAlarm.send_alarm(ALARM_KEY, breached_data)


def main():
    global identifier
    identifier = "hdfsPerformance"
    deleting_temp_files() ## This is to make sure if any previous instance is failed, cleanup and start.
    create_backup_file()
    stats = get_hdfs_perfromance_stats()
    write_stats_to_csv(stats)
    push_data_to_maria_db(stats)
    breached_write, breached_read = get_breached_info(stats)
    send_alert(breached_write,ALARM_KEY_WRITE,'write')
    send_alert(breached_read,ALARM_KEY_READ,'read')



if __name__ == '__main__':
    main()

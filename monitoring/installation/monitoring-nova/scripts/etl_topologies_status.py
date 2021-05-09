#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  a4yadav
# Version: 1
# Purpose: This script writes topology details to csv and mariadb.
#
# Date:  05-03-2020
#############################################################################
#############################################################################
from concurrent import futures
from datetime import datetime
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from dateTimeUtil import DateTimeUtil
from dateutil import parser
from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from enum_util import AlarmKeys
from enum_util import ETLTopologyStatus
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils
from propertyFileUtil import PropertyFileUtil
from py4j.java_gateway import Py4JError, traceback
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

ALARM_KEY = AlarmKeys.MEDIATION_ALARM.value


def main():
    try:
        global gateway_obj
        gateway_obj = DbConnection().initialiseGateway()
        logger.info("Getting Topologies Info")
        topologies = get_topologies_info()
        thresholds = get_input_delay_threshold()
        topologies_details = get_toplogies_details(topologies)
        write_topologies_status_to_csv(topologies_details)
        push_to_maria_db(topologies_details)
        breached_info = get_breached_info(topologies_details, thresholds)
        send_alert(breached_info)
        logger.info("Writing & Inserting Topologies info is completed")
    except Py4JError:
        traceback.print_exc()
    finally:
        gateway_obj.shutdown()


def get_toplogies_details(topologies):
    logger.debug('Getting topoliges info from postgres')
    with futures.ThreadPoolExecutor() as executor:
        data = list(executor.map(parse_topology_info, topologies))
    return list(filter(None, data))


def get_topologies_info():
    topologies = [topology for topology in
                  XmlParserUtils.get_topologies_from_xml().keys()]
    sql_query = PropertyFileUtil('etl_topologies', 'PostgresSqlSection'
                                 ).getValueForKey()
    output = DbConnection().getConnectionAndExecuteSql(sql_query, "postgres")
    logger.debug('Fetched all topologies info from postgres.')
    info = [topology.split(',') for topology in output.split('\n')]
    return [topology for topology in info if topology[0] in topologies]


def get_input_delay_threshold():
    return {topology: info['DataArrivalCheckInMINs'] for topology, info in
            XmlParserUtils.get_topologies_from_xml().items()}


def parse_topology_info(topology):
    logger.debug('Parsing topogies info for : %s' % topology[0])
    name, partition = topology[0], topology[4]
    input_time, input_delay, output_time, output_delay, partition_info = get_time_and_delay(
        topology)
    if None not in (input_time, input_delay, output_time, output_delay):
        topology_details = {'Topology': name, 'Partition': partition,
                            'InputTime': input_time, 'InputDelay': input_delay,
                            'OutputTime': output_time, 'OutputDelay': output_delay,
                            'PartitionInfo': str(partition_info)}
        update_usage_stats(topology_details, topology)
        return topology_details


def get_time_and_delay(topology_info):
    file_status = gateway_obj.jvm.com.nokia.monitoring.hive.\
                    HdfsFileStatusManager("/etc/hadoop/conf/")
    import_path = '/mnt/staging/import/%s' % topology_info[1]
    us_path = '/ngdb/us/import/%s' % topology_info[2]
    input_time, input_delay = get_last_modification_time_and_delay(import_path, file_status)
    output_time, output_delay = get_last_modification_time_and_delay(us_path, file_status)
    partition_info = get_partition_details(us_path, file_status)
    return input_time, input_delay, output_time, output_delay, partition_info


def get_last_modification_time_and_delay(path, file_status):
    time_utc = file_status.getFileStatus(path)
    if time_utc == "Not Valid Path":
        return None, None
    time_local = DateTimeUtil().convertUtcToLocalTime(parser.parse(time_utc))
    delay = (datetime.now() - parser.parse(time_local))
    delay_in_mins = int(delay.total_seconds() / 60)
    return time_local, str(delay_in_mins)


def get_partition_details(path, file_status):
    partition_info = file_status.getPartitionDetails(path)
    if partition_info == "Not Valid Path":
        return "NULL"
    partition_time = DateTimeUtil.get_date_from_epoch(int(partition_info))
    return partition_time


def update_usage_stats(topology_details, topology_info):
    topology_details['UsageJob'] = topology_info[3]
    topology_details['UsageBoundary'] = topology_info[5]
    topology_details['UsageDelay'] = get_usage_delay(topology_info[5])


def get_usage_delay(boundary):
    if boundary != 'NULL':
        return '%.f' % ((datetime.now() - parser.parse(boundary)
                         ).total_seconds() / 60)
    else:
        return str(ETLTopologyStatus.DELAY_WHEN_BOUNDARY_NULL.value)


def write_topologies_status_to_csv(topologies_details):
    logger.debug('Writing data to csv')
    for topology_details in topologies_details:
        CsvUtil().writeDictToCsv('ETLTopologyStatus', topology_details)


def push_to_maria_db(topologies_details):
    logger.debug('Inserting data to db')
    topologies_details_json = str(topologies_details).replace("'", '"')
    DBUtil().jsonPushToMariaDB(topologies_details_json, "ETLTopologyStatus")


def get_breached_info(topologies_details, thresholds):
    breached_topologies = []
    for data in topologies_details:
        threshold, delay = thresholds[data['Topology']], data['InputDelay']
        if int(delay) >= int(threshold):
            logger.info("%s breached input delay threshold" % data['Topology'])
            info = {'Topology': data['Topology'],
                    'Last arrival time at Mediation': data['InputTime'],
                    'Delay in Minutes': delay,
                    'Threshold in Minutes': threshold}
            breached_topologies.append(info)
    return breached_topologies


def send_alert(breached_data):
    if breached_data:
        severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
        html = str(HtmlUtil().generateHtmlFromDictList(
            "Mediation not sending data", breached_data))
        EmailUtils().frameEmailAndSend("[{0} ALERT]:Mediation not sending data at {1}".format(# noqa: 501
            severity, datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, ALARM_KEY)
        SnmpAlarm.send_alarm(ALARM_KEY, breached_data)


if __name__ == '__main__':
    main()


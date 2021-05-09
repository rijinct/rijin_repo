'''
Created on 03-Sep-2020

@author: deerakum
'''
from concurrent import futures
from copy import deepcopy
from datetime import datetime
import itertools
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dateTimeUtil import DateTimeUtil
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
NGDB_PATH = '/ngdb/us/import/'
WORK_PATH = '/ngdb/us/work/'
ALARM_KEY = AlarmKeys.BACKLOG_ALARM.value


def main():
    global gateway_obj
    global backlog_manager_obj
    try:
        gateway_obj = DbConnection().initialiseGateway()
        backlog_manager_obj = gateway_obj.jvm.com.nokia.monitoring.hive.\
                    BacklogManager("/etc/hadoop/conf/")
        _execute()
    except Py4JError:
        logger.exception(traceback.print_exc())
    finally:
        gateway_obj.shutdown()


def _execute():
    raw_data = Collector().collect()
    logger.debug("Raw Data has %s", raw_data)
    processed_data = Processor(raw_data).process()
    Presenter(processed_data).present()


class Collector:

    def collect(self):
        return self._get_backlogs_data()

    def _get_backlogs_data(self):
        global topologies
        topologies = self._get_list_of_topologies()
        logger.info("Collecting Backlog Data for enabled Topologies %s",
                    topologies)
        with futures.ThreadPoolExecutor() as executor:
            return list(executor.map(self._get_backlogs_for_all, topologies))

    def _get_list_of_topologies(self):
        logger.debug("Getting Topologies Info")
        topologies = []
        data = XmlParserUtils.get_topologies_from_xml()
        for name, info in data.items():
            topologies.append({
                'name':
                name,
                'etl_input_dir':
                '%s%s' % (IMP_PATH, info['Input']),
                'usage_input_dir':
                '%s%s' % (NGDB_PATH, info['UsageInput']),
                'work_input_dir':
                '%s%s' % (WORK_PATH, info['UsageInput'])
            })
        return topologies

    def _get_backlogs_for_all(self, topology):
        logger.debug('Parsing topologies info for : %s' % topology['name'])
        data = self._get_backlogs_in_a_dir(**topology)
        return data

    def _get_backlogs_in_a_dir(self, name, etl_input_dir, usage_input_dir,
                               work_input_dir):
        result = []
        java_list_obj = gateway_obj.jvm.java.util.ArrayList()
        java_list_obj.append(etl_input_dir)
        java_list_obj.append(usage_input_dir)
        java_list_obj.append(work_input_dir)
        logger.debug("List Passing to java has %s", java_list_obj)
        backlog_counts = backlog_manager_obj.getFileCount(name, java_list_obj)
        logger.debug("Response from Java is %s ", backlog_counts)
        result.append(backlog_counts)
        return result


class Processor:

    def __init__(self, raw_data):
        self._raw_data = raw_data

    def process(self):
        final_list_dict = []
        flat_list_dict = list(itertools.chain.from_iterable(self._raw_data))
        for backlog_dict in flat_list_dict:
            for backlog_key in backlog_dict.keys():
                temp_dict = {}
                temp_dict['DataSource'] = backlog_key
                temp_dict['datFileCountMnt'] = backlog_dict.get(
                    backlog_key).get('datFileCountMnt')
                temp_dict['processingDatFileCountMnt'] = backlog_dict.get(
                    backlog_key).get('processingDatFileCountMnt')
                temp_dict['errorDatFileCountMnt'] = backlog_dict.get(
                    backlog_key).get('errorDatFileCountMnt')
                temp_dict['datFileCountNgdbUsage'] = backlog_dict.get(
                    backlog_key).get('datFileCountNgdbUsage')
                temp_dict['datFileCountNgdbWork'] = backlog_dict.get(
                    backlog_key).get('datFileCountNgdbWork')
                temp_dict['DateInLocalTZ'] = DateTimeUtil.get_utc_time()
                final_list_dict.append(temp_dict)
        logger.debug("Processed Data is %s ", final_list_dict)
        return eval(str(final_list_dict).replace("None", "-1"))


class Presenter:

    def __init__(self, processed_data):
        self._processed_data = processed_data

    def present(self):
        self._write_backlogs_status_to_csv()
        self._push_to_maria_db()
        breached_backlog = self._get_breached_backlog_info()
        self._send_alert(breached_backlog)

    def _write_backlogs_status_to_csv(self):
        logger.debug('Writing data to csv')
        local_data = deepcopy(self._processed_data)
        for backlog_details in local_data:
            backlog_details.pop('DateInLocalTZ')
            CsvUtil().writeDictToCsv('backlog', backlog_details)

    def _push_to_maria_db(self):
        logger.debug('Inserting data to db')
        backlogs_details_json = str(self._processed_data).replace("'", '"')
        DBUtil().jsonPushToMariaDB(backlogs_details_json, "backlog")

    def _get_breached_backlog_info(self):
        breached_info = []
        threshold_info = self._get_backlog_threshold()
        for backlog_dict in self._processed_data:
            breached_dict = self._check_breached_threshold(threshold_info,
                                                      backlog_dict)
            if breached_dict:
                breached_info.append(breached_dict)
        return breached_info

    def _check_breached_threshold(self, threshold_info, backlog_dict):
        temp_info_dict = {}
        count = 0
        etl_backlog, usage_backlog = int(threshold_info[
            backlog_dict['DataSource']]['ETLBacklogThreshold']), int(
                threshold_info[
                    backlog_dict['DataSource']]['UsageBacklogThreshold'])
        if int(backlog_dict['datFileCountMnt']) > etl_backlog:
            temp_info_dict["DAT Files @ {0}".format(IMP_PATH)] = str(
                backlog_dict['datFileCountMnt'])
            count = count + 1
        elif int(backlog_dict['processingDatFileCountMnt']) > etl_backlog:
            temp_info_dict["PROCESSING Files @ {0}".format(IMP_PATH)] = str(
                backlog_dict['processingDatFileCountMnt'])
            count = count + 1
        elif int(backlog_dict['errorDatFileCountMnt']) > etl_backlog:
            temp_info_dict["ERROR Files @ {0}".format(IMP_PATH)] = str(
                backlog_dict['errorDatFileCountMnt'])
            count = count + 1
        elif int(backlog_dict['datFileCountNgdbUsage']) > usage_backlog:
            temp_info_dict["DAT Files @ {0}".format(NGDB_PATH)] = str(
                backlog_dict['datFileCountNgdbUsage'])
            count = count + 1
        elif int(backlog_dict['datFileCountNgdbWork']) > usage_backlog:
            temp_info_dict["DAT Files @ {0}".format(WORK_PATH)] = str(
                backlog_dict['datFileCountNgdbWork'])
            count = count + 1
        if count >= 1:
            temp_info_dict['Topology'] = backlog_dict['DataSource']
        if len(temp_info_dict) > 0:
            return temp_info_dict
        else:
            return None

    def _get_input_dir(self, topology_name, dir_name):
        for topology_dict in topologies:
            if topology_dict.keys() == topology_name:
                return topology_dict[dir_name]

    def _get_backlog_threshold(self):
        return {
            topology: {
                'ETLBacklogThreshold': info['ETLBacklogThreshold'],
                'UsageBacklogThreshold': info['UsageBacklogThreshold']
            }
            for topology, info in
            XmlParserUtils.get_topologies_from_xml().items()
        }

    def _send_alert(self, breached_backlog):
        if breached_backlog:
            severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
            html = str(HtmlUtil().generateHtmlFromDictList(
                "Breached Backlog Info", breached_backlog))
            EmailUtils().frameEmailAndSend(
                "[{0} ALERT]: ETL_BACKLOG_HIGH at {1}".format(
                    severity,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                html, ALARM_KEY)
            SnmpAlarm.send_alarm(ALARM_KEY, breached_backlog)


if __name__ == '__main__':
    main()

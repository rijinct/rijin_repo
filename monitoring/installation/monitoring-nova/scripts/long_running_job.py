##############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 1
# Purpose: This script raises an alert for long running job
#
# Date:  29-09-2020
#############################################################################
#############################################################################
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from dateTimeUtil import DateTimeUtil
from enum_util import AlarmKeys, TypeOfUtils
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.framework.specification.defs import DBType
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def main():
    _execute()


def _execute():
    raw_data = Collector().collect()
    processed_data = Processor(raw_data).process()
    Presenter(processed_data).present()


class Collector:

    def collect(self):
        logger.info('Getting long running job data from postgres')
        startTimeStamp = DateTimeUtil.get_last_hour_date()
        childTags = XmlParserUtils.extract_child_tags('AGGREGATIONTHRESHOLD',
                                                      'property')
        prop = {
            property.attributes['Name'].value.lower() + '_long_duration':
            property.attributes['value'].value
            for property in childTags
        }
        prop['starts_time'] = startTimeStamp
        data = QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.LONG_RUNNING_JOB_QUERY, **prop)
        return data


class Processor:

    def __init__(self, raw_data):
        self._data = raw_data

    def process(self):
        process_data = []
        for jobs in self._data:
            duration = int(jobs[4])
            if duration < 60:
                duration = str(duration) + " mins"
            else:
                duration = str(round(duration / 60, 1)) + " hours"
            json_data = {
                'Type': jobs[0],
                'Job Name': jobs[1],
                'Start Time': str(jobs[2]),
                'End Time': str(jobs[3]),
                'Duration': duration
            }
            process_data.append(json_data)
        return process_data


class Presenter:
    ALARM_KEY = AlarmKeys.LONG_RUNNING_JOBS_ALARM.value
    TYPE_OF_UTIL = TypeOfUtils.LONG_RUNING_JOB_UTIL.value

    def __init__(self, data):
        self._data = data

    def present(self):
        if len(self._data) > 0:
            severity = SnmpAlarm.get_severity_for_email(Presenter.ALARM_KEY)
            self.__frame_html_and_send(self._data, severity)
            for data in self._data:
                CsvUtil().writeDictToCsv(Presenter.TYPE_OF_UTIL, data)

    def __frame_html_and_send(self, json_list_for_alert, severity):
        html = HtmlUtil().generateHtmlFromDictList("Long running Job",
                                                   json_list_for_alert)
        EmailUtils().frameEmailAndSend(
            "[{0} ALERT]: Long Running Jobs".format(severity), html,
            Presenter.ALARM_KEY)
        SnmpAlarm.send_alarm(Presenter.ALARM_KEY,
                             "Long Running Jobs: " + str(json_list_for_alert),
                             severity)


if __name__ == "__main__":
    main()

#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script checks jobs which are failed for the last
# 15 minutes and alerts the end users of monitoring
# Date:
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
#
#############################################################################
import sys, subprocess, datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from dateTimeUtil import DateTimeUtil
from dbConnectionManager import DbConnection
from enum_util import AlarmKeys
from generalPythonUtils import PythonUtils
from htmlUtil import HtmlUtil
from logger_util import *
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from writeCsvUtils import CsvWriter

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def checkFailedJobs(type):
        if type in ('15MIN', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'TNP', 'REAGG'):
            ALARM_KEY = AlarmKeys.AGGREGATION_JOBS_ALARM.value
        elif type == 'DIMENSION':
            ALARM_KEY = AlarmKeys.DIMENSION_JOBS_ALARM.value
        elif type == 'QS':
            ALARM_KEY = AlarmKeys.QS_JOBS_ALARM.value
        elif type == 'USAGE':
            ALARM_KEY = AlarmKeys.USAGE_JOBS_ALARM.value
        if type == '15MIN':
                aggJob = "%{0}_AggregateJob".format(type)
                expJob = "%{0}_ExportJob".format(type)
                startTimeStamp = DateTimeUtil.get_last_15min_date()
        elif type in ('HOUR', 'DAY', 'WEEK', 'MONTH'):
                aggJob = "%{0}_AggregateJob".format(type)
                expJob = "%{0}_ExportJob".format(type)
                startTimeStamp = DateTimeUtil.get_last_hour_date()
        elif type == 'USAGE':
                aggJob = "Usage%_LoadJob"
                expJob = "Usage%_LoadJob"
                startTimeStamp = DateTimeUtil.get_last_15min_date()
        elif type == 'TNP':
                aggJob = "PS_TNP%_KPIJob"
                expJob = "PS_TNP%_ThresholdJob"
                startTimeStamp = DateTimeUtil.get_last_15min_date()
        elif type == 'REAGG':
                aggJob = "%ReaggregateJob"
                expJob = "%ReaggregateJob"
                startTimeStamp = DateTimeUtil.get_last_hour_date()
        elif type == 'DIMENSION':
                aggJob = "%CorrelationJob"
                expJob = "%CorrelationJob"
                startTimeStamp = DateTimeUtil.get_last_hour_date()
        elif type == 'QS':
                aggJob = "%QSJob"
                expJob = "%QSJob"
                startTimeStamp = DateTimeUtil.get_last_15min_date()
        failedJobsCountquerySql = PropertyFileUtil('failedJobsCountQuery', 'PostgresSqlSection').getValueForKey()
        finalfailedJobsCountquerySql = failedJobsCountquerySql.replace("AggJob", str(aggJob)).replace("ExpJob", str(expJob)).replace("StartTime", str(startTimeStamp))
        failedJobsCountStatus = DbConnection().getConnectionAndExecuteSql(finalfailedJobsCountquerySql, "postgres", "COUNT_OF_ERROR_JOBS")
        if int(failedJobsCountStatus) > 0:
                failedErrorJobSql = PropertyFileUtil('failedErrorJobs', 'PostgresSqlSection').getValueForKey()
                finalfailedErrorJobSql = failedErrorJobSql.replace("AggJob", str(aggJob)).replace("ExpJob", str(expJob)).replace("StartTime", str(startTimeStamp))
                failedErrorStatus = DbConnection().getConnectionAndExecuteSql(finalfailedErrorJobSql, "postgres")
                for failedError in failedErrorStatus.split('\n'):
                        csvWriter = CsvWriter('JobsFailure', 'failedJobs_%s_%s.csv' % (type, datetime.date.today().strftime("%Y%m%d")), datetime.date.today().strftime("%Y-%m-%d") + ',' + failedError.strip())
                errorList = appendHeader(failedErrorStatus.split('\n'))
                frameEmailContent(errorList, ALARM_KEY)
                logger.debug('Failed Jobs are', errorList)
                exit(2)
        else:
                logger.info('No Failed Jobs')
                exit(0)


def appendHeader(failedErrorStatus):
        return ['Job Name,Start Time, End Time, Status, Error Description'] + failedErrorStatus


def frameEmailContent(errorList, ALARM_KEY):
        label = 'Below is the Failed Job information:'
        html = str(HtmlUtil().generateHtmlFromList(label, errorList))
        severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
        EmailUtils().frameEmailAndSend("[{0} ALERT]:Job(s) Failure Alert on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, ALARM_KEY)
        SnmpAlarm.send_alarm(ALARM_KEY, errorList)


def main():
        if len(sys.argv) > 1:
                checkFailedJobs(sys.argv[1])
        else:
                logger.exception('Incorrect Usage: Either no Arguments passed nor argument passed is incorrect, so exiting..')


if __name__ == "__main__":
        main()

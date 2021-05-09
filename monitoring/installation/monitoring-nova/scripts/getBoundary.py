#############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:Monitoring Team
# Version: 0.1
# Purpose:To check the Usage and Perf Job Boundary
#
# Date:    26-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

from collections import OrderedDict
import sys, datetime, subprocess, os, csv
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dbConnectionManager import DbConnection
from enum_util import AlarmKeys
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

ALARM_KEY = AlarmKeys.DELAYED_USAGE_JOBS_ALARM.value

### Variables assignment ###

curDate = datetime.date.today()
curDateTime = datetime.datetime.now()
hr_agg_date_hr = subprocess.getoutput('date +"%Y-%m-%d %H"')
postgres_To_hr_agg_range= curDateTime.hour
postgres_From_hr_agg_range = (curDateTime - datetime.timedelta(hours=1)).hour
postgres_Day_agg_date =  (curDate - datetime.timedelta(days = 1)).day
postgres_From_week_date = (curDate - datetime.timedelta(days = curDate.weekday()+7) if curDate.weekday() == 0 else curDate - datetime.timedelta(days = curDate.weekday()))
postgres_From_week_day = postgres_From_week_date.day
postgres_To_week_agg_date =  (curDate - datetime.timedelta(days = curDate.weekday()+1))
postgres_To_week_day = postgres_To_week_agg_date.day
postgres_From_week_month = postgres_From_week_date.strftime("%B")
postgres_To_week_month =  postgres_To_week_agg_date.strftime("%B")

def boundaryCheck(type):
        if type == 'USAGE':
                UsageBoundary = getUsageBoundary()
        elif type == '15MIN':
                fifMinBoundary = get15MinBoundary()
        elif type == 'HOUR':
                HourBoundary = getHourBoundary()
        elif type == 'DAY':
                DayBoundary = getDayBoundary()
        elif type == 'WEEK':
                WeekBoundary = getWeekBoundary()
        elif type == 'REAGG':
                ReaggBoundary = getReaggBoundary()

def getThreshold(type):
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName('USAGETHRESHOLD')[0].getElementsByTagName('property')
        return int([val.attributes['value'].value for val in elements if val.attributes['Name'].value==type][0])

def jobEnabled(usageJob):
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('DelayedUsageJobs')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyNum in range(len(propertyTag)):
            if usageJob.split(",")[1] == propertyTag[propertyNum].attributes['Name'].value:
                enabledValue = propertyTag[propertyNum].attributes['enabled'].value
                if enabledValue.lower() == "no":
                    return 0
                else:
                    return 1


def checkUsageThreshold(usageJobs):
        logger.debug('Usage Jobs in function is',usageJobs)
        dayBoundaryAdapJobs = ['Usage_TT_1_LoadJob','Usage_BCSI_1_LoadJob','Usage_BB_THRPT_1_LoadJob']
        todayDate = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        breachedUsageJobs = ['Delayed Usage Jobs']
        usageJobsList = usageJobs.split("\n")
        for usageJob in usageJobsList:
            if jobEnabled(usageJob):
                logger.debug('job is enabled',usageJob)
                maxValue = usageJob.split(",")[2].strip()
                if maxValue and maxValue !="NULL" and usageJob.split(",")[1].strip() in dayBoundaryAdapJobs:
                        if maxValue != todayDate:
                                breachedUsageJobs.append(usageJob.split(",")[1].strip())
                        else:
                                logger.debug('Job {0} is running on time'.format(usageJob.split(",")[1].strip()))
                elif maxValue and maxValue != "NULL":
                        dateDiff = (datetime.datetime.now() - datetime.datetime.strptime(maxValue, "%Y-%m-%d %H:%M:%S"))
                        dateDiff = str(dateDiff).split(",")
                        if len(dateDiff) > 1:
                                day= dateDiff[0]
                                time=dateDiff[1]
                        else:
                                day = '0'
                                time=dateDiff[0]
                        mins = int(day.strip('day').strip('days')) * 24 * 60 + int(time.split(":")[0]) * 60 + int(time.split(":")[1])
                        if mins > getThreshold("USAGE"):
                                breachedUsageJobs.append(usageJob.split(",")[1].strip())
                else:
                        logger.debug("This job %s is not handled" , usageJob.split(",")[1].strip())
            else:
                logger.debug("Job '%s' is not enabled",usageJob)
        if len(breachedUsageJobs) > 1:
                logger.info("Breached Usage jobs list: '%s' ",breachedUsageJobs)
                severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
                html = HtmlUtil().generateHtmlFromList('Below are the Delayed Usage Jobs', breachedUsageJobs)
                EmailUtils().frameEmailAndSend('[{0} ALERT] Delayed Usage Job(s) list {1}'.format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, ALARM_KEY)
                SnmpAlarm.send_alarm(ALARM_KEY, breachedUsageJobs)


def getUsageBoundary():
        usageBoundaryquerySql = PropertyFileUtil('usageBoundaryquery','PostgresSqlSection').getValueForKey()
        finalusageBoundaryquerySql = usageBoundaryquerySql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range))
        checkUsageThreshold(CsvUtil().executeAndWriteToCsv('UsageBoundary',finalusageBoundaryquerySql))


def get15MinBoundary():
        fifminBoundaryquerySql = PropertyFileUtil('fifminBoundaryquery','PostgresSqlSection').getValueForKey()
        finalfifminBoundaryquerySql = fifminBoundaryquerySql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range)).replace("to_hr_agg_range",str(postgres_To_hr_agg_range))
        CsvUtil().executeAndWriteToCsv('15minBoundary',finalfifminBoundaryquerySql)


def getHourBoundary():
        hourBoundaryquerySql = PropertyFileUtil('hourBoundaryquery','PostgresSqlSection').getValueForKey()
        finalhourBoundaryquerySql = hourBoundaryquerySql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range)).replace("to_hr_agg_range",str(postgres_To_hr_agg_range))
        CsvUtil().executeAndWriteToCsv('hourBoundary',finalhourBoundaryquerySql)


def getDayBoundary():
        dayBoundaryquerySql = PropertyFileUtil('dayBoundaryquery','PostgresSqlSection').getValueForKey()
        finaldayBoundaryquerySql = dayBoundaryquerySql.replace("day_agg_date",str(postgres_Day_agg_date))
        CsvUtil().executeAndWriteToCsv('dayBoundary',finaldayBoundaryquerySql)


def getWeekBoundary():
        weekBoundaryquerySql = PropertyFileUtil('weekBoundaryquery','PostgresSqlSection').getValueForKey()
        finalweekBoundaryquerySql = weekBoundaryquerySql.replace("from_week_day",str(postgres_From_week_day)).replace("from_week_month",str(postgres_From_week_month)).replace("to_week_day",str(postgres_To_week_day)).replace("to_week_month",str(postgres_To_week_month))
        CsvUtil().executeAndWriteToCsv('weekBoundary',finalweekBoundaryquerySql)


def getReaggBoundary():
        reaggBoundaryquerySql = PropertyFileUtil('reaggBoundaryquery','PostgresSqlSection').getValueForKey()
        finalreaggBoundaryquerySql = reaggBoundaryquerySql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range)).replace("to_hr_agg_range",str(postgres_To_hr_agg_range))
        CsvUtil().executeAndWriteToCsv('reaggBoundary',finalreaggBoundaryquerySql)


try:
        type = sys.argv[1]
except IndexError:
        logger.exception('Please pass a valid argument')
        logger.exception('[USAGE: python getBoundary.py USAGE/15MIN/HOUR/DAY/WEEK/REAGG]')
        exit(1)
boundaryCheck(type)

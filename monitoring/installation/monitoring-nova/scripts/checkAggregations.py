#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose:
#
# Date:    02-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################
from collections import OrderedDict
import sys, subprocess, datetime, os, csv
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dbConnectionManager import DbConnection
from enum_util import AlarmKeys
from generalPythonUtils import PythonUtils
from htmlUtil import HtmlUtil
from logger_util import *
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from writeCsvUtils import CsvWriter

htmlUtil = HtmlUtil()
ALARM_KEY = AlarmKeys.LONG_RUNNING_JOBS_ALARM.value

severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

### Variables assignment ###

curDate = datetime.date.today()
curDateForFile = curDate.strftime("%Y%m%d")
curDateTime = datetime.datetime.now()
hr_agg_date_hr = subprocess.getoutput('date +"%Y-%m-%d %H"')
day_agg_date_min = datetime.datetime.combine(curDate, datetime.time.min)
day_agg_date_max = datetime.datetime.combine(curDate, datetime.time.max)
postgres_To_hr_agg_range= curDateTime.hour
postgres_From_hr_agg_range = (curDateTime - datetime.timedelta(hours=1)).hour
postgres_Day_agg_date =  (curDate - datetime.timedelta(days = 1)).day
postgres_From_week_agg_range = (curDate - datetime.timedelta(days = curDate.weekday()+7) if curDate.weekday() == 0 else curDate - datetime.timedelta(days = curDate.weekday())).day
postgres_To_week_agg_range =  (curDate - datetime.timedelta(days = curDate.weekday()+1)).day
aggregationFolder = "/opt/nsn/ngdb/monitoring/output/jobStatus/"


def getThresholdValues():
        global thresholdValue
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName('AGGREGATIONTHRESHOLD')[0].getElementsByTagName('property')
        thresholdValue = OrderedDict(list(zip([val.attributes['Name'].value for val in elements],[int(val.attributes['value'].value) for val in elements])))

def buildSummaryOfHourAggregations(hourAggregations):
        logger.info("Building Summary of Hour Aggregations")
        failedHourAgg = 0
        totalHourAgg = 0
        runningHourAgg = 0
        waitingHourAgg = 0
        if hourAggregations is not None:
            if len(hourAggregations.split("\n")) > 1:
                totalHourAgg = len(hourAggregations.split("\n"))
                for hourAggregation in hourAggregations.split("\n")[1:]:
                        if 'E' == hourAggregation.split(",")[5].strip():
                                failedHourAgg = failedHourAgg + 1
                        elif 'R' == hourAggregation.split(",")[5].strip():
                                runningHourAgg = runningHourAgg + 1
                        elif 'W' == hourAggregation.split(",")[5].strip():
                                waitingHourAgg = waitingHourAgg + 1
                        else:
                                logger.debug('Job is in Success State')
                successfulHourAgg = totalHourAgg - (failedHourAgg + runningHourAgg + waitingHourAgg)
                sendHourAggAlert(totalHourAgg,failedHourAgg,runningHourAgg,waitingHourAgg,successfulHourAgg)


def sendHourAggAlert(totalHourAgg,failedHourAgg,runningHourAgg,waitingHourAgg,successfulHourAgg):
        hourAggSummary = ["Type of Aggregations,Count of Aggregations Jobs","Total Hour Aggregations Executed,{0}".format(totalHourAgg),"Failed Hour Aggregations,{0}".format(failedHourAgg),"Running Hour Aggregations,{0}".format(runningHourAgg),"Waiting Hour Aggregations,{0}".format(waitingHourAgg),"Successful Hour Aggregations,{0}".format(successfulHourAgg)]
        html = htmlUtil.generateHtmlFromList("Below is the Hour Aggregation status",hourAggSummary)
        EmailUtils().frameEmailAndSend("Hour Aggregation status for the hour {0}".format(postgres_From_hr_agg_range),html)

def aggregationsCheck(type):
        if type == 'HOUR':
                hourAggregations = getHourAggregations()
                buildSummaryOfHourAggregations(hourAggregations)
        elif type == 'DAY':
                dayAggregations = getDayAggregations()
        elif type == 'WEEK':
                weekAggregations = getWeekAggregations()
        elif type == 'MONTH':
                monthAggregations = getMonthAggregations()
        elif type == '15MIN':
                fifminAggregations = get15MinAggregations()
        else: getSummary(type)

def getHourAggregations():
        hourAggregationsSql = PropertyFileUtil('hourAggregationquery','PostgresSqlSection').getValueForKey()
        finalhourAggregationsSql = hourAggregationsSql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range)).replace("to_hr_agg_range",str(postgres_To_hr_agg_range)).replace("hr_agg_date",str(hr_agg_date_hr)).replace("hr_agg_date",str(hr_agg_date_hr))
        CsvUtil().executeAndWriteToCsv('hourJobStatus',finalhourAggregationsSql)

def getDayAggregations():
        dayAggregationsSql = PropertyFileUtil('dayAggregationquery','PostgresSqlSection').getValueForKey()
        finaldayAggregationsSql = dayAggregationsSql.replace("day_agg_date",str(postgres_Day_agg_date)).replace("day_agg_min",str(day_agg_date_min)).replace("day_agg_max",str(day_agg_date_max))
        CsvUtil().executeAndWriteToCsv('dayJobStatus',finaldayAggregationsSql)

def getWeekAggregations():
        weekAggregationsSql = PropertyFileUtil('weekAggregationquery','PostgresSqlSection').getValueForKey()
        finalweekAggregationsSql = weekAggregationsSql.replace("from_week_agg_range",str(postgres_From_week_agg_range)).replace("to_week_agg_range",str(postgres_To_week_agg_range)).replace("day_agg_date_min",str(day_agg_date_min)).replace("day_agg_date_max",str(day_agg_date_max))
        CsvUtil().executeAndWriteToCsv('weekJobStatus',finalweekAggregationsSql)

def getMonthAggregations():
        monthAggregationsSql = PropertyFileUtil('monthAggregationquery','PostgresSqlSection').getValueForKey()
        finalmonthAggregationsSql = monthAggregationsSql.replace("day_agg_min",str(day_agg_date_min)).replace("day_agg_max",str(day_agg_date_max))
        CsvUtil().executeAndWriteToCsv('monthJobStatus',finalmonthAggregationsSql)

def get15MinAggregations():
        fifMinAggregationsSql = PropertyFileUtil('15MinAggregationquery','PostgresSqlSection').getValueForKey()
        final15MinAggregationsSql = fifMinAggregationsSql.replace("from_hr_agg_range",str(postgres_From_hr_agg_range)).replace("to_hr_agg_range",str(postgres_To_hr_agg_range)).replace("hr_agg_date_hr",str(hr_agg_date_hr)).replace("hr_agg_date_hr",str(hr_agg_date_hr))
        CsvUtil().executeAndWriteToCsv('15MinJobStatus',final15MinAggregationsSql)

def getSummary(type):
        if type == 'DAYSUMMARY':
                daySummary = dayLastJobSummary()
                if len(daySummary) >= 1:
                        checkFileExistence(aggregationFolder+'dayJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                        lastJobNameList = getLastJobName(aggregationFolder+'dayJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv', type)
                        output = daySummary + "," +",".join(lastJobNameList)
                        CsvUtil().writeToCsv('daySummary',output)
                else: logger.info('No Day Jobs Found')

        elif type == 'HOURSUMMARY':
                hourSummary = hourJobCountSummary()
                if len(hourSummary) >= 1:
                        checkFileExistence(aggregationFolder+'hourJobStatus_' + str(curDate) + '.csv')
                        lastJobNameList = getLastJobName(aggregationFolder+'hourJobStatus_' + str(curDate)+'.csv', type)
                        output = hourSummary + "," +",".join(lastJobNameList)
                        CsvUtil().writeToCsv('hourSummary',output)
                else: logger.info('No Hour Jobs')

        elif type == 'WEEKSUMMARY':
                weekSummary = weekJobCountSummary()
                if len(weekSummary) >= 1:
                        checkFileExistence(aggregationFolder+'weekJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                        lastJobNameList = getLastJobName(aggregationFolder+'weekJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv', type)
                        output = weekSummary + "," +",".join(lastJobNameList)
                        CsvUtil().writeToCsv('weekSummary',output)
                else: logger.info('No Week Jobs')

        elif type == '15MINSUMMARY':
                fifMinSummary = fiFMinJobCountSummary()
                if len(fifMinSummary) >= 1:
                        checkFileExistence(aggregationFolder+'15MinJobStatus_' + str(curDateForFile) + '.csv')
                        lastJobNameList = getLastJobName(aggregationFolder+'15MinJobStatus_' + str(curDateForFile) + '.csv', type)
                        output = fifMinSummary + "," +",".join(lastJobNameList)
                        CsvUtil().writeToCsv('fifMinSummary',output)
                else: logger.info('No 15Min Jobs')

        else: getLongRunningJobs(type)

def dayLastJobSummary():
        dayLastJobSummarySql = PropertyFileUtil('dayLastJobSummaryQuery','PostgresSqlSection').getValueForKey()
        finaldayLastJobSummarySql = dayLastJobSummarySql.replace("day_agg_min",str(day_agg_date_min)).replace("day_agg_max",str(day_agg_date_max))
        dayLastJobSummaryStatus = DbConnection().getConnectionAndExecuteSql(finaldayLastJobSummarySql,"postgres")
        return dayLastJobSummaryStatus

def hourJobCountSummary():
        hourLastJobSummarySql = PropertyFileUtil('hourLastJobSummaryQuery','PostgresSqlSection').getValueForKey()
        finalhourLastJobSummarySql = hourLastJobSummarySql.replace("hr_agg_date",str(hr_agg_date_hr)).replace("hr_agg_date",str(hr_agg_date_hr))
        hourLastJobSummaryStatus=DbConnection().getConnectionAndExecuteSql(finalhourLastJobSummarySql,"postgres")
        return hourLastJobSummaryStatus

def weekJobCountSummary():
        weekLastJobSummarySql = PropertyFileUtil('weekLastJobSummaryQuery','PostgresSqlSection').getValueForKey()
        finalweekLastJobSummarySql = weekLastJobSummarySql.replace("day_agg_min",str(day_agg_date_min)).replace("day_agg_max",str(day_agg_date_max))
        weekLastJobSummaryStatus=DbConnection().getConnectionAndExecuteSql(finalweekLastJobSummarySql,"postgres")
        return weekLastJobSummaryStatus

def fiFMinJobCountSummary():
        fiFMinLastJobSummarySql = PropertyFileUtil('fiFMinLastJobSummaryQuery','PostgresSqlSection').getValueForKey()
        finalfiFMinLastJobSummarySql = fiFMinLastJobSummarySql.replace("hr_agg_date",str(hr_agg_date_hr)).replace("hr_agg_date",str(hr_agg_date_hr))
        fiFMinLastJobSummaryStatus=DbConnection().getConnectionAndExecuteSql(finalfiFMinLastJobSummarySql,"postgres")
        return fiFMinLastJobSummaryStatus

def getLastJobName(filename, type):
        if type == 'DAYSUMMARY':
                cmd = 'grep -A1 "^[A-Z]" {0} | grep -v Time |awk -F"," \'{{print $2}}\' |tail -1'.format(filename)
        elif type == 'WEEKSUMMARY':
                cmd = 'grep -A1 "^[A-Z]" {0} | grep -v Time |awk -F"," \'{{print $2}}\' |tail -1'.format(filename)
        else:
                cmd = 'grep -A1 "^[A-Z]" {0} | grep -v Time |awk -F"," \'{{print $2}}\' | tail -1'.format(filename)
        return subprocess.getoutput(cmd).split()

def getLongRunningJobs(type):
        if type == 'LONGDAY':
                getLongRunDay(type)
        elif type == 'LONGHOUR':
                getLongRunHour(type)
        elif type == 'LONGWEEK':
                getLongRunWeek(type)
        elif type == 'LONGMONTH':
                getLongRunMonth(type)
        elif type == 'LONG15MIN':
                getLongRun15Min(type)

def getLongRunDay(type):
        filename = aggregationFolder + 'dayJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Day'])
        cmdexport = "tac {0} | sed -n '1,/^Time/p' |grep -i \"HIVETOHBASELOADER\" |awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['ExportDay'])
        exportJobs = subprocess.getoutput(cmdexport).split('\n')[::-1]
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Day'])
        stillRunningExportJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' |grep -i \"HIVETOHBASELOADER\"| awk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['ExportDay'])
        if len(exportJobs) > 1:
                recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs + exportJobs + stillRunningExportJobs
        else:
                recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                dayLongJobs = [OrderedDict(list(zip(recordList[0].strip().split(','), item.strip().split(',')))) for item in recordList[1:]]
                writeToCsv(dayLongJobs, aggregationFolder, 'dayLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[{0} ALERT]:Long Running Day Jobs on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Day Jobs')
        else: logger.info('No long running jobs found for DAY')

def getLongRunHour(type):
        filename = aggregationFolder + 'hourJobStatus_' + str(curDate) + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Hour'])
        cmdexport = "tac {0} | sed -n '1,/^Time/p' |grep -i \"HIVETOHBASELOADER\" |awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['ExportHour'])
        exportJobs = subprocess.getoutput(cmdexport).split('\n')[::-1]
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Hour'])
        stillRunningExportJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' |grep -i \"HIVETOHBASELOADER\"| awk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['ExportHour'])
        if len(exportJobs) > 1:
                recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs + exportJobs + stillRunningExportJobs
        else:
                recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                hourLongJobs = [OrderedDict(list(zip(recordList[0].strip().split(','), item.strip().split(',')))) for item in recordList[1:]]
                writeToCsv(hourLongJobs, aggregationFolder, 'hourLongRunningJobs_' + str(curDate) + '_Allhours.csv')
                sendMail("[{0} ALERT]:Long Running Hour Jobs on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Hour Jobs')
        else: logger.info('No long running jobs found for HOUR')

def getLongRunWeek(type):
        filename = aggregationFolder + 'weekJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Week'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Week'])
        recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                weekLongJobs = [OrderedDict(list(zip(recordList[0].strip().split(','), item.strip().split(',')))) for item in recordList[1:]]
                writeToCsv(weekLongJobs, aggregationFolder, 'weekLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[{0} ALERT]:Long Running Week Jobs on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Week Jobs')
        else: logger.info('No long running jobs found for WEEK')

def getLongRunMonth(type):
        filename = aggregationFolder + 'monthJobStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($4>{1}) print $0}}'".format(filename, thresholdValue['Month'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($4 == \"\") print $0}}'".format(filename), thresholdValue['Month'])
        recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                monthLongJobs = [OrderedDict(list(zip(recordList[0].strip().split(','), item.strip().split(',')))) for item in recordList[1:]]
                writeToCsv(monthLongJobs, aggregationFolder, 'monthLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[{0} ALERT]:Long Running Month Jobs on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Month Jobs')
        else: logger.info('No long running jobs found for MONTH')

def getLongRun15Min(type):
        filename = aggregationFolder + '15MinJobStatus_' + str(curDateForFile) + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['15Min'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^Time/p' | awk -F\",\" '{{ if($5 == \"\") print $0}}'".format(filename), thresholdValue['15Min'])
        recordList =  subprocess.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                fifMinLongJobs = [OrderedDict(list(zip(recordList[0].strip().split(','), item.strip().split(',')))) for item in recordList[1:]]
                writeToCsv(fifMinLongJobs, aggregationFolder, '15MinJobStatus_' + str(curDateForFile) + '.csv')
                sendMail("[{0} ALERT]:Long Running 15 Min Jobs on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running 15 Min Jobs')
        else: logger.info('No long running jobs found for 15 MIN')

def getStillRunningJobs(cmd, threshold):
        jobsUnderExecution = []
        output = subprocess.getoutput(cmd)

        if len(output) > 0:
                recordList = output.split('\n')
                for record in recordList:
                        startTime = datetime.datetime.strptime(record.split(',')[2], '%Y-%m-%d %H:%M:%S')
                        execDuration = (curDateTime - startTime).total_seconds()/60.0
                        if execDuration > threshold:
                                jobsUnderExecution.append(record)
        return jobsUnderExecution

def checkFileExistence(fileName):
        if not os.path.exists(fileName):
                logger.info("File '%s' doesn't exist ", fileName)
                exit(0)

def sendMail(label,logs,summary=None):
        logger.info("Sending Email Alert '%s' ", summary)
        html = htmlUtil.generateHtmlFromList(summary,logs)
        EmailUtils().frameEmailAndSend(label,html)
        SnmpAlarm.send_alarm(ALARM_KEY, logs)


def writeToCsv(log, filePath, fileName):
        logger.info("Writing to csv '%s'", fileName)
        if not os.path.exists(filePath):
                subprocess.getoutput('mkdir -p %s' %(filePath))
        with open(filePath+fileName, 'a') as f:
                writer = csv.DictWriter(f, list(log[0].keys()))
                fields = list(log[0].keys())
                writer.writerow(dict(list(zip(fields, fields))))
                writer.writerows(log)

try:
        type = sys.argv[1]
except IndexError:
        logger.exception('Please pass a valid argument')
        logger.exception('[USAGE1: python checkAggregations.py 15MIN/HOUR/DAY/WEEK/MONTH]')
        logger.exception('[USAGE2: python checkAggregations.py 15MINSUMMARY/HOURSUMMARY/DAYSUMMARY/WEEKSUMMARY]')
        logger.exception('[USAGE3: python checkAggregations.py LONG15MIN/LONGHOUR/LONGDAY/LONGWEEK/LONGMONTH]')

getThresholdValues()
aggregationsCheck(type)

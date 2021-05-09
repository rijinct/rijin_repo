#                           Monitor
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

import sys, datetime, commands, os, csv
from collections import OrderedDict
from xml.dom import minidom
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
from htmlUtil import HtmlUtil
from snmpUtils import SnmpUtils
htmlUtil = HtmlUtil()

### Variables assignment ###

curDate = datetime.date.today()
curDateTime = datetime.datetime.now()
hourAggregationLogs="/var/local/monitoring/output/aggregations_Hour/"
dayAggregationLogs="/var/local/monitoring/output/aggregations_Day/"
weekAggregationLogs="/var/local/monitoring/output/aggregations_Week/"
monthAggregationLogs="/var/local/monitoring/output/aggregations_Month/"
fifMinAggregationLogs="/var/local/monitoring/output/aggregations_15Min/"
hr_agg_date_hr = commands.getoutput('date +"%Y-%m-%d %H"')
day_agg_date_min = datetime.datetime.combine(curDate, datetime.time.min)
day_agg_date_max = datetime.datetime.combine(curDate, datetime.time.max)
postgres_To_hr_agg_range= curDateTime.hour
postgres_From_hr_agg_range = (curDateTime - datetime.timedelta(hours=1)).hour
postgres_Day_agg_date =  (curDate - datetime.timedelta(days = 1)).day
postgres_From_week_agg_range = (curDate - datetime.timedelta(days = curDate.weekday()+7) if curDate.weekday() == 0 else curDate - datetime.timedelta(days = curDate.weekday())).day
postgres_To_week_agg_range =  (curDate - datetime.timedelta(days = curDate.weekday()+1)).day

def getThresholdValues():
        global thresholdValue
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName('AGGREGATIONTHRESHOLD')[0].getElementsByTagName('property')
        thresholdValue = OrderedDict(zip([val.attributes['Name'].value for val in elements],[int(val.attributes['value'].value) for val in elements]))

def buildSummaryOfHourAggregations(hourAggregations):
        failedHourAgg = 0
        totalHourAgg = 0
        runningHourAgg = 0
        waitingHourAgg = 0
        if len(hourAggregations.split("\n")) > 1:
                totalHourAgg = len(hourAggregations.split("\n"))
                for hourAggregation in hourAggregations.split("\n")[1:]:
                        if 'E' == hourAggregation.split("|")[5].strip():
                                failedHourAgg = failedHourAgg + 1
                        elif 'R' == hourAggregation.split("|")[5].strip():
                                runningHourAgg = runningHourAgg + 1
                        elif 'W' == hourAggregation.split("|")[5].strip():
                                waitingHourAgg = waitingHourAgg + 1
                        else:
                                print 'Job is in Success State'
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
                fileName = 'hourJobsStatus_' + str(curDate) + '_Allhours.csv'
                writeToCsv(getFormattedDictList(hourAggregations, type), hourAggregationLogs, fileName)
        elif type == 'DAY':
                dayAggregations = getDayAggregations()
                fileName = 'dayJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
                writeToCsv(getFormattedDictList(dayAggregations, type), dayAggregationLogs, fileName)
        elif type == 'WEEK':
                weekAggregations = getWeekAggregations()
                fileName = 'weekJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m')  + '.csv'
                writeToCsv(getFormattedDictList(weekAggregations, type), weekAggregationLogs, fileName)
        elif type == 'MONTH':
                monthAggregations = getMonthAggregations()
                fileName = 'monthJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m')  + '.csv'
                writeToCsv(getFormattedDictList(monthAggregations, type), monthAggregationLogs, fileName)
        elif type == '15MIN':
                fifminAggregations = get15MinAggregations()
                fileName = 'fifMinJobsStatus_' + str(curDate) + '.csv'
                writeToCsv(getFormattedDictList(fifminAggregations, type), fifMinAggregationLogs, fileName)
        else: getSummary(type)

def getHourAggregations():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Hour\'||\'===\'||{2}||\'Hour\' as Time_Frame,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >\'{3}:00:00\' and start_time <\'{4}:59:59\' and job_name like\'%HOUR_AggregateJob\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range, postgres_To_hr_agg_range, hr_agg_date_hr, hr_agg_date_hr)
        return commands.getoutput(cmd).strip()

def getDayAggregations():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1} as Day,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >\'{2}\' and start_time <\'{3}\' and job_name like \'%DAY_AggregateJob\' or job_name like \'%DAY_ExportJob\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_Day_agg_date, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def getWeekAggregations():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Week\'||\'===\'||{2}||\'Week\' as Week,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >\'{3}\' and start_time <\'{4}\' and job_name like\'%WEEK_AggregateJob\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_week_agg_range, postgres_To_week_agg_range, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def getMonthAggregations():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where status=\'S\' and start_time >\'{1}\' and start_time <\'{2}\' and job_name like\'%MONTH_AggregateJob\' order by executionduration  desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def get15MinAggregations():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Hour\'||\'===\'||{2}||\'Hour\' as Time_Frame,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >\'{3}:00:00\' and start_time <\'{4}:59:59\' and job_name like\'%15MIN_AggregateJob\' order by executionduration  desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range, postgres_To_hr_agg_range, hr_agg_date_hr, hr_agg_date_hr)
        return commands.getoutput(cmd).strip()

def getSummary(type):
        if type == 'DAYSUMMARY':
                daySummary = dayLastJobSummary()
                if len(daySummary.split("\n")) > 1:
                        fileName = 'daySummary_' + str(curDate) + '_Allday.csv'
                        checkFileExistence(dayAggregationLogs+'dayJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                        lastJobNameList = getLastJobName(dayAggregationLogs+'dayJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv', type)
                        formattedDict = getFormattedDictList(daySummary, type, lastJobNameList)
                        writeToCsv(formattedDict, dayAggregationLogs, fileName)
                else: print 'No Day Jobs Found'

        elif type == 'HOURSUMMARY':
                hourSummary = hourJobCountSummary()
                if len(hourSummary.split("\n")) > 1:
                        fileName = 'hourSummary_' + str(curDate) + '_Allhours.csv'
                        checkFileExistence(hourAggregationLogs+'hourJobsStatus_' + str(curDate) + '_Allhours.csv')
                        lastJobNameList = getLastJobName(hourAggregationLogs+'hourJobsStatus_' + str(curDate) + '_Allhours.csv', type)
                        formattedDict = getFormattedDictList(hourSummary, type, lastJobNameList)
                        writeToCsv(formattedDict, hourAggregationLogs, fileName)
                else: print 'No Hour Jobs'

        elif type == 'WEEKSUMMARY':
                weekSummary = weekJobCountSummary()
                if len(weekSummary.split("\n")) > 1:
                        fileName = 'weekSummary_' + str(curDate) + '_AllWeek.csv'
                        checkFileExistence(weekAggregationLogs+'weekJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                        lastJobNameList = getLastJobName(weekAggregationLogs+'weekJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv', type)
                        formattedDict = getFormattedDictList(weekSummary, type, lastJobNameList)
                        writeToCsv(formattedDict, weekAggregationLogs, fileName)
                else: print 'No Week Jobs'

        elif type == '15MINSUMMARY':
                fifMinSummary = fiFMinJobCountSummary()
                if len(fifMinSummary.split("\n")) > 1:
                        fileName = '15MinJobsSummary_' + str(curDate) + '_Allhours.csv'
                        checkFileExistence(fifMinAggregationLogs+'fifMinJobsStatus_' + str(curDate) + '.csv')
                        lastJobNameList = getLastJobName(fifMinAggregationLogs+'fifMinJobsStatus_' + str(curDate) + '.csv', type)
                        formattedDict = getFormattedDictList(fifMinSummary, type, lastJobNameList)
                        writeToCsv(formattedDict, fifMinAggregationLogs, fileName)
                else: print 'No 15Min Jobs'

        else: getLongRunningJobs(type)

def dayLastJobSummary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select extract(day from start_time) as DAY, max(end_time) as DAY_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = \'00:00:00\' then job_name end) as ZERO_DURATION, COUNT(case when status = \'S\' then job_name end) as SUCCESS, COUNT(case when status = \'E\' then job_name end) as ERROR, COUNT(case when status = \'R\' then job_name end) as RUNNING from sairepo.etl_status where (job_name like \'%DAY%Aggre%\' or job_name like \'%DAY_ExportJob\') and start_time >\'{1}\' and start_time <\'{2}\' group by extract(day from start_time) order by extract(day from start_time);\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def hourJobCountSummary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select extract(hour from start_time) as HOUR, max(end_time) as HOUR_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = \'00:00:00\' then job_name end) as ZERO_DURATION, COUNT(case when status = \'S\' then job_name end) as SUCCESS, COUNT(case when status = \'E\' then job_name end) as ERROR, COUNT(case when status = \'R\' then job_name end) as RUNNING from sairepo.etl_status where (job_name like \'%HOUR%Aggre%\' or job_name like \'%HOUR_ExportJob\') and start_time >\'{1}\' and start_time <\'{2}\' group by extract(hour from start_time) order by extract(hour from start_time);\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def weekJobCountSummary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select extract(week from start_time) as WEEK, max(end_time) as WEEK_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = \'00:00:00\' then job_name end) as ZERO_DURATION, COUNT(case when status = \'S\' then job_name end) as SUCCESS, COUNT(case when status = \'E\' then job_name end) as ERROR, COUNT(case when status = \'R\' then job_name end) as RUNNING from sairepo.etl_status where job_name like \'%WEEK_AggregateJob%\' and start_time >\'{1}\' and start_time <\'{2}\' group by extract(week from start_time) order by extract(week from start_time);\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def fiFMinJobCountSummary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select extract(hour from start_time) as hour, max(end_time) as FMIN_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = \'00:00:00\' then job_name end) as ZERO_DURATION, COUNT(case when status = \'S\' then job_name end) as SUCCESS, COUNT(case when status = \'E\' then job_name end) as ERROR, COUNT(case when status = \'R\' then job_name end) as RUNNING from sairepo.etl_status where job_name like \'%15MIN%Aggre%\' and start_time >\'{1}\' and start_time <\'{2}\' group by extract(hour from start_time) order by extract(hour from start_time);\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, day_agg_date_min, day_agg_date_max)
        return commands.getoutput(cmd).strip()

def getLastJobName(filename, type):
        if type == 'DAYSUMMARY':
                cmd = 'grep -A1 "^[a-z]" {0} | grep -v day | grep "^[0-9]" |gawk -F"," \'{{print $2}}\' |tail -1'.format(filename)
        elif type == 'WEEKSUMMARY':
                cmd = 'grep -A1 "^[a-z]" {0} | grep -v week | grep "^[0-9]" |gawk -F"," \'{{print $2}}\' |tail -1'.format(filename)
        else:
                cmd = 'grep -A1 "^[a-z]" {0} | grep -v time_frame | grep "^[0-9]" |gawk -F"," \'{{print $2}}\''.format(filename)
        return commands.getoutput(cmd).split()

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
        filename = dayAggregationLogs + 'dayJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^day/p' | gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Day'])
        cmdexport = "tac {0} | sed -n '1,/^day/p' |grep -i \"HIVETOHBASELOADER\" |gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['ExportDay'])
        exportJobs = commands.getoutput(cmdexport).split('\n')[::-1]
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^day/p' | gawk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Day'])
        stillRunningExportJobs = getStillRunningJobs("tac {0} | sed -n '1,/^day/p' |grep -i \"HIVETOHBASELOADER\"| gawk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['ExportDay'])
        if len(exportJobs) > 1:
                recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs + exportJobs + stillRunningExportJobs
        else:
                recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                dayLongJobs = [OrderedDict(zip(recordList[0].strip().split(','), item.strip().split(','))) for item in recordList[1:]]
                writeToCsv(dayLongJobs, dayAggregationLogs, 'dayLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[MAJOR_ALERT]:Long Running Day Jobs on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Day Jobs')
                frameSnmpContent(dayLongJobs,type)
        else: print 'No long running jobs found for DAY'

def frameSnmpContent(longJobs,type):
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::longRunningJobs SAI-MIB::jobNames s "{3}" SAI-MIB::type s "{4}"'.format(snmpCommunity,snmpIp,snmpPort,longJobs,type))
        if str(status) =='0':
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'

def getLongRunHour(type):
        filename = hourAggregationLogs + 'hourJobsStatus_' + str(curDate) + '_Allhours.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^time_frame/p' | gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Hour'])
        cmdexport = "tac {0} | sed -n '1,/^time_frame/p' |grep -i \"HIVETOHBASELOADER\" |gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['ExportHour'])
        exportJobs = commands.getoutput(cmdexport).split('\n')[::-1]
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^time_frame/p' | gawk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Hour'])
        stillRunningExportJobs = getStillRunningJobs("tac {0} | sed -n '1,/^time_frame/p' |grep -i \"HIVETOHBASELOADER\"| gawk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['ExportHour'])
        if len(exportJobs) > 1:
                recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs + exportJobs + stillRunningExportJobs
        else:
                recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                hourLongJobs = [OrderedDict(zip(recordList[0].strip().split(','), item.strip().split(','))) for item in recordList[1:]]
                writeToCsv(hourLongJobs, hourAggregationLogs, 'hourLongRunningJobs_' + str(curDate) + '_Allhours.csv')
                sendMail("[MAJOR_ALERT]:Long Running Hour Jobs on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Hour Jobs')
                frameSnmpContent(hourLongJobs,type)
        else: print 'No long running jobs found for HOUR'

def getLongRunWeek(type):
        filename = weekAggregationLogs + 'weekJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^week/p' | gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['Week'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^week/p' | gawk -F\",\" '{{ if($5==\"\") print $0}}'".format(filename), thresholdValue['Week'])
        recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                weekLongJobs = [OrderedDict(zip(recordList[0].strip().split(','), item.strip().split(','))) for item in recordList[1:]]
                writeToCsv(weekLongJobs, weekAggregationLogs, 'weekLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[MAJOR_ALERT]:Long Running Week Jobs on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Week Jobs')
                frameSnmpContent(weekLongJobs,type)
        else: print 'No long running jobs found for WEEK'

def getLongRunMonth(type):
        filename = monthAggregationLogs + 'monthJobsStatus_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^job_name/p' | gawk -F\",\" '{{ if($4>{1}) print $0}}'".format(filename, thresholdValue['Month'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^job_name/p' | gawk -F\",\" '{{ if($4 == \"\") print $0}}'".format(filename), thresholdValue['Month'])
        recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                monthLongJobs = [OrderedDict(zip(recordList[0].strip().split(','), item.strip().split(','))) for item in recordList[1:]]
                writeToCsv(monthLongJobs, monthAggregationLogs, 'monthLongRunningJobs_' + str(curDate.year) + '-' + curDate.strftime('%m') + '.csv')
                sendMail("[MAJOR_ALERT]:Long Running Month Jobs on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running Month Jobs')
                frameSnmpContent(monthLongJobs,type)
        else: print 'No long running jobs found for MONTH'

def getLongRun15Min(type):
        filename = fifMinAggregationLogs + 'fifMinJobsStatus_' + str(curDate) + '.csv'
        checkFileExistence(filename)
        cmd = "tac {0} | sed -n '1,/^time_frame/p' | gawk -F\",\" '{{ if($5>{1}) print $0}}'".format(filename, thresholdValue['15Min'])
        stillRunningJobs = getStillRunningJobs("tac {0} | sed -n '1,/^time_frame/p' | gawk -F\",\" '{{ if($5 == \"\") print $0}}'".format(filename), thresholdValue['15Min'])
        recordList =  commands.getoutput(cmd).split('\n')[::-1] + stillRunningJobs
        if len(recordList) > 1:
                fifMinLongJobs = [OrderedDict(zip(recordList[0].strip().split(','), item.strip().split(','))) for item in recordList[1:]]
                writeToCsv(fifMinLongJobs, fifMinAggregationLogs, 'fifMinLongRunningJobs_' + str(curDate) + '.csv')
                sendMail("[MAJOR_ALERT]:Long Running 15 Min Jobs on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), recordList, 'Long Running 15 Min Jobs')
                frameSnmpContent(fifMinLongJobs,type)
        else: print 'No long running jobs found for 15 MIN'

def getStillRunningJobs(cmd, threshold):
        jobsUnderExecution = []
        output = commands.getoutput(cmd)

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
                print '{0} does not exist.'.format(fileName)
                exit(0)

def sendMail(label,logs,summary=None):
        print 'Sending alert for {0}'.format(summary)
        html = htmlUtil.generateHtmlFromList(summary,logs)
        EmailUtils().frameEmailAndSend(label,html)


def getFormattedDictList(output, type, lastJobNameList=[]):
        formattedOutput = [map(str.strip, " ".join(row.split()).split('|')) for row in output.split('\n')]
        if type == 'DAYSUMMARY' or type == 'WEEKSUMMARY' or type == 'HOURSUMMARY' or type == '15MINSUMMARY':
                jobNameList = ['Last Job Name'] + lastJobNameList
                for i in range(len(jobNameList)):
                        formattedOutput[i].append(jobNameList[i])
        if len(formattedOutput) <= 1:
                print 'No data found for {0}'.format(type)
                exit(0)
        return [OrderedDict(zip(formattedOutput[0], item)) for item in formattedOutput[1:]]

def writeToCsv(log, filePath, fileName):
        print 'Writing data to %s' %(filePath)
        if not os.path.exists(filePath):
                commands.getoutput('mkdir -p %s' %(filePath))
        with open(filePath+fileName, 'ab') as f:
                writer = csv.DictWriter(f, log[0].keys())
                fields = log[0].keys()
                writer.writerow(dict(zip(fields, fields)))
                writer.writerows(log)

try:
        type = sys.argv[1]
except IndexError:
        print 'Please pass a valid argument'
        print '[USAGE1: python checkAggregations.py 15MIN/HOUR/DAY/WEEK/MONTH]'
        print '[USAGE2: python checkAggregations.py 15MINSUMMARY/HOURSUMMARY/DAYSUMMARY/WEEKSUMMARY]'
        print '[USAGE3: python checkAggregations.py LONG15MIN/LONGHOUR/LONGDAY/LONGWEEK/LONGMONTH]'
        exit(1)

getThresholdValues()
aggregationsCheck(type)
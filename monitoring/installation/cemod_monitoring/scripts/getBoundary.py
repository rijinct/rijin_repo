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

import sys, datetime, commands, os, csv
from collections import OrderedDict
from xml.dom import minidom
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils

### Variables assignment ###

curDate = datetime.date.today()
curDateTime = datetime.datetime.now()
hr_agg_date_hr = commands.getoutput('date +"%Y-%m-%d %H"')
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

def checkUsageThreshold(usageJobs):
        dayBoundaryAdapJobs = ['Usage_TT_1_LoadJob','Usage_BCSI_1_LoadJob','Usage_BB_THRPT_1_LoadJob']
        todayDate = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        breachedUsageJobs = ['Delayed Usage Jobs,Delay']
        usageJobsList = usageJobs.split("\n")
        for usageJob in usageJobsList:
                maxValue = usageJob.split(",")[2].strip()
                if maxValue and maxValue !=" " and usageJob.split(",")[1].strip() in dayBoundaryAdapJobs:
                        if maxValue != todayDate:
                                breachedUsageJobs.append(usageJob.split(",")[1].strip())
                        else:
                                print 'Job {0} is running on time'.format(usageJob.split(",")[1].strip())
                elif maxValue and maxValue != " " and 'ArchivingJob' not in usageJob.split(",")[1].strip() and 'DQM' not in usageJob.split(",")[1].strip():
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
                                breachedUsageJobs.append(usageJob.split(",")[1].strip()+","+str(mins))
                else:
                        print 'This job {0} is not handled'.format(usageJob.split(",")[1].strip())
        print 'Breached Usage jobs list is',breachedUsageJobs
        if len(breachedUsageJobs) > 1:
                html = HtmlUtil().generateHtmlFromList('Below are the Delayed Usage Jobs', breachedUsageJobs)
                EmailUtils().frameEmailAndSend('Delayed Usage Job(s) list',html)


def getUsageBoundary():
        usagecmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1} as Time_Frame,a.jobid,maxvalue,region_id from (select boundary.jobid as jobid,maxvalue,region_id from boundary where jobid not in (select jd.jobid from job_dictionary jd join job_prop jp on jd.id=jp.jobid and jp.paramname=\'ARCHIVING_DAYS\' and jp.paramvalue=\'0\' and jd.jobid like \'%Usage%\')) a where a.jobid like\'%Usage%\' order by maxvalue desc;\\"" | egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range)
        usageboundaryStatus = commands.getoutput(usagecmd).strip().replace("|",",").rstrip()
        csvWriter = CsvWriter('boundaryStatusUsage','UsageBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('boundaryStatusUsage','UsageBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),usageboundaryStatus)
        checkUsageThreshold(usageboundaryStatus)

def get15MinBoundary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Hour\'||\'==\'||{2}||\'Hour\' as Time_Frame,a.jobid,maxvalue from (select boundary.jobid as jobid,maxvalue from boundary where jobid not in (select jd.jobid from job_dictionary jd join job_prop jp on jd.id=jp.jobid and jp.paramname=\'JOB_ENABLED\' and jp.paramvalue=\'NO\' and jd.jobid like \'%15MIN_AggregateJob\')) a where a.jobid like\'%15MIN_AggregateJob\' or a.jobid like\'%15MIN_ExportJob\' order by maxvalue desc;\\"" | egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range, postgres_To_hr_agg_range)
        fifminboundaryStatus = commands.getoutput(cmd).strip().replace("|",",").rstrip()
        csvWriter = CsvWriter('boundaryStatus','15MinBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('boundaryStatus','15MinBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),fifminboundaryStatus)

def getHourBoundary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Hour\'||\'==\'||{2}||\'Hour\' as Time_Frame,a.jobid,maxvalue from (select boundary.jobid as jobid,maxvalue from boundary where jobid not in (select jd.jobid from job_dictionary jd join job_prop jp on jd.id=jp.jobid and jp.paramname=\'JOB_ENABLED\' and jp.paramvalue=\'NO\' and jd.jobid like \'%HOUR%AggregateJob%\')) a where a.jobid like\'%HOUR_AggregateJob%\' or a.jobid like\'%HOUR_ExportJob\' order by maxvalue desc;\\"" | egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range, postgres_To_hr_agg_range)
        hourboundaryStatus = commands.getoutput(cmd).replace("|",",").rstrip()
        csvWriter = CsvWriter('boundaryStatus','HourBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('boundaryStatus','HourBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),hourboundaryStatus)


def getDayBoundary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1} as Time_Frame,a.jobid,maxvalue from (select boundary.jobid as jobid,maxvalue from boundary where jobid not in (select jd.jobid from job_dictionary jd join job_prop jp on jd.id=jp.jobid and jp.paramname=\'JOB_ENABLED\' and jp.paramvalue=\'NO\' and jd.jobid like \'%DAY%AggregateJob%\')) a where a.jobid like\'%DAY%AggregateJob%\' or a.jobid like \'%DAY_ExportJob%\' order by maxvalue desc;\\"" | egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host, postgres_Day_agg_date)
        dayboundaryStatus = commands.getoutput(cmd).replace("|",",").rstrip()
        csvWriter = CsvWriter('boundaryStatus','DayBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('boundaryStatus','DayBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),dayboundaryStatus)

def getWeekBoundary():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select \'Week(\'||\'{1}\'||\'{2}\'||\'-\'||\'{3}\'||\'{4}\'||\')\' as Time_Frame,a.jobid,maxvalue from (select boundary.jobid as jobid,maxvalue from boundary where jobid not in (select jd.jobid from job_dictionary jd join job_prop jp on jd.id=jp.jobid and jp.paramname=\'JOB_ENABLED\' and jp.paramvalue=\'NO\' and jd.jobid like \'%WEEK_AggregateJob%\')) a where a.jobid like\'%WEEK_AggregateJob%\' or a.jobid like \'%WEEK_ExportJob%\' order by maxvalue desc;\\"" | egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host,postgres_From_week_day,postgres_From_week_month,postgres_To_week_day,postgres_To_week_month)
        weekboundaryStatus = commands.getoutput(cmd).strip().replace("|",",").rstrip()
        csvWriter = CsvWriter('boundaryStatus','WeekBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('boundaryStatus','WeekBoundaryStatus_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),weekboundaryStatus)

def getReaggBoundary():
        timePeriodList = ['15MIN','HOUR','DAY','WEEK','MONTH']
        for timePeriod in timePeriodList:
                cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'Hour\'||\'==\'||{2}||\'Hour\' as Time_Frame,provider_jobname,status,min(TO_TIMESTAMP(report_time::text, \'YYYY-MM-DD HH24:MI:SS\')) as report_time from reagg_list where provider_jobname in (select distinct provider_jobname from reagg_list where provider_jobname like \'%{3}%\') and (status=\'Completed\' or status=\'Completed_Part\') group by provider_jobname,status order by report_time desc;\\""| egrep -v "rows|row|--|time_frame"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hr_agg_range, postgres_To_hr_agg_range,timePeriod)
                reaggboundaryStatus = commands.getoutput(cmd).strip().replace("|",",").rstrip()
                csvWriter = CsvWriter('boundaryStatusReagg','ReaggBoundaryStatus_%s_%s.csv'%(timePeriod,datetime.date.today().strftime("%Y%m%d")),header="Yes")
                csvWriter = CsvWriter('boundaryStatusReagg','ReaggBoundaryStatus_%s_%s.csv'%(timePeriod,datetime.date.today().strftime("%Y%m%d")),reaggboundaryStatus)


try:
        type = sys.argv[1]
except IndexError:
        print 'Please pass a valid argument'
        print '[USAGE: python getBoundary.py USAGE/15MIN/HOUR/DAY/WEEK/REAGG]'
        exit(1)

boundaryCheck(type)

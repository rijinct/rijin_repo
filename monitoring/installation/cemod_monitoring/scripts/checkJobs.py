#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose:
#
# Date:    06-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import sys, datetime, commands, os, csv
from collections import OrderedDict
from xml.dom import minidom
from checkAggregations import getFormattedDictList, writeToCsv
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

### Variables assignment ###

currentDate = datetime.date.today()
TNPJobLogs = "/var/local/monitoring/output/tnpJobStatus/"
hr_to_date = commands.getoutput('date +"%Y-%m-%d %H:%M"')
hr_from_date = commands.getoutput('date -d \'1 hour ago\' "+%Y-%m-%d %H:%M"')
postgres_To_15min_agg_range = commands.getoutput('date +"%Y-%m-%d %H:%M" | cut -d" " -f2')
postgres_From_15min_agg_range = commands.getoutput('date -d \'1 hour ago\' "+%Y-%m-%d %H:%M" | cut -d" " -f2')

dayQSLogs ="/var/local/monitoring/output/qsJobStatus_Day/"
weekQSLogs ="/var/local/monitoring/output/qsJobStatus_Week/"
postgres_Day_agg_date = commands.getoutput('date -d \'1 day ago\' "+%d-%m-%Y"|cut -d- -f1')
day_agg_date_day = currentDate
week_agg_date_week = currentDate
postgres_From_week_agg_range = commands.getoutput('date -dlast-monday +%Y-%m-%d|cut -d- -f3')
postgres_To_week_agg_range = commands.getoutput('date -dlast-sunday +%Y-%m-%d|cut -d- -f3')

usageJobLogs ="/var/local/monitoring/output/Usage_Job/"
hr_to_date = commands.getoutput('date +"%Y-%m-%d %H:%M"')
hr_from_date = commands.getoutput('date -d \'1 hour ago\' "+%Y-%m-%d %H:%M"')
postgres_To_hour_usage_range = commands.getoutput('date +"%Y-%m-%d %H:%M" | cut -d" " -f2')
postgres_From_hour_usage_range = commands.getoutput('date -d \'1 hour ago\' "+%Y-%m-%d %H:%M" | cut -d" " -f2')

### End ###

def initialize(type):
	if type == 'TNP':
		fileName = 'tnpJobsStatus_{0}_Allhours.csv'.format(currentDate)
		writeToCsv(getFormattedDictList(tnpJobsCheck(), type),TNPJobLogs, fileName)
	elif type == 'DAYQS':
		fileName = 'dayJobsStatus_QS_{0}.csv'.format(commands.getoutput('date +"%Y-%m"'))
		writeToCsv(getFormattedDictList(dayQSJobsCheck(), type), dayQSLogs, fileName)
	elif type == 'WEEKQS':
		fileName = 'weekJobsStatus_QS_{0}.csv'.format(commands.getoutput('date +"%Y-%m"'))
		writeToCsv(getFormattedDictList(weekQSJobsCheck(), type), weekQSLogs, fileName)
	elif type == 'USAGE':
		fileName = 'UsageJobsStatus_{0}_Allhours.csv'.format(currentDate)
		writeToCsv(getFormattedDictList(usageJobsCheck(), type), usageJobLogs, fileName)

def tnpJobsCheck():	
	return commands.getoutput('ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select \'{1}\'||\'-\'||\'{2}\' as Time_Frame,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time )*60 + DATE_PART(\'seconds\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where load_time >\'{3}:00\' and load_time <\'{4}:00\' and job_name like\'%TNP%\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_15min_agg_range, postgres_To_15min_agg_range, hr_from_date, hr_to_date)).strip()

def dayQSJobsCheck():
	return commands.getoutput('ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1} as Day,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where load_time >\'{2} 00:00:00\' and load_time <\'{3} 23:59:59\' and job_name like \'%DAY_QS%\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_Day_agg_date, day_agg_date_day, day_agg_date_day)).strip()

def weekQSJobsCheck():
	return commands.getoutput('ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select {1}||\'-\'||{2} as Week,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where status=\'S\' and load_time >\'{3} 00:00:00\' and load_time <\'{4} 23:59:59\' and job_name like\'%WEEK_QS%\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_week_agg_range, postgres_To_week_agg_range, week_agg_date_week, week_agg_date_week)).strip()

def usageJobsCheck():
	return commands.getoutput('ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select \'{1}\'||\'-\'||\'{2}\' as Time_Frame,job_name,start_time,end_time,DATE_PART(\'hour\', end_time - start_time )* 60 + DATE_PART(\'minute\', end_time - start_time ) ExecutionDuration,status,error_description from sairepo.etl_status where load_time >\'{3}:00\' and load_time <\'{4}:00\' and job_name like \'%Usage%\' order by end_time desc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, postgres_From_hour_usage_range, postgres_To_hour_usage_range, hr_from_date, hr_to_date)).strip()

try:
	type = sys.argv[1]
except IndexError:
	print 'Please pass a valid argument'
	print '[USAGE: python checkJobs.py TNP/DAYQS/WEEKQS/USAGE]'
	
initialize(type)
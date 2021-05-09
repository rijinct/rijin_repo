#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring team
# Version: 0.1
# Purpose:Scripts checks the Jobs status for Usage,TNP and QS
#
# Date:    06-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import sys, datetime, subprocess, os, csv
from collections import OrderedDict
from xml.dom import minidom
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from dbConnectionManager import DbConnection
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

### Variables assignment ###
curDate = datetime.date.today()
curDateTime = datetime.datetime.now()
hr_to_date = subprocess.getoutput('date +"%Y-%m-%d %H:%M"')
hr_from_date = (curDateTime - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
postgres_Day_agg_date =  (curDate - datetime.timedelta(days = 1)).day
day_agg_date_day = curDate
week_agg_date_week = curDate

postgres_To_week_agg_date =  (curDate - datetime.timedelta(days = curDate.weekday()+1))
postgres_To_week_agg_range = postgres_To_week_agg_date.day
postgres_From_week_date = (curDate - datetime.timedelta(days = curDate.weekday()+7) if curDate.weekday() == 0 else curDate - datetime.timedelta(days = curDate.weekday()))
postgres_From_week_agg_range = postgres_From_week_date.day

postgres_To_hour_usage_range= curDateTime.hour
postgres_From_hour_usage_range = (curDateTime - datetime.timedelta(hours=1)).hour
### End ###


def initialize(type):
        if type == 'TNP':
                TNPJOBS = tnpJobsCheck()
        elif type == 'DAYQS':
                DAYQSJOBS = dayQSJobsCheck()
        elif type == 'WEEKQS':
                WEEKQSJOBS = weekQSJobsCheck()
        elif type == 'USAGE':
                USAGEJOBS = usageJobsCheck()



def tnpJobsCheck():
        tnpJobsquerySql = PropertyFileUtil('tnpJobsquery','PostgresSqlSection').getValueForKey()
        finaltnpJobsquerySql = tnpJobsquerySql.replace("hr_from_date",str(hr_from_date)).replace("hr_to_date",str(hr_to_date)).replace("hr_from_date",str(hr_from_date)).replace("hr_to_date",str(hr_to_date))
        CsvUtil().executeAndWriteToCsv('tnpJobs',finaltnpJobsquerySql)

def dayQSJobsCheck():
        dayQSJobsquerySql = PropertyFileUtil('dayQSJobsquery','PostgresSqlSection').getValueForKey()        
        finaldayQSquerySql = dayQSJobsquerySql.replace("day_agg_date",str(postgres_Day_agg_date)).replace("day_agg_from_date",str(day_agg_date_day)).replace("day_agg_to_date",str(day_agg_date_day))        
        CsvUtil().executeAndWriteToCsv('dayQS',finaldayQSquerySql)

def weekQSJobsCheck():
        weekQSJobsquerySql = PropertyFileUtil('weekQSJobsquery','PostgresSqlSection').getValueForKey()        
        finalweekQSquerySql = weekQSJobsquerySql.replace("From_week_agg_range",str(postgres_From_week_agg_range)).replace("To_week_agg_range",str(postgres_To_week_agg_range)).replace("week_agg_date",str(week_agg_date_week))        
        CsvUtil().executeAndWriteToCsv('weekQS',finalweekQSquerySql)

def usageJobsCheck():
        usageJobsquerySql = PropertyFileUtil('usageJobsquery','PostgresSqlSection').getValueForKey()        
        finalusageJobquerySql = usageJobsquerySql.replace("From_hour_usage_range",str(postgres_From_hour_usage_range)).replace("To_hour_usage_range",str(postgres_To_hour_usage_range)).replace("hr_from_date",str(hr_from_date)).replace("hr_to_date",str(hr_to_date))        
        CsvUtil().executeAndWriteToCsv('usageJob',finalusageJobquerySql)


try:
        type = sys.argv[1]
except IndexError:
        logger.exception('Please pass a valid argument')
        logger.exception('[USAGE: python checkJobs.py TNP/DAYQS/WEEKQS/USAGE]')

initialize(type)


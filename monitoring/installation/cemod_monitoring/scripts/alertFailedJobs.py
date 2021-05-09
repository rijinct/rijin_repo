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
# 2.
#############################################################################
import sys,commands,datetime
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
from snmpUtils import SnmpUtils
from writeCsvUtils import CsvWriter
from htmlUtil import HtmlUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))

def checkFailedJobs(type):
        curDate=datetime.datetime.now()
        if type == 'USAGE':
                pattern1="Usage_%LoadJob"
                pattern2="Usage_%LoadJob"
                startTimeStamp = (datetime.datetime.now()-datetime.timedelta(minutes=15))
                endTimeStamp = datetime.datetime.now()
        elif type == '15MIN':
                pattern1="%15min_AggregateJob"
                pattern2="%15MIN_ExportJob"
                startTimeStamp = (datetime.datetime.now()-datetime.timedelta(minutes=15))
                endTimeStamp = datetime.datetime.now()
        elif type == 'HOUR':
                pattern1="%Hour_AggregateJob"
                pattern2="%Hour_ExportJob"
                endTimeStamp = datetime.datetime.now()
                startTimeStamp = (datetime.datetime.now()-datetime.timedelta(minutes=60))
        elif type == 'DAY' or type == 'WEEK' or type == 'MONTH':
                pattern1="%{0}_AggregateJob".format(type)
                pattern2="%{0}_ExportJob".format(type)
                startTimeStamp = datetime.datetime.combine(curDate, datetime.time.min)
                endTimeStamp = datetime.datetime.combine(curDate, datetime.time.max)
        elif type == 'REAGG':
                pattern1="%ReaggregateJob"
                pattern2="%ReaggregateJob"
                startTimeStamp = (datetime.datetime.now()-datetime.timedelta(minutes=60))
                endTimeStamp = datetime.datetime.now()
        countErrorJobs = commands.getoutput('ssh %s "psql sai sairepo -c \\"select count(*) from etl_status where status=\'E\' and (job_name like\'%s\' or job_name like\'%s\') and start_time >= \'%s\'and end_time <= \'%s\';\\""'%(cemod_postgres_sdk_fip_active_host,pattern1,pattern2,startTimeStamp,endTimeStamp))
        if int(countErrorJobs.split()[2]) > 0:
                errorJobs = commands.getoutput('ssh %s "psql sai sairepo -c \\"select job_name,start_time,end_time,status,"error_description" from etl_status where status=\'E\' and (job_name like\'%s\' or job_name like\'%s\') and start_time >= \'%s\' and end_time <= \'%s\';\\"" | egrep -v "rows|row|job_name|--" '%(cemod_postgres_sdk_fip_active_host,pattern1,pattern2,startTimeStamp,endTimeStamp)).strip().replace("|",",")
                for errorJob in errorJobs.split('\n'):
                        csvWriter = CsvWriter('JobsFailure','failedJobs_%s_%s.csv'%(type,datetime.date.today().strftime("%Y%m%d")),datetime.date.today().strftime("%Y-%m-%d")+','+errorJob.strip())
                errorList = appendHeader(errorJobs.split('\n'))
                frameEmailContent(errorList)
                frameSnmpContent(errorList,type)
                print 'Failed Jobs are',errorList
                exit(2)
        else:
                print 'No Failed Jobs'
                exit(0)

def frameSnmpContent(errorList,type):
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::schedulerJobsFailure SAI-MIB::jobNames s "{3}" SAI-MIB::type s "{4}"'.format(snmpCommunity,snmpIp,snmpPort,errorList[1:],type))
        if str(status) == '0':
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'

def appendHeader(errorJobs):
        return ['Job Name,Start Time, End Time, Status, Error Description'] + errorJobs

def frameEmailContent(errorList):
        label = 'Below is the Failed Job information:'
        html = str(HtmlUtil().generateHtmlFromList(label,errorList))
        EmailUtils().frameEmailAndSend("[CRITICAL_ALERT]:Job(s) Failure Alert on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)

def main():
        if len(sys.argv) > 1:
                checkFailedJobs(sys.argv[1])
        else:
                print 'Incorrect Usage: Either no Arguments passed nor argument passed is incorrect, so exiting..'
                exit(1)

if __name__ == "__main__":
        main()
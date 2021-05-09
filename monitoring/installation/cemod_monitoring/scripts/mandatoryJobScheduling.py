#############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose:
#
# Date:    16-02-2018
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
from snmpUtils import SnmpUtils
from htmlUtil import HtmlUtil

def getJobName(type):
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName('MANDATORYJOBSCHEDULING')[0].getElementsByTagName('property')
        return [val.attributes['value'].value.split(',') for val in elements if val.attributes['Name'].value==type][0]

def getTriggerName(type):
        if type == 'Aggregation':
                type = 'Perf'

        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai saischedule -c \\"select distinct job_name from qrtz_triggers where job_name like \'%{1}_%\';\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, type)
        return commands.getoutput(cmd).strip().split()[1:]

def frameSnmpContent(missingJobsList):
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::mandatoryJobsList SAI-MIB::mandatoryJobs s "{3}"'.format(snmpCommunity,snmpIp,snmpPort,thresholdBreachedDict))
        if str(status) == '0':
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'

def getMissingJobs(type):
        print "Unscheduled jobs for for {0}".format(type)
        missingJobs = list(set(getJobName(type)) - set(getTriggerName(type)))
        print missingJobs
        if len(missingJobs) > 0:
                missingJobsList = ['Job Name'] + missingJobs
                html = HtmlUtil().generateHtmlFromList('Mandatory Jobs not Scheduled', missingJobsList)
                EmailUtils().frameEmailAndSend("[CRITICAL_ALERT]:{0} Mandatory Jobs Scheduling Alert on {1}".format(type,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)
                frameSnmpContent(missingJobsList)
                exit(2)
        else: print "No missing jobs for {0}".format(type)
        exit(0)

try:
        type = sys.argv[1]
except IndexError:
        print 'Please pass a valid argument'
        print '[USAGE: python mandatoryJobScheduling.py Usage/Aggregation]'
        exit(1)
getMissingJobs(type)


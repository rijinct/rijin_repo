#############################################################################                           
#                              Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:Monitoring team
# Version: 0.1
# Purpose:To capture the TNP Latency
#
# Date:    18-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import sys, datetime, commands, os
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter

patternDate = datetime.date.today()
tnpLatency="/var/local/monitoring/output/Tnp_Latency"


def getJobName():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai saischedule -c \\"select job_name from qrtz_job_details where job_name like \'%TNP%ThresholdJob\';\\""| egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host)
        jobNames = commands.getoutput(cmd).strip().split()[1:]
        return jobNames

def writeTnpLatency(tnpjobNames):
        csvWriter = CsvWriter('TnpLatency','TnpJobsLatency_%s.csv'%(patternDate.strftime("%Y-%m-%d")))
        for tnpJobName in tnpjobNames:
                commands.getoutput('ssh {0} "find /opt/nsn/ngdb/scheduler/app/log/{1}* -mmin -60 -type f | xargs grep -i \'Getting Job Properties For Job:\|Total latency summary for time\|Overall latency\'" >> {2}/JobsLatency_{3}_First.csv '.format(cemod_postgres_sdk_fip_active_host,tnpJobName,tnpLatency,patternDate))
        commands.getoutput('awk -F" " \'/Getting Job Properties For Job/ {{ gsub(/Getting Job Properties For Job:/,"",$NF) ; A=$NF; next }}/Total latency summary for time/ {{ gsub(/Total latency summary for time/,"",$NF) ; gsub(/:/,"",$NF); B=strftime("%Y-%m-%d %H:%M:%S",$NF/1000); gsub(",.*","",$1) ; gsub(".*log:","",$1); C= $1" "$2 ; next}}/Overall latency/ {{ gsub(/Overall latency/,"",$0); gsub(".*log:","",$0);D=$0}}{{ print A","B","C","D }}\' {0}/JobsLatency_{1}_First.csv >> {0}/JobsLatency_{1}_Second.csv'.format(tnpLatency,patternDate))
        commands.getoutput('sed \'s/,:/,/g\' {0}/JobsLatency_{1}_Second.csv | gawk -F"," \'{{print $1","$2","$3","$5}}\' >> {0}/JobsLatency_{1}_Second.csv'.format(tnpLatency,patternDate))
        commands.getoutput('awk -F, \'{{gsub(/[-:]/," ",$B);gsub(/[-:]/," ",$C);d2=mktime($3);d1=mktime($2);print (d2-d1)/60;}}\' {0}/JobsLatency_{1}_Second.csv >> {0}/JobsLatency_{1}_Third.csv'.format(tnpLatency,patternDate))
        commands.getoutput('paste -d, {0}/JobsLatency_{1}_Second.csv {0}/JobsLatency_{1}_Third.csv >> {0}/TnpJobsLatency_{1}.csv'.format(tnpLatency,patternDate))
        removeUnusedFiles('{0}/JobsLatency_{1}_First.csv'.format(tnpLatency, patternDate))
        removeUnusedFiles('{0}/JobsLatency_{1}_Second.csv'.format(tnpLatency, patternDate))
        removeUnusedFiles('{0}/JobsLatency_{1}_Third.csv'.format(tnpLatency, patternDate))

def removeUnusedFiles(filename):
        try:
                os.remove(filename)
        except OSError:
                pass

writeTnpLatency(getJobName())

############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: monitoring tool team
# Version: 2.0
# Date:04-April-2017
# Purpose : Collect stats of Rejected,Processed and Total Records
#############################################################################
#############################################################################
# Code Modification History
#############################################################################

import commands,os
import datetime
import time
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")


def createOutputCSV(topologiesList):
        global currentDate
        global filepath
        currentDate = datetime.date.today()
        filepath = '/var/local/monitoring/output/processMonitor/processMonitor_'+ str(currentDate) + '.csv'
        header = 'Hour,'
        status1,output1 = commands.getstatusoutput('ls ' + filepath)
        if status1 != 0:
                for i in topologiesList:
                        header = header +  i + '_Total,'
                        header = header +  i + '_Proc,'
                        header = header +  i + '_Bad,'
                header = header[:-1] + '\n'
                new_file = open(filepath,'w')
                new_file.write(header)
                new_file.close()


def getReportTime():
        global minTime
        global minTime_epoch
        global maxTime_epoch
        global currentDateTime
        currentDate = datetime.date.today()
        currentDateTime = datetime.datetime.now()
        hour_delta = datetime.timedelta(hours=1)
        minTime = datetime.time((currentDateTime-hour_delta).hour,0,0)
        if currentDateTime.hour >= 0 and currentDateTime.hour < 1:
                currentDate1 = datetime.date.today() - datetime.timedelta(days=1)
                minTime = datetime.datetime.combine(currentDate1,minTime)
        else:   minTime = datetime.datetime.combine(currentDate,minTime)
        maxTime = minTime + hour_delta
        pattern = '%Y-%m-%d %H:%M:%S'
        minTime_epoch = int(time.mktime(time.strptime(str(minTime), pattern)))*1000
        maxTime_epoch = int(time.mktime(time.strptime(str(maxTime), pattern)))*1000
        csvString = ''
        csvString = csvString + getRecords(minTime_epoch,maxTime_epoch)
        addToCsv(csvString)


def getRecords(minTime_epoch,maxTime_epoch):
        topolgiesList = []
        topologiesValues = ""
        cmd_processmonitor = 'su - %s -c "beeline -u \'%s\'  --silent=true --showHeader=false --outputformat=csv2 -e \'SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set hive.exec.reducers.max=1;select topology_name,sum(tot_recs),sum(processed_recs),sum(bad_recs) from process_monitor where report_time >= \'%s\' and report_time < \'%s\' group by topology_name order by topology_name ;\' 2>Logs.txt"|grep -v DEBUG|grep -v INFO' %(cemod_hdfs_user,cemod_hive_url,minTime_epoch,maxTime_epoch)
        print cmd_processmonitor
        processMonitorRec = commands.getoutput(cmd_processmonitor)
        if processMonitorRec:
                processMonitorRecList = processMonitorRec.split("\n")
                for processMonitorRec in processMonitorRecList:
                        topolgiesList.append(processMonitorRec.split(",")[0].strip())
                        topologyValue = processMonitorRec.split(",")[1:4]
                        for topoValue in topologyValue:
                                topologiesValues = topologiesValues + "," + topoValue
                print 'Topologies Value is ',topologiesValues.lstrip(",")
                createOutputCSV(topolgiesList)
                return topologiesValues.lstrip(",")
        else:
                print 'No data present in process monitor table'
                exit (0)

def addToCsv(csvString):
        final_out = str(minTime.hour) + 'th Hour,' + csvString + '\n'
        filename = '/var/local/monitoring/output/processMonitor/processMonitor_'+ str(minTime.date()) + '.csv'
        dump_out = open(filename,'a')
        dump_out.write(final_out)
        dump_out.close()

def processCheck():
        processCmd = 'ps -eaf | grep "%s" | grep -v grep | gawk -F" " \'{print $2}\''%(os.path.basename(__file__))
        process = commands.getoutput(processCmd).split('\n')
        if len(process) > 3:
                exit(0)

processCheck()
getReportTime()

status,delLogs = commands.getstatusoutput('su - %s -c "rm -rf Logs.txt"' %(cemod_hdfs_user))

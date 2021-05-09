#!/usr/bin/python
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com,kalpashree.hv@nokia.com
# Version: 0.1  2016-10-25 16:30:00 2016-10-26 08:38
# Purpose: Send Monitorin Cluster Report
#
# Date:    09-03-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import smtplib,csv,commands,datetime,sys,os
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
from htmlUtil import HtmlUtil
from xml.dom import minidom
from csvUtil import CsvUtil

logPath = '/var/local/monitoring/'
commonIfwPath = '/opt/nsn/ngdb/ifw/etc/common'
outputFilePath = '/var/local/monitoring/output/sendSummaryReport/'
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
htmlUtil = HtmlUtil()
today = datetime.datetime.today()

def getThreshold(type):
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        elements = xmldoc.getElementsByTagName('SUMMARYTHRESHOLDS')[0].getElementsByTagName('property')
        return [val.attributes['value'].value for val in elements if val.attributes['Name'].value==type][0]

def getBacklogThreshold(type,application):
        xmldoc = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        if application == "CEMoD":
                elements = xmldoc.getElementsByTagName('BACKLOG')[0].getElementsByTagName('Input')
        else:
                elements = xmldoc.getElementsByTagName('BACKLOGAE')[0].getElementsByTagName('Input')
        return [val.attributes['Thresholdlimit'].value for val in elements if val.attributes['Name'].value==type][0]

def getLatestFile(path):
        get_file_cmd = 'ls -ltr %s|tail -1|awk -F " " \'{print $9}\'' %path
        status,output = commands.getstatusoutput(get_file_cmd)
        if 'No such file' not in output:
                return output


def checkFileExistence(filePath):
        if os.path.exists(filePath):
                return filePath

def countLine(fileName):
        if fileName:
                return int(commands.getoutput('cat {0}| wc -l'.format(fileName)))

def getOldFile(path):
        get_file_cmd = 'ls -ltr %s|tail -2|head -1|awk -F " " \'{print $9}\'' %path
        status,output = commands.getstatusoutput(get_file_cmd)
        if 'No such file' not in output:
                return output

def getQSCacheLogs(label,path):
        try:
                new_file = open(path)
                row_list = new_file.readlines()
                final_list = []
                final_list.append(row_list[0])
                valueList = row_list[-1].split(',')
                for idx,item in enumerate(valueList):
                        if item.strip() == str(getThreshold('QSCache')):         #Using strip() to remove '\n' from last column
                                item = item.strip() + 'Red'
                                valueList[idx] = item
                final_list.append(','.join(valueList))
                return str(htmlUtil.generateHtmlFromList(label, final_list))
        except (IOError, ValueError):
                final_list = ['No QS Cache Count observed']
                return str(htmlUtil.generateHtmlFromList(label, final_list))

def getserviceStability():
        try:
            files  = getLatestFile("/var/local/monitoring/output/serviceStability/ServiceStability_*")
            filePath=checkFileExistence(files)
            if filePath and countLine(filePath) > 1:
                    status,getRows = commands.getstatusoutput("cat %s | sed -n '/^Date/{p; :loop n; p; /^Date/q; b loop}'" %(filePath))
                    if 'error' in getRows.split('\n')[-1]:
                            final_list = getRows.split('\n')[:-1]
                    else: final_list = getRows.split('\n')
                    #Reverse the list
                    final_list = final_list
                    finalList = []
                    finalList.append(final_list[0])
                    for subString in final_list[1:]:
                            finalList.append(serviceStabilityColorCoding(subString))
            else:
                    finalList = ['File not present or is empty']
        except IndexError:
            finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('Service Stability',finalList))

def serviceStabilityColorCoding(subString):
        subList = subString.split(',')
        threshold = str(getThreshold('ServiceStability'))
        if subList[4].strip().startswith(threshold):
                subList[4] = subList[4] + 'Orange'
        return ','.join(subList)

def getTableCountUsage():
        file_path = checkFileExistence('{0}/output/tableCount_Usage/total_count_usage_{1}.csv'.format(logPath,datetime.date.today().strftime("%Y%m%d")))
        contents = []
        if file_path:
                with open(file_path) as file:
                        contents = file.readlines()
        if contents:
                counts = []
                for row in contents:
                        counts.append(CsvUtil.delete_field_in_csv_string(row, 1))
                counts[-1] = counts[-1].replace(',24,', ',Total,')
                label = 'Usage Table Count    BEPD: %s' % (get_total_bepd(counts))
        else:
                label = 'Usage Table Count'
                counts = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList(label, counts))

def get_total_bepd(final_list):
        last_row = final_list[-1].split(',')
        bepd_fixed_and_mobile = sum(list(map(int, last_row[-2:])))
        return round((bepd_fixed_and_mobile / 1000000000), 2)



def getBacklog(backlogType):
        filePath = getLatestFile("/var/local/monitoring/output/backlogHadoop/backlogs.*.csv")
        if filePath and countLine(filePath) > 1:
                if backlogType == 'Current Backlog':
                        status,backlogRecords = commands.getstatusoutput("cat %s | sed -n '/^Time/{p; :loop n; p; /^Time/q; b loop}'|sed '1 d'" %(filePath))
                        backlogRecords = backlogRecords.split('\n')[:-2][::-1]
                elif backlogType == 'Threshold Backlog':
                        status,backlogRecords = commands.getstatusoutput("gawk -F\",\" '{print $1\",\"$2\",\"$3\",\"$4\",\"$5\",\"$6\",\"$7}' %s | gawk -F\",\" '{ if($3>500 || $4>500 || $5>500 || $6>500 || $7>500) print $0}' | grep -v \"^Time\" | tac" %(filePath))
                        if len(backlogRecords) != 0:
                                backlogRecords = getFilteredRecords(backlogRecords.split('\n')[::-1],'CEMoD')
                        else:
                                backlogRecords = ['']
                if len(backlogRecords) == 1 and backlogRecords[0] == '':
                        finalList = ['No backlogs greater than Threshold observed']
                else:
                        finalList = ['Time,DataSource,datFileCountMnt,errorDatFileCountMnt,processingDatFileCountMnt,datFileCountNgdbUsage,datFileCountNgdbWork']
                        for subString in backlogRecords:
                                finalList.append(backlogColorCoding(subString,'CEMoD'))
        else: finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList(backlogType, finalList))


def getBacklogAE(backlogType):
        filePath = checkFileExistence('{0}output/backlogHadoop/backlogsAE.{1}.csv'.format(logPath, today.strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                if backlogType == 'Current Backlog for AE:':
                        status,backlogRecords = commands.getstatusoutput("tac %s | sed -n '/^Time/{p; :loop n; p; /^Time/q; b loop}'|sed '1 d'" %(filePath))
                        backlogRecords = backlogRecords.split('\n')[:-2][::-1]
                elif backlogType == 'Threshold Backlog for AE:':
                        status,backlogRecords = commands.getstatusoutput("gawk -F\",\" '{print $1\",\"$2\",\"$3\",\"$4}' %s | gawk -F\",\" '{ if($3>500 || $4>500 ) print $0}' | grep -v \"^Time\" | tac" %(filePath))
                        if len(backlogRecords) != 0:
                                backlogRecords = getFilteredRecords(backlogRecords.split('\n')[::-1],'AE')
                        else:
                                backlogRecords = ['']
                if len(backlogRecords) == 1 and backlogRecords[0] == '':
                        finalList = ['No backlogs greater than Threshold observed']
                else:
                        finalList = ['Time,DataSource,datFileCountMnt,processingCtrFileCountMnt']
                        for subString in backlogRecords:
                                finalList.append(backlogColorCoding(subString,'AE'))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList(backlogType, finalList))

def getFilteredRecords(backlogRecords,application):
        outputList = []
        for record in backlogRecords:
                splittedRecord = record.split(',')
                threshold = int(getBacklogThreshold(splittedRecord[1],application))
                if splittedRecord[2] > threshold or splittedRecord[2] > threshold or splittedRecord[2] > threshold or splittedRecord[2] > threshold or splittedRecord[2] > threshold:
                        outputList.append(','.join(splittedRecord))
        return outputList

def backlogColorCoding(subString,application):
        threshold = int(getBacklogThreshold(subString.split(',')[1],application))
        subList = subString.split(',')[2:]
        for idx,item in enumerate(subList):
                if int(item) > threshold:
                        item = item + 'Red'
                        subList[idx] = item
        return ','.join(subString.split(',')[:2])+','+','.join(subList)

def getHourJobCount(day):
        yesterday = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        if day == 'Yesterday':
                filePath = checkFileExistence('{0}output/aggregations_Hour/hourSummary_{1}_Allhours.csv'.format(logPath, yesterday))
        if filePath and countLine(filePath) > 1:
                finalList = jobCountList("tac %s | sed -n '1,/^hour/p' | sed '1 d'" %(filePath))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('HourJob Count for ' + day,finalList))

def getDayJobCount():
        filePath = checkFileExistence('{0}output/aggregations_Day/daySummary_{1}_Allday.csv'.format(logPath, datetime.date.today()))
        if filePath and countLine(filePath) > 1:
                finalList = jobCountList("tac %s | sed -n '1,/^day/p'" %(filePath))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('DayJob Count',finalList))

def getWeekJobCount():
        filePath = checkFileExistence('{0}output/aggregations_Week/weekSummary_{1}_Allweek.csv'.format(logPath, datetime.date.today()))
        if filePath and countLine(filePath) > 1:
                finalList = jobCountList("tac %s | sed -n '1,/^week/p' | sed '1 d'" %(filePath))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('WeekJob Count',finalList))

def getfMinJobCount():
        yesterday = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        filePath = checkFileExistence('{0}output/aggregations_15Min/15MinJobsSummary_{1}_Allhours.csv'.format(logPath, yesterday))
        if filePath and countLine(filePath) > 1:
                finalList = jobCountList("tac %s | sed -n '1,/^day/p' | sed '1 d'" %(filePath))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('15 Minutes Aggregation Count',finalList))

def jobCountList(cmd):
        status,getRows = commands.getstatusoutput(cmd)
        final_list = getRows.split('\n')
        #Reverse the list
        reverse_list = final_list[::-1]
        finalList = []
        finalList.append(reverse_list[0].replace('\r',''))
        for subString in reverse_list[1:]:
                finalList.append(jobCountColorCoding(subString).replace('\r',''))
        return finalList

def jobCountColorCoding(subString):
        subList = (subString.split(',')[2:])
        for idx,item in enumerate(subList):
                if idx == 3 and int(item) > int(getThreshold('JobCount')):
                        item = item + 'Red'
                        subList[idx] = item
        return ','.join(subString.split(',')[:2])+','+','.join(subList)

def getDayTableCount():
        filePath = checkFileExistence('{0}output/tableCount_Day/DayTable_DistinctIMSI_RecordCount_{1}.csv'.format(logPath, today.strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                finalList = commands.getoutput('cat {0}'.format(filePath)).split("\n")
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('Day Table Count',finalList))

def getWeekTableCount():
        filePath = checkFileExistence('{0}output/tableCount_Week/weekTable_DistinctIMSI_RecordCount_{1}.csv'.format(logPath, firstDayOfWeek()))
        if filePath and countLine(filePath) > 1:
                finalList = commands.getoutput('cat {0}'.format(filePath)).split("\n")
        else:
                        finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('Week Table Count',finalList))

def tableCountList(filePath):
        with open(filePath, 'rb') as csvfile:
                readLog = csv.reader(csvfile, delimiter=',', quotechar='|')
                header = csvfile.next()
                finalList = []
                finalList.append(header)
                for row in readLog:
                        if row[1] == '' or int(row[1]) == int(getThreshold('TableCount')):
                                row[1] = row[1] + 'Red'
                                finalList.append(','.join(row))
        return finalList

def getTnplatency():
        yesterday = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        filePath = checkFileExistence('{0}output/Tnp_Latency/TnpJobsLatency_{1}.csv'.format(logPath, yesterday))
        if filePath and countLine(filePath) > 1:
                status,getRows = commands.getstatusoutput("tac %s | gawk -F\",\" '{ if($5>30) print $0}'" %(filePath))
                final_list = getRows.split('\n')
                if len(final_list) > 1:
                        reverse_list = final_list[::-1]
                        finalList = []
                        finalList.append(reverse_list[0])
                        for subString in reverse_list[1:]:
                                finalList.append(tnpColorCoding(subString))
                else:   finalList = ['Overall latency is not greater than 30mins']
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('Tnp Latency', finalList))

def tnpColorCoding(subString):
        subList = subString.split(',')
        if float(subList[4].strip()) > int(getThreshold('TnpLatency')):
                subList[4] = subList[4] + 'Red'
        return ','.join(subList)

def getHourBoundary():
        filePath = checkFileExistence('{0}output/boundaryStatus/HourBoundaryStatus_{1}.csv'.format(logPath, today.strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                lastHourDateTime = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y-%m-%d %H:00:00')
                lastHourDateTime = convertToDatetime(lastHourDateTime)
                finalList = boundaryList(filePath, lastHourDateTime)
                finalList = [ item for item in finalList if item is not None]
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('HourBoundary',finalList))

def getDayBoundary():
        filePath = checkFileExistence('{0}output/boundaryStatus/DayBoundaryStatus_{1}.csv'.format(logPath, today.strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                lastDayDateTime = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d 00:00:00')
                lastDayDateTime = convertToDatetime(lastDayDateTime)
                finalList = boundaryList(filePath, lastDayDateTime)
                finalList = [ item for item in finalList if item is not None]
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('DayBoundary',finalList))

def getWeekBoundary():
        filePath = checkFileExistence('{0}output/boundaryStatus/WeekBoundaryStatus_{1}.csv'.format(logPath, firstDayOfWeek()))
        if filePath and countLine(filePath) > 1:
                lastMonday = (today - datetime.timedelta(days = today.weekday()+7)).strftime('%Y-%m-%d 00:00:00')
                lastMonday = convertToDatetime(lastMonday)
                finalList = boundaryList(filePath, lastMonday)
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('WeekBoundary',finalList))

def getUsageBoundary():
        filePath = checkFileExistence('{0}output/boundaryStatus/UsageBoundaryStatus_{1}.csv'.format(logPath, today.strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                        getRows = commands.getoutput("tac %s |sed -n '1,/^time_frame/p'|gawk -F\",\" '{ print $2\",\"}'" %(filePath))
                        if getRows == 'ArchivingJob' in getRows.split(",")[1].strip():
                                lastDayDateTime = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d 00:00:00')
                                lastDayDateTime = convertToDatetime(lastDayDateTime)
                                finalList = boundaryList(filePath, lastDayDateTime)
                        else:
                                lastHourDateTime = today.strftime('%Y-%m-%d %H:00:00')
                                lastHourDateTime = convertToDatetime(lastHourDateTime)
                                finalList = boundaryList(filePath, lastHourDateTime)

        else:   finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('UsageBoundary',finalList))


def convertToDatetime(inputDate):
        format = '%Y-%m-%d %H:%M:%S'
        return datetime.datetime.strptime(inputDate, format)

def boundaryList(filePath, threshold,type=None):
        status,getRows = commands.getstatusoutput("tac %s | sed -n '1,/^time_frame/p'" %(filePath))
        final_list = getRows.split('\n')
        final_list = final_list[::-1]
        finalList = []
        finalList.append(final_list[0])
        for subString in final_list[1:]:
                if type is None:
                        finalList.append(boundaryColorCoding(subString, threshold))
                else:
                        finalList.append(subString)
        return finalList

def boundaryColorCoding(subString, threshold):
        dayBoundaryAdapJobs = ['Usage_TT_1_LoadJob','Usage_BCSI_1_LoadJob','Usage_BB_THRPT_1_LoadJob']
        subList = (subString.split(','))
        if len(subList) > 1:
                for idx,item in enumerate(subList):
                        if item.strip() in dayBoundaryAdapJobs:
                                break
                        else:
                                if (idx == 2 and item.strip() != '' and convertToDatetime(item.strip()) < threshold) or (idx == 2 and item.strip() == ''):
                                        item = item + 'Red'
                                        subList[idx] = item
                if subList[1].strip() in dayBoundaryAdapJobs and subList[2] != '' and subList[2].strip():
                        todaysDateTime = (datetime.datetime.now()).strftime('%Y-%m-%d 00:00:00')
                        todaysDateTimeObj = convertToDatetime(todaysDateTime)
                        actualDateTimeObj = convertToDatetime(subList[2].strip())
                        if actualDateTimeObj < todaysDateTimeObj:
                                subList[2] = subList[2] + 'Red'
                return ','.join(subList)

def firstDayOfWeek():
        firstDay = datetime.datetime.now() - datetime.timedelta(days=today.weekday())
        return firstDay.strftime("%Y%m%d")

def getDayJobStatus():
        filePath = checkFileExistence('{0}output/aggregations_Day/dayJobsStatus_{1}.csv'.format(logPath, today.strftime("%Y-%m")))
        if filePath and countLine(filePath) > 1:
                cmd = "tac %s | sed -n '1,/^day/p'" %(filePath)
                finalList = jobStatusList(cmd, int(getThreshold('DayJob')))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('DayJob Status',finalList))

def getWeekJobStatus():
        filePath = checkFileExistence('{0}output/aggregations_Week/weekJobsStatus_{1}.csv'.format(logPath, today.strftime("%Y-%m")))
        if filePath and countLine(filePath) > 1:
                cmd = "tac %s | sed -n '1,/^week/p'" %(filePath)
                finalList = jobStatusList(cmd, int(getThreshold('WeekJob')))
        else:
            finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('WeekJob Status',finalList))

def jobStatusList(cmd, threshold):
        status,getRows = commands.getstatusoutput(cmd)
        final_list = getRows.split('\n')
        final_list = final_list[::-1]
        finalList = []
        finalList.append(final_list[0].replace('\r',''))
        for subString in final_list[1:]:
                finalList.append(jobColorCoding(subString, threshold).replace('\r',''))
        return finalList

def jobColorCoding(subString, threshold):
        subList = subString.split(',')
        if subList[4].strip() == '' or int(subList[4].strip()) > threshold:
                subList[4] = subList[4] + 'Red'
        return ','.join(subList)

def getProcessMonitorOutput():
        yesterday = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        filePath = checkFileExistence('{0}output/processMonitor/processMonitor_{1}.csv'.format(logPath, yesterday))
        if filePath and countLine(filePath) > 1:
                getRows = commands.getoutput("cat %s |gawk -F\",\" '{ print $1\",\"$4\",\"$7\",\"$10\",\"$13\",\"$16\",\"$19\",\"$22\",\"$25\",\"$28\",\"$31\",\"$34\",\"$37\",\"$40\",\"$43\",\"$46}'" %(filePath))
                final_list = getRows.split('\n')
        else:
                final_list = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('Bad Records view', final_list))

def getURLStatus():
        filePath = checkFileExistence('{0}output/checkURL/checkURL_{1}.csv'.format(logPath, datetime.date.today()))
        if filePath and countLine(filePath) > 1:
                status,getRows = commands.getstatusoutput("tac %s | sed -n '1,/^Date/p'" %(filePath))
                final_list = getRows.split('\n')
                reverse_list = final_list[::-1]
                finalList = []
                finalList.append(reverse_list[0])
                for subString in reverse_list[1:]:
                        finalList.append(urlColorCoding(subString))
        else:
                finalList = ['File not present or is empty']
        return str(htmlUtil.generateHtmlFromList('UI Status',finalList))

def urlColorCoding(subString):
        subList = subString.split(',')
        if subList[2].strip() == 'DOWN' or subList[2].strip() == 'IP not configured':
                subList[2] = subList[2] + 'Red'
        return ','.join(subList)

def getQsTableLevel(qsDict):
        filePath = checkFileExistence('{0}{1}_{2}.csv'.format(logPath,qsDict[0],datetime.date.today().strftime("%Y%m%d")))
        if filePath and countLine(filePath) > 1:
                fileContent = commands.getoutput('cat {0}'.format(filePath))
                return str(htmlUtil.generateHtmlFromList(qsDict[1],fileContent.split('\n')))
        else:
                return str(htmlUtil.generateHtmlFromList(qsDict[1],['File not found or is empty']))


def getReaggBoundary():
        timePeriodList = ['15MIN','HOUR','DAY','WEEK']
        reaggHtml = ""
        for timePeriod in timePeriodList:
                filePath = checkFileExistence('{0}output/boundaryStatus/ReaggBoundaryStatus_{1}_{2}.csv'.format(logPath,timePeriod,today.strftime("%Y%m%d")))
                if filePath and countLine(filePath) > 1:
                        lastHourDateTime = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y-%m-%d %H:00:00')
                        lastHourDateTime = convertToDatetime(lastHourDateTime)
                        finalList = boundaryList(filePath, lastHourDateTime, "Reagg")
                        reaggHtml = reaggHtml + str(htmlUtil.generateHtmlFromList('{0} Reagg Boundary'.format(timePeriod),finalList))
                else:
                        finalList = ['File not present or is empty']
        return reaggHtml


def getNodeName():
        nodeName_cmd = ('ssh %s cat /opt/nsn/ngdb/ifw/etc/platform/platform_config.xml | grep "cluster_name" | gawk -F"\\"" \'{print $4}\''%cemod_hive_hosts.split()[0])
        return commands.getoutput(nodeName_cmd)

'''Execute the script'''

qsTableDict = { 'All': ['output/qsCache/expectedActualQsInfo_DAY','Qs Expected Vs Actual'],'Fail': ['output/qsCache/failedQsInfo_DAY','Qs Failed Information'] }
qsTableDictweek = { 'All': ['output/qsCache/expectedActualQsInfo_WEEK','Qs Expected Vs Actual'],'Fail': ['output/qsCache/failedQsInfo_WEEK','Qs Failed Information'] }
currentDay = commands.getoutput('date +%A')
html = ""
if is_kube_cluster_enabled == "no":
        html = getserviceStability()
html = html + getBacklog('Current Backlog') + getBacklog('Threshold Backlog')
if cemod_application_content_pack_ICE_status == "yes":
        html = html + getBacklogAE('Current Backlog for AE:') + getBacklogAE('Threshold Backlog for AE:')
html = html + getUsageBoundary() + getHourBoundary() + getDayBoundary() + getHourJobCount('Yesterday') + getDayJobStatus() + getfMinJobCount() + getDayJobCount() + getQSCacheLogs('QS Cache Logs DAY','%soutput/qsCache/QS_cache_count_DAY.csv'%logPath) + getQsTableLevel(qsTableDict['All']) + getQsTableLevel(qsTableDict['Fail']) + getURLStatus() + getProcessMonitorOutput() + getTableCountUsage() + getDayTableCount() + getReaggBoundary()
if is_kube_cluster_enabled == "no":
        html = html + getTnplatency()
if currentDay == 'Monday':
        html = html + getWeekBoundary() + getWeekJobStatus() + getWeekJobCount() + getWeekTableCount() + getQsTableLevel(qsTableDictweek['All']) + getQsTableLevelweek(qsTableDict['Fail'])
EmailUtils().frameEmailAndSend("{0} Summary Report".format(getNodeName()),html)

date = datetime.datetime.now()
date = date.strftime("%d-%m-%Y-%H-%M-%S")
filePath=('%s/sendSummaryReport_%s.html'%(outputFilePath,date))
sendFile = open(filePath, 'w')
sendFile.write(html)
sendFile.close()

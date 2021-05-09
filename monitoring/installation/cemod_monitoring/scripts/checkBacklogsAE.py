#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:
# Version: 0.1
# Purpose: This script gets the count of backlogs, checks the threshold count
# and raise an alert if breached for AE.
# Date:    28-09-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality of getting count, checking threshold
# and raising alert
# 2.
#############################################################################
import commands,os, multiprocessing, time, datetime, sys
from xml.dom import minidom
#from sendMailUtil import EmailUtils
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
global adaptationsConfigured
configurationXml = '/opt/nsn/ngdb/monitoring/conf/monitoring.xml'
mntImportDirectory = '/mnt/staging/import/AE'
outputDirectory = '/var/local/monitoring/output/backlogHadoop'
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

def getCountWriteCsvAndAlert(adaptations):
        global breachedBacklog
        countDictionary = {}
        breachedBacklog = {}
        header = 'Time,DataSource,datFileCountMnt(/mnt/staging/import),processingCtrFileCountMnt(/mnt/staging/import) \n'
        currentDate = datetime.date.today().strftime('%Y%m%d')
        completeFileName = outputDirectory + '/backlogsAE.' + currentDate +'.csv'
        headerWriter = open(completeFileName,'a')
        headerWriter.write(header)
        headerWriter.close()
        for adaptation in adaptations:
                print 'Checking the count for the adatation '+adaptation
                xmlparser = minidom.parse(configurationXml)
                backlogTag = xmlparser.getElementsByTagName('BACKLOGAE')
                inputTagList = backlogTag[0].getElementsByTagName('Input')
                for inputTag in inputTagList:
                        if adaptation == inputTag.attributes['Name'].value:
                                importDirectoryFromConf = inputTag.attributes['Import'].value
                                thresholdValueFromConf = inputTag.attributes['Thresholdlimit'].value
                                mntStatus,mntImportCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s/*dat 2>> /dev/null | wc -l" '%(cemod_hdfs_user,mntImportDirectory,importDirectoryFromConf))
                                if "No such file" in mntImportCount:
                                       mntImportCount='0'
                                countDictionary['mntStatus'] = mntImportCount
                                mntProcStatus,mntProcessingCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s/*proc 2>> /dev/null | wc -l" '%(cemod_hdfs_user,mntImportDirectory,importDirectoryFromConf))
                                if "No such file" in mntProcessingCount:
                                       mntProcessingCount='0'
                                countDictionary['mntProcStatus'] = mntProcessingCount

                                writeToCsv(adaptKey=adaptation,mntImpKey=mntImportCount,mntProcKey=mntProcessingCount)
                                for key, value in countDictionary.iteritems():
                                        if int(value) > int(thresholdValueFromConf):
                                                breachedBacklog [adaptation+'_'+str(key)] = {key:value}
        return breachedBacklog

def sendAlert(alertBacklogDict):
                if alertBacklogDict:
                        html = '''<html><head><style>
                        table, th, td {
                        border: 1px solid black;
                        border-collapse: collapse;
                         }
                        </style></head><body>'''
                        for key, value in alertBacklogDict.iteritems():
                                if isinstance(value, dict):
                                        for innerKey, innerValue in alertBacklogDict[key].iteritems():
                                                html += '<h4>Backlog count is high for the adaptation %s and the value is %s </h4>' %(str(key),str(innerValue))
                                        html = html.replace('_mntStatus',' in the location /mnt/staging/import/AE').replace('_mntProcStatus',' in the location /mnt/staging/import/AE/*proc*')
                                else:
                                        print 'No inner dictionary'
                        html +=  '''<br></body></html>'''                        
			EmailUtils().frameEmailAndSend("[CRITICAL_ALERT]:Backlog Alert on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)
                else:
                        print 'No values in the alertBacklogDict, so no need to send alert'

def writeToCsv(**kwargs):
        currentDate = datetime.date.today().strftime('%Y%m%d')
        currentTime = datetime.datetime.now().strftime('%H:%M:%S')
        completeFileName = outputDirectory + '/backlogsAE.' + currentDate +'.csv'
        status,fileCheck = commands.getstatusoutput('ls '+completeFileName)
        if status ==0:
                fileContent = currentTime+','+kwargs['adaptKey']+','+kwargs['mntImpKey']+','+kwargs['mntProcKey']+ '\n'
                fileOpen = open(completeFileName,'a')
                fileOpen.write(fileContent)
                fileOpen.close()
        else:
                print 'File not found'

def getAdaptationsConfigured():
        adaptationsConfigured = []
        xmlparser = minidom.parse(configurationXml)
        backlogTag = xmlparser.getElementsByTagName('BACKLOGAE')
        inputTagList = backlogTag[0].getElementsByTagName('Input')
        for tags in inputTagList:
                adaptationsConfigured.append(tags.attributes['Name'].value)
        return adaptationsConfigured

def processCheck():
        processCmd = 'ps -eaf | grep "%s" | grep -v grep | gawk -F" " \'{print $2}\''%(os.path.basename(__file__))
        process = commands.getoutput(processCmd).split('\n')
        print 'Process id is',process
        if len(process) > 2:
                exit(0)
        else:
                print 'No existing process is running, continues with execution'

def main():
        processCheck()
        adaptations=getAdaptationsConfigured()
        thresholdBreached = getCountWriteCsvAndAlert(adaptations)
        sendAlert(breachedBacklog)

if __name__ == "__main__":
        main()
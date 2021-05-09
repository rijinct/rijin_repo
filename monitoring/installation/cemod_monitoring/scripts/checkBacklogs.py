#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author: deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script gets the count of backlogs, checks the threshold count
# and raise an alert if breached.
# Date:    28-09-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality of getting count, checking threshold
# and raising alert
# 2.
#############################################################################
import commands,datetime,smtplib, os, sys
from xml.dom import minidom
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from snmpUtils import SnmpUtils
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from propertyFileUtil import PropertyFileUtil

global adaptationsConfigured
configurationXml = '/opt/nsn/ngdb/monitoring/conf/monitoring.xml'
mntImportDirectory = '/mnt/staging/import'
ngdbImportDirectory = '/ngdb/us/import'
ngdbWorkDirectory = '/ngdb/us/work'
outputDirectory = '/var/local/monitoring/output/backlogHadoop'
commonXml = '/opt/nsn/ngdb/ifw/etc/common/common_config.xml'
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

def getCountWriteCsvAndAlert(adaptations):
        global breachedBacklog,currentDate
        countDictionary = {}
        breachedBacklog = {}
        header = 'Time,DataSource,datFileCountMnt(/mnt/staging/import),errorDatFileCountMnt(/mnt/staging/import),processingDatFileCountMnt(/mnt/staging/import),datFileCountNgdbUsage(/ngdb/us/import),datFileCountNgdbWork(/ngdb/us/work) \n'
        currentDate = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
        completeFileName = outputDirectory + '/backlogs.' + currentDate +'.csv'
        headerWriter = open(completeFileName,'a')
        headerWriter.write(header)
        headerWriter.close()
        for adaptation in adaptations:
                xmlparser = minidom.parse(configurationXml)
                backlogTag = xmlparser.getElementsByTagName('BACKLOG')
                inputTagList = backlogTag[0].getElementsByTagName('Input')
                for inputTag in inputTagList:
                        if adaptation == inputTag.attributes['Name'].value:
                                importDirectoryFromConf = inputTag.attributes['Import'].value
                                ngdbDirectoryFromConf = inputTag.attributes['Ngdb'].value
                                thresholdValueFromConf = inputTag.attributes['Thresholdlimit'].value
                                mntStatus,mntImportCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s/*dat  2>> /dev/null | wc -l" '%(cemod_hdfs_user,mntImportDirectory,importDirectoryFromConf))
                                mntImportCount = mntImportCount.split(" ")[0].split("\n")[0]
                                if "No such file" in mntImportCount or " " in mntImportCount:
                                       mntImportCount='0'
                                countDictionary['mntStatus'] = mntImportCount
                                mntErrorStatus,mntImportErrorCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s/*error 2>> /dev/null | wc -l" '%(cemod_hdfs_user,mntImportDirectory,importDirectoryFromConf))
                                mntImportErrorCount = mntImportErrorCount.split(" ")[0].split("\n")[0]
                                if "No such file" in mntImportErrorCount or " " in mntImportErrorCount:
                                       mntImportErrorCount='0'
                                countDictionary['mntErrorStatus'] = mntImportErrorCount
                                mntProcStatus,mntProcessingCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s/*processing  2>> /dev/null | wc -l" '%(cemod_hdfs_user,mntImportDirectory,importDirectoryFromConf))
                                mntProcessingCount = mntProcessingCount.split(" ")[0].split("\n")[0]
                                if "No such file" in mntProcessingCount or " " in mntProcessingCount:
                                       mntProcessingCount='0'
                                countDictionary['mntProcStatus'] = mntProcessingCount
                                ngdbUsStatus,ngdbUsCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s|grep .dat  2>> /dev/null | wc -l| tail -1 "'%(cemod_hdfs_user,ngdbImportDirectory,ngdbDirectoryFromConf))
                                ngdbUsCount = ngdbUsCount.split("\n")[-1]
                                if "No such file" in ngdbUsCount or " " in ngdbUsCount:
                                       ngdbUsCount='0'
                                countDictionary['ngdbUsStatus'] = ngdbUsCount
                                ngdbUsWorkStatus,ngdbUsWorkCount = commands.getstatusoutput('su - %s -c "/opt/nsn/ngdb/hadoop/bin/hdfs dfs -ls -R %s/%s|grep .dat 2>> /dev/null | wc -l | tail -1" '%(cemod_hdfs_user,ngdbWorkDirectory,ngdbDirectoryFromConf))
                                ngdbUsWorkCount = ngdbUsWorkCount.split("\n")[-1]
                                if "No such file" in ngdbUsWorkCount  or " " in ngdbUsWorkCount:
                                       ngdbUsWorkCount='0'
                                countDictionary['ngdbUsWorkStatus'] = ngdbUsWorkCount
                                writeToCsv(adaptKey=adaptation,mntImpKey=mntImportCount,mntErrKey=mntImportErrorCount,mntProcKey=mntProcessingCount,ngdbUsKey=ngdbUsCount,ngdbWorkKey=ngdbUsWorkCount)
                                for key, value in countDictionary.iteritems():
                                        if int(value) > int(thresholdValueFromConf):
                                                breachedBacklog [adaptation+'_'+str(key)] = {key:value}
        return breachedBacklog

def frameSnmpContent(breachedBacklog):
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::backlogHadoop SAI-MIB::Adaptations s "{3}"'.format(snmpCommunity,snmpIp,snmpPort,breachedBacklog))
        if str(status) == '0':
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'

def sendAlert(alertBacklogDict):
        if alertBacklogDict:
                xmlparser = minidom.parse(commonXml)
                smtpTag = xmlparser.getElementsByTagName('SMTP')
                propertySmtpTag = smtpTag[0].getElementsByTagName('property')
                for property in propertySmtpTag:
                        for propertynum in range(len(propertySmtpTag)):
                                if "IP" == propertySmtpTag[propertynum].attributes['name'].value:
                                        smtpIp = propertySmtpTag[propertynum].attributes['value'].value
                                if "SenderEmailID" == propertySmtpTag[propertynum].attributes['name'].value:
                                        sender = propertySmtpTag[propertynum].attributes['value'].value
                                if "RecepientEmailIDs" == propertySmtpTag[propertynum].attributes['name'].value:
                                        receivers = propertySmtpTag[propertynum].attributes['value'].value
                                        receipientList = receivers.split(";")
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
                                html = html.replace('_mntStatus',' in the location /mnt/staging/import').replace('_mntErrorStatus',' in the location /mnt/staging/import/*error*').replace('_mntProcStatus',' in the location /mnt/staging/import/*processing*').replace('_ngdbUsStatus',' in the location /ngdb/us/import/').replace('_ngdbUsWorkStatus',' in the location /ngdb/us/work/')
                        else:
                                print 'No inner dictionary'
                html +=  '''<br></body></html>'''
                message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: ("[CRITICAL_ALERT]:Backlog Alert on %s")

<b>****AlertDetails****</b>
%s


""" %(sender,receipientList,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),html)
                try:
                        smtpObj = smtplib.SMTP(smtpIp)
                        smtpObj.sendmail(sender, receipientList, message)
                        smtpObj.quit()
                except smtplib.SMTPException:
                        print 'Error: Unable to send email alert'

        else:
                print 'No values in the alertBacklogDict, so no need to send alert'

def pushDataToPostgresDB():
        global currentDate
        outputFilePath = PropertyFileUtil('backlog','DirectorySection').getValueForKey()
        fileName = outputFilePath + "backlogs.{0}.csv".format(currentDate)
        jsonFileName=JsonUtils().convertCsvToJson(fileName)
        DBUtil().pushDataToPostgresDB(jsonFileName,"backlog")

def writeToCsv(**kwargs):
        global currentDate
        currentTime = datetime.datetime.now().strftime('%H:%M:%S')
        completeFileName = outputDirectory + '/backlogs.' + currentDate +'.csv'
        status,fileCheck = commands.getstatusoutput('ls '+completeFileName)
        if status ==0:
                fileContent = currentTime+','+kwargs['adaptKey']+','+kwargs['mntImpKey']+','+kwargs['mntErrKey']+','+kwargs['mntProcKey']+','+kwargs['ngdbUsKey']+','+kwargs['ngdbWorkKey'] + '\n'
                fileOpen = open(completeFileName,'a')
                fileOpen.write(fileContent)
                fileOpen.close()
        else:
                print 'File not found'

def getAdaptationsConfigured():
        adaptationsConfigured = []
        xmlparser = minidom.parse(configurationXml)
        backlogTag = xmlparser.getElementsByTagName('BACKLOG')
        inputTagList = backlogTag[0].getElementsByTagName('Input')
        for tags in inputTagList:
                adaptationsConfigured.append(tags.attributes['Name'].value)
        return adaptationsConfigured

def processCheck():
        processCmd = 'ps -eaf | grep "%s" | grep -v grep | gawk -F" " \'{print $2}\''%(os.path.basename(__file__))
        process = commands.getoutput(processCmd).split('\n')
        if len(process) > 3:
                exit(0)

def main():
        processCheck()
        adaptations=getAdaptationsConfigured()
        thresholdBreached = getCountWriteCsvAndAlert(adaptations)
        pushDataToPostgresDB()
        if thresholdBreached:
                print 'Breached Backlogs Threshold',thresholdBreached
                sendAlert(breachedBacklog)
                frameSnmpContent(breachedBacklog)
                exit(2)
        else:
                print 'No backlogs breached threshold'
                exit(0)

if __name__ == "__main__":
        main()

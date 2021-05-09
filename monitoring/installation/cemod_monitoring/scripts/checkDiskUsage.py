############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: deeraj.kumar_y_m_v@nokia.com
# Version: 1.0
# Date:12-10-2017
# Purpose :  This script checks the disk usage of all disks present in all
# the node of the platform Cluster
#############################################################################
# Code Modification History
# 1. First Draft
#############################################################################
from commands import *
from xml.dom import minidom
import datetime,os,smtplib
from collections import OrderedDict

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))

def getPlatformClusterNodes():
        return getoutput('ssh %s \'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${cemod_platform_hosts[@]}\' '%(cemod_hive_hosts.split(' ')[0])).split(' ')

def getApplicationClusterNodes():
        return cemod_application_hosts.split(' ')

def getDiskUsageThresholdValue():
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('DiskUsage')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                diskUsageThresholdValue = propertyelements.attributes['diskusagethreshold'].value
        return int(diskUsageThresholdValue)

def writeDiskUsageToCsv(nodes):
        global outputCsvWriter
        outputCsv = '/var/local/monitoring/output/diskSpaceUtilization/diskUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d"))
        outputCsvWriter = open(outputCsv,'a+')
        writeHeaderIfRequired(outputCsv)
        writeDiskUsageContent(getDiskUsage(nodes))
        outputCsvWriter.close()

def writeHeaderIfRequired(outputCsv):
        global outputCsvWriter
        if os.path.getsize(outputCsv) == 0:
                header = 'Time, Node, DiskName, UtilizedCapacity \n'
                outputCsvWriter.write(header)

def writeDiskUsageContent(diskUsageAllNodesDict):
        global outputCsvWriter
        for key, value in diskUsageAllNodesDict.iteritems():
                content = '%s,%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),key[0],key[1],value)
                outputCsvWriter.write(content+'\n')

def getDiskUsage(nodes):
        global thresholdDiskUsage
        diskUsageNodeDict = OrderedDict()
        for node in nodes:
                for disk in getDiskUsageForNode(node):
                        try:
                                diskName,diskUtilizationValue = disk.split(',')
                        except ValueError:
                                pass
                        diskUsageNodeDict[node,diskName] = diskUtilizationValue
        checkThreshold(diskUsageNodeDict)
        return diskUsageNodeDict

def getDiskUsageForNode(node):
                return getoutput('ssh %s "ssh %s df -PH |grep -v Mounted|gawk -F\\" \\" \'{print \\$6\\",\\"\\$5}\' " '%(cemod_hive_hosts.split(' ')[0],node)).split('\n')

def checkThreshold(diskUsageNodeDict):
        thresholdDiskUsage = {}
        diskUsageThresholdValue = getDiskUsageThresholdValue()
        for key, value in diskUsageNodeDict.iteritems():
                diskUtilizationValue = int(value.rstrip('%'))
                if diskUtilizationValue >= diskUsageThresholdValue:
                        thresholdDiskUsage[key] = str(diskUtilizationValue)+'%'
        sendThresholdValuesToFrameEmail(thresholdDiskUsage)

def sendThresholdValuesToFrameEmail(thresholdDiskUsage):
        if thresholdDiskUsage:
                getEmailParamsAndSendEmail(thresholdDiskUsage)

def getEmailParamsAndSendEmail(message):
        commonXml = '/opt/nsn/ngdb/ifw/etc/common/common_config.xml'
        xmlparser = minidom.parse(commonXml)
        smtpTag = xmlparser.getElementsByTagName('SMTP')
        propertySmtpTags = smtpTag[0].getElementsByTagName('property')
        smtpIp,sender,receipientList = getEmailDetails(propertySmtpTags)
        sendEmail(frameMessage(message),sender,receipientList,smtpIp)

def getEmailDetails(propertySmtpTags):
        for propertynum in range(len(propertySmtpTags)):
                if "IP" == propertySmtpTags[propertynum].attributes['name'].value:
                        smtpIp = propertySmtpTags[propertynum].attributes['value'].value
                if "SenderEmailID" == propertySmtpTags[propertynum].attributes['name'].value:
                        sender = propertySmtpTags[propertynum].attributes['value'].value
                if "RecepientEmailIDs" == propertySmtpTags[propertynum].attributes['name'].value:
                        receivers = propertySmtpTags[propertynum].attributes['value'].value.split(";")
        return smtpIp,sender,receivers

def frameMessage(message):
        html = '''<html><head><style>
             table, th, td {
             border: 1px solid black;
             border-collapse: collapse;
             }
             </style></head><body>'''
        for key, value in message.iteritems():
                html += '<h4>Disk Utilization is high on the node %s for the disk %s and current capacity is %s </h4>' %(str(key[0]),str(key[1]),str(value))
        html +=  '''<br></body></html>'''
        return html

def sendEmail(html,sender,receipientList,smtpIp):
        message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: ("[CRITICAL_ALERT]:Disk Usage Alert on %s")

<b>****AlertDetails****</b>
%s


""" %(sender,receipientList,  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),html)
        try:
                print 'Message to send is',message
                smtpObj = smtplib.SMTP(smtpIp)
                smtpObj.sendmail(sender, receipientList, message)
                smtpObj.quit()
        except smtplib.SMTPException:
                print 'Error: Unable to send email alert'

def main():
        writeDiskUsageToCsv(getPlatformClusterNodes())
        writeDiskUsageToCsv(getApplicationClusterNodes())
if __name__ == "__main__":
        main()
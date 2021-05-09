############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring Team
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
import datetime,os,smtplib,sys
from collections import OrderedDict
sys.path.insert(0,'/opt/nsn/rtb/monitoring/utils')
from writeCsvUtils import CsvWriter

def getRtbClusterNodes():
        clusterNodes = []
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        platformClusterNodes = xmlparser.getElementsByTagName('HostNames')
        propertyTag=platformClusterNodes[0].getElementsByTagName('Host')
        for propertyelements in propertyTag:
                clusterNodes.append(propertyelements.attributes['IPADDR'].value)
        return clusterNodes

def getDiskUsageThresholdValue():
        xmlparser = minidom.parse('/opt/nsn/rtb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('DiskUsage')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                diskUsageThresholdValue = propertyelements.attributes['diskusagethreshold'].value
        return int(diskUsageThresholdValue)

def writeDiskUsageToCsv(nodes):
        csvWriter = CsvWriter('DiskUsage','diskUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d")))
        writeDiskUsageContent(getDiskUsage(nodes))

def writeDiskUsageContent(diskUsageAllNodesDict):
        global outputCsvWriter
        for key, value in diskUsageAllNodesDict.iteritems():
                content = '%s,%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),key[0],key[1],value)
                csvWriter = CsvWriter('DiskUsage','diskUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)

def getDiskUsage(nodes):
        diskUsageNodeDict = OrderedDict()
        for node in nodes:
                for disk in getDiskUsageForNode(node):
                        diskName,diskUtilizationValue = disk.split(',')
                        diskUsageNodeDict[node,diskName] = diskUtilizationValue
        checkThreshold(diskUsageNodeDict)
        return diskUsageNodeDict

def getDiskUsageForNode(node):
                return getoutput('ssh %s "df -PH |grep -v Mounted|gawk -F\\" \\" \'{print \\$6\\",\\"\\$5}\' " '%(node)).split('\n')

def checkThreshold(diskUsageNodeDict):
        thresholdDiskUsage = {}
        diskUsageThresholdValue = getDiskUsageThresholdValue()
        for key, value in diskUsageNodeDict.iteritems():
                diskUtilizationValue = int(value.rstrip('%'))
                if diskUtilizationValue >= getDiskUsageThresholdValue:
                        thresholdDiskUsage[key] = value
        sendThresholdValuesToFrameEmail(thresholdDiskUsage)

def sendThresholdValuesToFrameEmail(thresholdDiskUsage):
        if thresholdDiskUsage:
                getEmailParamsAndSendEmail(thresholdDiskUsage)

def getEmailParamsAndSendEmail(message):
        commonXml = '/opt/nsn/rtb/monitoring/conf/monitoring.xml'
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
Subject: Disk Usage Alert:

<b>****AlertDetails****</b>
%s


""" %(sender,receipientList,html)
        try:
                print 'Message to send is',message
                smtpObj = smtplib.SMTP(smtpIp)
                smtpObj.sendmail(sender, receipientList, message)
                smtpObj.quit()
        except smtplib.SMTPException:
                print 'Error: Unable to send email alert'

def main():
        writeDiskUsageToCsv(getRtbClusterNodes())
if __name__ == "__main__":
        main()

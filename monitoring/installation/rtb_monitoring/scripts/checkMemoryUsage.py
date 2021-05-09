############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring team
# Version: 1.0
# Date:26-11-2017
# Purpose :  This script checks the memory usage of all disks present in all
# the node of the rtb Cluster
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

def writeMemoryUsageToCsv(nodes):
        csvWriter = CsvWriter('MemoryUsage','memoryUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d")))
        writeMemoryUsageContent(nodes)

def writeMemoryUsageContent(nodes):
        thresholdMemoryUsage = {}
        for node in nodes:
                freeMemory = getoutput('ssh %s "free -g | gawk -F\\" \\" \'{print \\$4}\' | grep -v shared | head -1"' %(node))
                thresholdMemoryUsage[node,'Free Memory'] = freeMemory
                buffersCache = getoutput('ssh %s "free -g | gawk -F\\" \\" \'{print \\$4}\' | grep -v shared | head -2 | tail -1"' %(node))
                thresholdMemoryUsage[node,'Free Buffer Cache'] = buffersCache
                content = '%s,%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),node,freeMemory,buffersCache)
                csvWriter = CsvWriter('MemoryUsage','memoryUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
        checkThreshold(thresholdMemoryUsage)

def checkThreshold(memoryUsageNodeDict):
        thresholdBreachedMemoryUsage = {}
        for key, value in memoryUsageNodeDict.iteritems():
                memoryFreeValue = int(value)
                if memoryFreeValue <= 3:
                        thresholdBreachedMemoryUsage[key[0],key[1]] = memoryFreeValue
        sendThresholdValuesToFrameEmail(thresholdBreachedMemoryUsage)

def sendThresholdValuesToFrameEmail(thresholdMemoryUsage):
        if thresholdMemoryUsage:
                getEmailParamsAndSendEmail(thresholdMemoryUsage)

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
                html += '<h4>Node %s has %s of  %s </h4>' %(str(key[0]),str(key[1]),str(value))
        html +=  '''<br></body></html>'''
        return html

def sendEmail(html,sender,receipientList,smtpIp):
        message = """From: %s
To: %s
MIME-Version: 1.0
Content-type: text/html
Subject: Memory Usage Alert:

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
        writeMemoryUsageToCsv(getRtbClusterNodes())
if __name__ == "__main__":
        main()
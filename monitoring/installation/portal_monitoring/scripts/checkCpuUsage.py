############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring team
# Version: 1.0
# Date:10-01-2018
# Purpose :  This script checks the CPU usage of all Nodes of the Portal Cluster

#############################################################################
# Code Modification History
# 1. First Draft
#############################################################################
from commands import *
from xml.dom import minidom
import datetime,os,smtplib,sys
from collections import OrderedDict
sys.path.insert(0,'/opt/portal/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils

def getPortalClusterNodes():
        clusterNodes = []
        xmlparser = minidom.parse('/home/portal/ifw/config/ifw_portal_config.xml')
        portalClusterNodes = xmlparser.getElementsByTagName('Hosts')
        propertyTag=portalClusterNodes[0].getElementsByTagName('Host')
        for propertyelements in propertyTag:
                clusterNodes.append(propertyelements.attributes['PUBLIC_DOMAIN_IP'].value)
        return clusterNodes

def writeCpuUsageContent(nodes):
        cpuThreshold = {}
        cpuMap = {0:'User CPU',1:'System CPU',2:'IO wait'}
        for node in nodes:
                cpuUsageCmd = 'ssh %s "top -b -n 1 | grep \\"Cpu(s)\\" | gawk -F\\" \\" \'{print \\$2\\" \\"\\$3\\" \\"\\$4\\" \\"\\$5\\" \\"\\$10\\" \\"\\$11}\' | tr \-d \'%s\' "'%(node,"%us|y|ni|d|wa")
                cpuUsage = getoutput(cpuUsageCmd).rstrip(',')
                content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),node,cpuUsage)
                csvWriter = CsvWriter('cpuUsage','CpuUsage_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                cpuUsageList = cpuUsage.split(',')
                cpuUsageList.pop(2)
                for item in range(0,len(cpuUsageList)):
                        if float(cpuUsageList[item]) >= 90.0:
                                cpuThreshold[cpuMap[item]+'_'+node] = cpuUsageList[item]
                        else:
                                print 'Cpu is in safe zone'
        frameMessage(cpuThreshold)


def checkThreshold(CpuUsageNodeDict):
        thresholdBreachedCpuUsage = {}
        for key, value in CpuUsageNodeDict.iteritems():
                print 'cpu Value in checkThreshold',value
                cpuValue = float(value.rstrip('%'))
                if cpuValue >= 90.0:
                        thresholdBreachedCpuUsage[key[0],key[1]] = cpuValue
        sendThresholdValuesToFrameEmail(thresholdBreachedCpuUsage)


def frameMessage(cpuThresholdDict):
        html = '''<html><head><style>
             table, th, td {
             border: 1px solid black;
             border-collapse: collapse;
             }
             </style></head><body>'''
        if cpuThresholdDict:
                for key, value in cpuThresholdDict.iteritems():
                        print 'Key is',key,'Value is',value
                        html += '<h4>%s on node %s is high and value is %s</h4>' %(key.split('_')[0],key.split('_')[1],value)
                html +=  '''<br></body></html>'''
                EmailUtils().frameEmailAndSend("CPU High Alert:",html)

def main():
        writeCpuUsageContent(getPortalClusterNodes())
if __name__ == "__main__":
        main()



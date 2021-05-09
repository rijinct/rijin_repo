############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring team
# Version: 1.0
# Date:10-02-2018
# Purpose :  This script checks the CPU usage of all Nodes on a RTB Cluster

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
from sendMailUtil import EmailUtils

def getRtbClusterNodes():
        clusterNodes = []
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        platformClusterNodes = xmlparser.getElementsByTagName('HostNames')
        propertyTag=platformClusterNodes[0].getElementsByTagName('Host')
        for propertyelements in propertyTag:
                clusterNodes.append(propertyelements.attributes['IPADDR'].value)
        return clusterNodes

def writeCpuUsageContent(nodes):
        cpuThreshold = {}
        cpuMap = {0:'User CPU',1:'System CPU',2:'IO wait'}
        for node in nodes:
                cpuUsageCmd = 'ssh %s "top -b -n 1 | grep \\"Cpu(s)\\" | gawk -F\\" \\" \'{print \\$2\\" \\"\\$3\\" \\"\\$5\\" \\"\\$6}\' | tr \-d \'%s\' "'%(node,"%us|y|ni|d|wa")
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
        writeCpuUsageContent(getRtbClusterNodes())
if __name__ == "__main__":
        main()


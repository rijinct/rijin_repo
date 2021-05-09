#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script monitors different application services
# and writes to a CSV file
# Date: 07-12-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with RTB & workers as services monitoring
#
# 2.
#############################################################################
from xml.dom import minidom
import commands, datetime, os, sys
sys.path.insert(0,'/opt/nsn/rtb/monitoring/utils')
from writeCsvUtils import CsvWriter

def getLastRestartTime(floatingIp):
        processId = commands.getoutput('ssh %s "jps | grep -i \"CodeServerMain\" | gawk -F\\" \\" \'{print \\$1}\' "'%(floatingIp))
        if processId:
                restartTime = commands.getoutput('ssh %s "ps -o lstart= -p %s" '%(floatingIp,processId))
                restartTimeDate = datetime.datetime.strptime(restartTime, "%a %b %d %H:%M:%S %Y")
                return restartTime,getUptime(restartTimeDate)
        else:
                print 'Process Id is empty, so service is down'

def getLastRestartTimeOfWorkers(workerNodeIps):
        for workerNode in workerNodeIps:
                processIdList = commands.getoutput('ssh %s "jps | grep -i \"PicoStart\" | gawk -F\\" \\" \'{print \\$1}\' " '%(workerNode))
                for processId in processIdList.split('\n'):
                        restartTime = commands.getoutput('ssh %s "ps -o lstart= -p %s" '%(workerNode,processId))
                        restartTimeDate = datetime.datetime.strptime(restartTime, "%a %b %d %H:%M:%S %Y")
                        uptime = getUptime(restartTimeDate)
                        writeContentToCsv(workerNode,'Worker'+'_'+processId,restartTime,uptime)


def getUptime(processRestartTime):
        currentDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        currentDateFormat = datetime.datetime.strptime(currentDate, "%Y-%m-%d %H:%M:%S")
        uptime = currentDateFormat - processRestartTime
        return uptime

def getFloatingIp(typeOfService):
        xmlparser = minidom.parse('/opt/nsn/ifw/config/ha_rtb_config.xml')
        parentFloatingIpsTag = xmlparser.getElementsByTagName('FloatingIps')
        propertyTag=parentFloatingIpsTag[0].getElementsByTagName('FloatingIp')
        for propertyelements in propertyTag:
                if propertyelements.attributes['type'].value == typeOfService:
                        floatingIp = propertyelements.attributes['value'].value
                        return floatingIp

def getWorkerNodesIp():
        workerIpAdd = []
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        parentHostNamesElements = xmlparser.getElementsByTagName('HostNames')
        childHostsElements = parentHostNamesElements[0].getElementsByTagName('Host')
        for childHostsElement in childHostsElements:
                if childHostsElement.attributes['Nodetype'].value == 'worker':
                        workerIpAdd.append(childHostsElement.attributes['IPADDR'].value)
        return workerIpAdd

def getPlatformNodesIp():        
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        parentHostNamesElements = xmlparser.getElementsByTagName('HostNames')
        childHostsElements = parentHostNamesElements[0].getElementsByTagName('Host')
        for childHostsElement in childHostsElements:
                if childHostsElement.attributes['Nodetype'].value == 'platform':                        
                        platformIp = childHostsElement.attributes['IPADDR'].value
                        return platformIp

def checkDeploymentAndGetIp():
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        deploymentTag = xmlparser.getElementsByTagName('config')
        childHostsElements = deploymentTag[0].getElementsByTagName('Configuration')
        deploymentType = childHostsElements[0].attributes['isHa'].value
        print "value of deploymenttype" ,deploymentType
        if deploymentType == "yes":
                return getFloatingIp('RTB')
        else:
                return getPlatformNodesIp()


def writeContentToCsv(hostIp,service,lastRestartTime,uptime):
        content = '%s,%s,%s,%s,%s'%(datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S"),service,hostIp,lastRestartTime,uptime)
        csvWriter = CsvWriter('ServiceStability','serviceStability_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)

def main():
        hostIp = checkDeploymentAndGetIp()
        restartTime,uptime = getLastRestartTime(hostIp)
        csvWriter = CsvWriter('ServiceStability','serviceStability_%s.csv'%(datetime.date.today().strftime("%Y%m%d")))
        writeContentToCsv(hostIp,'RTB',restartTime,uptime)
        workerNodeIps = getWorkerNodesIp()
        getLastRestartTimeOfWorkers(workerNodeIps)


if __name__ == "__main__":
        main()

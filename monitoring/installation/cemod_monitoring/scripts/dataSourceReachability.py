#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: This script monitors RTB Data source Reachability
# and writes to a CSV file
# Date: 07-02-2017
#############################################################################
#############################################################################
# Code Modification History
# 1.
# 2.
#############################################################################
from xml.dom import minidom
import commands, datetime, os, sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils
from htmlUtil import HtmlUtil

def checkReachabilityOfRTBSource(RTBSourcesIpAdds):
        nonReachableMachines = []
        nonReachableMachines.append("List Of IPs")
        for RTBSourcesIpAdd in RTBSourcesIpAdds:
                rtbSourcePingCmd = 'ping \\-c 1 %s | grep "packet loss" | gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' '%(RTBSourcesIpAdd,"%")
                rtbSourcePingOutput = commands.getoutput(rtbSourcePingCmd)
                try:
                        if int(rtbSourcePingOutput) != 0:
                                content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),RTBSourcesIpAdd,"Not Reachable")
                                csvWriter = CsvWriter("DataSourceReachability",'RTBMachinesReachability_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                                nonReachableMachines.append(RTBSourcesIpAdd)
                        else:
                                print 'RTB Machine {0} is reachable'.format(RTBSourcesIpAdd)
                except ValueError:
                        print 'IP Address {0} is not valid, add a Valid IP Address in the monitoring.xml'.format(RTBSourcesIpAdd)
        print 'Non Reachable Machines are',nonReachableMachines
        if len(nonReachableMachines) > 1:
                html = HtmlUtil().generateHtmlFromList('Below machines are not reachable', nonReachableMachines)
                EmailUtils().frameEmailAndSend("Non Reachable Machines",html)

def getRtbSourcesIp():
        RTBSourcesIpAdd = []
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentHostNamesElements = xmlparser.getElementsByTagName('RtbSources')
        childHostsElements = parentHostNamesElements[0].getElementsByTagName('Host')
        for childHostsElement in childHostsElements:
                        RTBSourcesIpAdd.append(childHostsElement.attributes['IPADDR'].value)
        return RTBSourcesIpAdd


def main():
        checkReachabilityOfRTBSource(getRtbSourcesIp())

if __name__ == "__main__":
        main()

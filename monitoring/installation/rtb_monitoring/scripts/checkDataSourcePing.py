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
# 1. First draft with RTB Data source and Cemod Reachability monitoring
#
# 2.
#############################################################################
from xml.dom import minidom
import commands, datetime, os, sys
sys.path.insert(0,'/opt/nsn/rtb/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils


def frameEmailContent(nonReachableMachines):
        label = 'Below machines are not reachable'
        header = '<th bgcolor="#C7D7F1"> Machine IP </th>'
        html = '''<html><head><style>
             table, th, td {
             border: 1px solid black;
             border-collapse: collapse;
             }
             </style></head><body>'''
        for nonReachableMachine in nonReachableMachines:
                html = html + '<tr>'
                for job in nonReachableMachine.split(','):
                        html = html + '<td>' + job.strip() + '</td>'
                html = html + '\n</tr>'
        html +=  '''<br></body></html>'''
        htmlMessage = '<h3>%s</h3><table><tr>%s</tr>%s</table><br />' %(label,header,html)
        EmailUtils().frameEmailAndSend("Non Reachable Machines",htmlMessage)

def checkReachabilityOfDataSource(DataSourcesIpAdds):
        nonReachableMachines = []
        for DataSourcesIpAdd in DataSourcesIpAdds:
                dataSourcePingCmd = 'ping \\-c 1 %s | grep "packet loss" | gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' '%(DataSourcesIpAdd,"%")
                dataSourcePingOutput = commands.getoutput(dataSourcePingCmd)
                if int(dataSourcePingOutput) != 0:
                        content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),DataSourcesIpAdd,"Not Reachable")
                        csvWriter = CsvWriter("DataSourceReachability",'DataSource_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                        nonReachableMachines.append(DataSourcesIpAdd)
                else:
                        print 'Data Source {0} is reachable'.format(DataSourcesIpAdd)
                print 'Non Reachable Machines are',nonReachableMachines
        frameEmailContent(nonReachableMachines)



def checkReachabilityOfCemodSource(CemodSourcesIpAdds):
        nonReachableMachines = []
        for CemodSourcesIpAdd in CemodSourcesIpAdds:
                cemodPingCmd = 'ping \\-c 1 %s | grep "packet loss" | gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' '%(CemodSourcesIpAdd,"%")
                cemodSourcePingOutput = commands.getoutput(cemodPingCmd)
                if int(cemodSourcePingOutput) != 0:
                        content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H-%M-%S"),CemodSourcesIpAdd,"Not Reachable")
                        csvWriter = CsvWriter("DataSourceReachability",'CemodSource_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                        nonReachableMachines.append(CemodSourcesIpAdd)
                else:
                        print 'Cemod Source {0} is reachable'.format(CemodSourcesIpAdd)
                print 'Non Reachable Machines are',nonReachableMachines
        frameEmailContent(nonReachableMachines)


def getDataSourcesIp():
        DataSourcesIpAdd = []
        xmlparser = minidom.parse('/opt/nsn/rtb/monitoring/conf/monitoring.xml')
        parentHostNamesElements = xmlparser.getElementsByTagName('DataSources')
        childHostsElements = parentHostNamesElements[0].getElementsByTagName('Host')
        for childHostsElement in childHostsElements:
                        DataSourcesIpAdd.append(childHostsElement.attributes['IPADDR'].value)
        return DataSourcesIpAdd

def getCemodSourcesIp():
        CemodSourcesIpAdd = []
        xmlparser = minidom.parse('/opt/nsn/rtb/monitoring/conf/monitoring.xml')
        parentHostNamesElements = xmlparser.getElementsByTagName('CemodSources')
        childHostsElements = parentHostNamesElements[0].getElementsByTagName('Host')
        for childHostsElement in childHostsElements:
                        CemodSourcesIpAdd.append(childHostsElement.attributes['IPADDR'].value)
        return CemodSourcesIpAdd


def main():
        checkReachabilityOfDataSource(getDataSourcesIp())
        checkReachabilityOfCemodSource(getCemodSourcesIp())

if __name__ == "__main__":
        main()

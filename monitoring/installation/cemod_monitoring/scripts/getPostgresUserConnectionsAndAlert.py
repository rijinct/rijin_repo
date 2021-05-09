#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This checks the different postgres user connections and on breach
# of configured threshold value, raises an alert through email
# Date : 27-03-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
import commands,sys,datetime
from xml.dom import minidom
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils
from snmpUtils import SnmpUtils
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

def getUsersThresholdValue():
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('PostgresUserInformation')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                usersThresholdValue = propertyelements.attributes['userconnectionthreshold'].value
        return usersThresholdValue

def frameEmailAndSend(thresholdBreachedDict):
        tableHeader = '<th bgcolor="#C7D7F1">' + "User" + '</th>' + '<th bgcolor="#C7D7F1">'+ "Connections Count" + '</th>'
        html = '''<html><head><style>
             table, th, td {
             border: 1px solid black;
             border-collapse: collapse;
             }
             </style></head><body>'''
        labelMessage = "Below is the Postgres User's Connections Breached Information"
        for key,value in thresholdBreachedDict.items():
                html = html + '<tr><td>' + key + '</td>'
                html = html + '<td>' + value + '</td>\n</tr>'
        html +=  '''<br></body></html>'''
        htmlMessage = '<h3>%s</h3><table><tr>%s</tr>%s</table><br />' %(labelMessage,tableHeader,html)
        EmailUtils().frameEmailAndSend("[CRITICAL ALERT]:Postgres User's Connection Alert on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),htmlMessage)

def checkThresholdAndSendAlert(userConnections):
        thresholdBreachedDict = {}
        configuredThresholdValue = getUsersThresholdValue()
        print 'Configured Threshold value in XML is',configuredThresholdValue
        for user in userConnections:
                if int(user.split(',')[1]) >= int(configuredThresholdValue):
                        thresholdBreachedDict[user.split(',')[0].strip()] = user.split(',')[1].strip()
        return thresholdBreachedDict

def frameSnmpContent(thresholdBreachedDict):
        snmpIp,snmpPort,snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status,sendSnmpTrap = commands.getstatusoutput('/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::postgresUserConnection SAI-MIB::postgresUser s "{3}"'.format(snmpCommunity,snmpIp,snmpPort,thresholdBreachedDict))
        if str(status) == '0':
                print 'SNMP Traps sent successfully.'
        else:
                print 'Error in sending SNMP Trap.'

def main():
        userConnectionCmd = 'ssh {0} "su - postgres -c \'/opt/nsn/ngdb/pgsql/bin/psql sai -c \\"select usename,count(*) from pg_stat_activity group by usename\\"\'" | egrep -v "rows|row|usename|--"'.format(cemod_postgres_sdk_fip_active_host)
        userConnections = commands.getoutput(userConnectionCmd).strip().replace("|",",").split('\n')
        print userConnections
        for item in userConnections:
                content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H:%M:%S"),item.split(',')[0].strip(),item.split(',')[1].strip())
                csvWriter = CsvWriter('postgresUsersCount','postgresUserCount_%s.csv'%(datetime.datetime.now().strftime("%Y-%m-%d")),content)
        thresholdBreachedDict = checkThresholdAndSendAlert(userConnections)
        if thresholdBreachedDict:
                frameEmailAndSend(thresholdBreachedDict)
                frameSnmpContent(thresholdBreachedDict)
                print 'Postgres User Connection are',thresholdBreachedDict
                exit(2)
        else:
                print 'Postgres User Connection are not breached'
                exit(0)

if __name__ == "__main__":
        main()

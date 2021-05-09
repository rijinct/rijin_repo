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
sys.path.insert(0,'/opt/portal/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils


def getHosts(service):
        xmlparser = minidom.parse('/home/portal/ifw/config/ifw_portal_config.xml')
        portalClusterNodes = xmlparser.getElementsByTagName('Roles')
        propertyTag=portalClusterNodes[0].getElementsByTagName('Role')
        for propertyelements in propertyTag:
                if propertyelements.attributes['Name'].value == service:
                        return propertyelements.attributes['Hosts'].value.split()

def getUsersThresholdValue():
        xmlparser = minidom.parse('/opt/portal/monitoring/conf/monitoring.xml')
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
        EmailUtils().frameEmailAndSend("Postgres User's Connection Alert:",htmlMessage)

def checkThresholdAndSendAlert(userConnections):
        thresholdBreachedDict = {}
        configuredThresholdValue = getUsersThresholdValue()
        print 'Configured Threshold value in XML is',configuredThresholdValue
        for user in userConnections:
                if int(user.split(',')[1]) >= int(configuredThresholdValue):
                        thresholdBreachedDict[user.split(',')[0].strip()] = user.split(',')[1].strip()
        return thresholdBreachedDict

def checkPortalDb():
        xmlparser = minidom.parse('/home/portal/ifw/config/ifw_portal_config.xml')
        deploymentTag = xmlparser.getElementsByTagName('Deployment')
        deploymentType = deploymentTag[0].attributes['HA'].value
        if deploymentType == "no":
                dbNode = getHosts("DB")
        else:
                dbNode = []
                dbNode.append(deploymentTag[0].attributes['FLOATING_IP_DB'].value)
        return dbNode[0]

def main():        
        userConnectionCmd = 'ssh {0} "PGPASSWORD=password psql -U cempostgres cemportal -c \\"select usename,count(*) from pg_stat_activity group by usename\\"" | egrep -v "rows|row|usename|--"'.format(checkPortalDb())
        userConnections = commands.getoutput(userConnectionCmd).strip().replace("|",",").split('\n')
        print userConnections
        for item in userConnections:
                content = '%s,%s,%s'%(datetime.datetime.now().strftime("%H:%M:%S"),item.split(',')[0].strip(),item.split(',')[1].strip())
                csvWriter = CsvWriter('postgresUsersCount','postgresUserCount_%s.csv'%(datetime.datetime.now().strftime("%Y-%m-%d")),content)
        thresholdBreachedDict = checkThresholdAndSendAlert(userConnections)
        if thresholdBreachedDict:
                frameEmailAndSend(thresholdBreachedDict)

if __name__ == "__main__":
        main()

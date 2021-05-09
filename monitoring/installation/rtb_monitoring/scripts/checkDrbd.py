#!/usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose:
#
# Date:   05-12-2017
#############################################################################
#############################################################################
# Code Modification History
# 1.
#
# 2.
#############################################################################
from xml.dom import minidom
import datetime, commands, sys
from collections import OrderedDict
sys.path.insert(0,'/opt/nsn/rtb/monitoring/utils')
from writeCsvUtils import CsvWriter
from sendMailUtil import EmailUtils

def getDrbdHosts():
        drbdHosts = []
        xmlparser = minidom.parse('/opt/nsn/ifw/config/ha_rtb_config.xml')
        clusterHostNamesTag = xmlparser.getElementsByTagName('ClusterHostNames')
        clusterMemberTags = clusterHostNamesTag[0].getElementsByTagName('ClusterMember')
        for clusterMemberTag in clusterMemberTags:
                drbdHosts.append(clusterMemberTag.attributes['IPADDR'].value)
        return drbdHosts

def checkDeploymentAndGetIps():
        xmlparser = minidom.parse('/opt/nsn/ifw/config/rtb_config.xml')
        configTag = xmlparser.getElementsByTagName('config')
        configurationTagElements = configTag[0].getElementsByTagName('Configuration')
        deploymentType = configurationTagElements[0].attributes['isHa'].value
        if deploymentType == "yes":
                return getDrbdHosts()
        else:
                print 'DRBD is not applicable'

def checkDrbdService(drbdHost):
        drbdStatusCmd = 'ssh %s "service drbd status | grep -i \\"drbd driver loaded OK\\" "'%(drbdHost)
        status,drbdOutput = commands.getstatusoutput(drbdStatusCmd)
        if int(status) != 0:
                content = '%s,%s,%s,%s,%s'%(datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S"),drbdHost,"DRBD Service","Service Down","ServiceDown-No Uptime")
                csvWriter = CsvWriter("ServiceStability",'serviceStability_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                composeHTML("DRBD Service is down on {1}".format(drbdHost), [])
        else:
		return drbdStatus(drbdHost)

def drbdStatus(drbdHost):
	drbdUpdateCmd = 'ssh %s "service drbd status |grep Connected| gawk -F\\" \\" \'{print \$3 \\" \\" \$4 \\" \\" \$6}\'" '%(drbdHost)
	status,drbdUpdate = commands.getstatusoutput(drbdUpdateCmd)
	drbdDict = {}
	drbdStatus = []
	print 'Checking DRBD status on',drbdHost
	for row in drbdUpdate.split('\n'):
		mounted = row.split()[2]
		ro = row.split()[0].split('/')
		ds = row.split()[1].split('/')
		combined_ro_ds = OrderedDict(zip(ro,ds))
		drbdDict[mounted] = combined_ro_ds
	for key,valueDict in drbdDict.items():
		for k in valueDict.keys():
			if valueDict[k] != 'UpToDate':
				drbdStatus.append('{0} - {1} - {2}'.format(key, k, valueDict[k]))
	if len(drbdStatus) > 0:
		return composeHtml('DRBD Service Status for {0}'.format(drbdHost), drbdStatus)
	else: return ''

def checkMountPoint(drbdHost):
	s,checkMount = commands.getstatusoutput('ssh %s "service drbd status |grep -A10 mounted|gawk -F \\" \\" \'{print \$6}\'| tr \-d [:space:]"' %(drbdHost))
	return checkMount == 'mounted'
	
	
def composeHtml(label, logs):
        html = '''<html><head><style>
              table, th, td {
              border: 1px solid black;
              border-collapse: collapse;
            }
            </style></head><body>'''

        for row in logs:
		html = html + row + '\n<br />'
        html +=  '''<br></body></html>'''
	return '<h3>%s</h3>%s<br />' %(label,html)
	
def main():
	htmlFormat = ''
        drbdHosts = checkDeploymentAndGetIps()
        for drbdHost in drbdHosts:
		if checkMountPoint(drbdHost):
			continue
                htmlFormat += checkDrbdService(drbdHost)
	if htmlFormat != '':
		print 'Mail Sent'
		EmailUtils().frameEmailAndSend('Alert: Service Down', htmlFormat)
	else: print 'DRBD Status is UpToDate on all nodes'
	

if __name__ == "__main__":
        main()

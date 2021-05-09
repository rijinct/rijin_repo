#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose: Checks the availability of the UI
#
# Date:    08-02-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################


import commands, os, csv, re, datetime, time, logging
from datetime import date, timedelta
from xml.dom import minidom


def getEpochTime():
        yesterday = date.today() - timedelta(1)
        start = yesterday.strftime('%d.%m.%Y %H:%M:%S')
        pattern = '%d.%m.%Y %H:%M:%S'
        startpartition = int(time.mktime(time.strptime(start, pattern)))*1000
        return startpartition

def createOutputCSV():
        global filepath
        filename = '/var/local/monitoring/output/checkURL/checkURL_'
        date = datetime.date.today()
        filepath = filename + str(date) + '.csv'
        status1,output1 = commands.getstatusoutput('ls ' + filepath)
        if status1 != 0:
                a = 'touch ' + filepath
                createFile = commands.getoutput('touch ' +  filepath)

def appendHeader():
        header = 'Date,Component,Status'
        header = header + '\n'
        new_file = open(filepath,'a')
        new_file.write(header)
        new_file.close()

def checkURL(urlname,url):
        if url == 'IP not configured':
                print 'IP is not configured'
                log = urlname + ',' + url
                logIntoFile(log)
        else:
                status,output = commands.getstatusoutput(url)
                if '200 OK' in output or 'HTTP/1.1 200' in output:
                        print 'Service is UP for',urlname
                        log =  urlname+',UP'
                        logIntoFile(log)
                elif '401' in output:
                        print 'Invalid Username/Password'
                else:
                        print urlname,'is DOWN',output
                        log =  urlname+',DOWN'
                        logIntoFile(log)

def getIP(hostName):
        if cemod_os_release_base_version == "RHEL6":
                return commands.getoutput("ssh %s ifconfig | grep \"inet\" | head -1 | gawk -F\" \" '{print $2}' | gawk -F\":\" '{print $2}'" %hostName)
        else:
                return commands.getoutput('perl -e "require \\"/opt/nsn/ngdb/ifw/utils/Logging.pl\\"; require \\"/opt/nsn/ngdb/ifw/utils/Utilities.pl\\"; print join(\' \',getPublicIPOfHostname(\\"%s\\"))"' %(hostName))

'''Chech Admin UI Availability'''
def checkAdminUI():
        xmldoc = minidom.parse('/opt/nsn/ngdb/ifw/etc/application/application_config.xml')
        elements = xmldoc.getElementsByTagName('Role')
        get_admin_node = [val.attributes['hosts'].value for val in elements if val.attributes['name'].value == 'monitoring']
        get_admin_hostname_cmd = 'cat /etc/hosts | grep %s | awk -F " " \'{print $2}\'' %get_admin_node[0]
        status,get_admin_hostname = commands.getstatusoutput(get_admin_hostname_cmd)
        admin_url = 'curl -k -Is https://%s/admin/|head -1' %getIP(get_admin_hostname)
        print admin_url
        checkURL('ADMIN UI',admin_url)

'''Chech BOXI CMC Availability'''
def checkBoxiCmcUI():
        global get_boxi_nodes
        get_boxi_nodes = cemod_boxi_hosts
        print get_boxi_nodes
        get_boxi_nodes = get_boxi_nodes.split()
        for host in get_boxi_nodes:
                url = 'curl -k -Is https://%s:8444/BOE/CEMoD/|head -1' %getIP(host)
                print url
                checkURL('BO CMC '+host,url)

'''Chech BOXI BI Launchpad Availability'''
def checkBoxiBiUI():
        for host in get_boxi_nodes:
                url = 'curl -k -Is https://%s:8444/BOE/CEMoDFlexiReport/|head -1' %getIP(host)
                checkURL('BO BI Launchpad '+host,url)

'''Chech Portal UI Availability'''
def checkPortalUI():
        portalIP_cmd = 'sed -n "/<portal/,/<\/portal/p" /opt/nsn/ngdb/monitoring/conf/monitoring.xml |grep -i Name|awk -F " " \'{print $3}\'|awk -F "=" \'{print $2}\'|awk -F "\\"" \'{print $2}\''
        status,portalIP = commands.getstatusoutput(portalIP_cmd)
        for portal in portalIP.split("\n"):
                if len(portal) > 0:
                        url = 'curl -k -Is https://%s:8443|head -1' %portal
                        checkURL('PORTAL '+ portal,url)
                else: checkURL('PORTAL '+ portal,'IP not configured')

def logIntoFile(logs):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s,%(message)s',
                    datefmt='%H:%M:%S',
                    filename=filepath,
                    filemode='a')
        logging.error(logs)
        l = logging.getLogger()
        for hdlr in l.handlers[:]:  # remove all old handlers
                l.removeHandler(hdlr)

def checkICERest():
        sqlQuery="select * from VOICE_RAW where imsi='262030014010001' and dt>='{0}'".format(getEpochTime())
        if cemod_application_content_pack_ICE_status == "yes":
                restReachabilityOutput = commands.getoutput('curl -k -d "sqlQuery={0}" -X POST "https://{1}:8443/icecache/v1/service/query"'.format(sqlQuery,cemod_webservice_active_fip_host))
                if 'HTTP Status 404' in restReachabilityOutput or 'HTTP Status 401' in restReachabilityOutput or 'HTTP Status 403' in restReachabilityOutput:
                        fileContent = 'ICE REST APP'+',DOWN'
                else:
                        fileContent = 'ICE REST APP'+',UP'
                logIntoFile(fileContent)
        else:
                print 'ICE is not configured'



exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
createOutputCSV()
appendHeader()
checkAdminUI()
checkBoxiCmcUI()
checkBoxiBiUI()
checkPortalUI()
checkICERest()

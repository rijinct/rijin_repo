#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose:
#
#
# Date :
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
from xml.dom import minidom

import commands, sys, os, re, datetime
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils
from writeCsvUtils import CsvWriter



exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))

def getOpenFileThresholdValue():
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('OpenFilesLimit')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                openFilesThresholdValue = propertyelements.attributes['openfilesthreshold'].value
        return int(openFilesThresholdValue)

def getListOfDirectories():
        correctDirList = []
        finalDirList = []
        dirSet = set()
        directories = os.walk("/proc/")
        for directory in directories:
                correctDirList.append(re.findall("/proc/[0-9]*/fd/*",directory[0]))
        for dir in correctDirList:
                if len(dir) >=1:
                        finalDirList.append(dir)
                for finalDir in finalDirList:
                        dirSet.add(finalDir[0])
        return dirSet

def getLsofPerProcess():
        breachedList = ['List of process(es),value']
        csvWriter = CsvWriter('openFiles','openFiles_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('openFilesDetails','openFiles_Details_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        csvWriter = CsvWriter('openFilesDebug','openFilesList_Debugging_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),header="Yes")
        directories = getListOfDirectories()
        for directory in directories:
                status,count = commands.getstatusoutput('''ls {0}/ |wc -l '''.format(directory))
                content = '{0},{1},{2}'.format(datetime.datetime.today().strftime("%Y-%m-%d-%H"),directory.split("/")[2],count)
                csvWriter = CsvWriter('openFiles','openFiles_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),content)
                if str(status) == '0' and 'No such' not in count:
                        if int(count) > getOpenFileThresholdValue():
                                status,processOutput = commands.getstatusoutput('''ps {0} | grep -v TTY | gawk -F" " '{{print $1","$5}}' '''.format(directory.split("/")[2]))
                                contentDetail = '{0},{1},{2}'.format(datetime.datetime.today().strftime("%Y-%m-%d-%H-%M"),processOutput,count)
                                csvWriter = CsvWriter('openFilesDetails','openFiles_Details_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),contentDetail)
                                status,processLsofOuput = commands.getstatusoutput('''lsof -p {0} | grep -v TTY | gawk -F" " '{{print $2","$9}}' '''.format(directory.split("/")[2]))
                                contentLsofDetail = '{0},{1}'.format(datetime.datetime.today().strftime("%Y-%m-%d-%H-%M"),processLsofOuput)
                                csvWriter = CsvWriter('openFilesDebug','openFilesList_Debugging_%s.csv'%(datetime.date.today().strftime("%Y%m%d")),contentLsofDetail)
                                breachedList.append('{0},{1}'.format(directory.split("/")[2],contentDetail))
                        else:
                                print 'No Open files breached threshold for the process {0}'.format(directory.split("/")[2])
                else:
                        print 'Error while getting the count for the directory: ',directory
        if len(breachedList) > 1:
                html = HtmlUtil().generateHtmlFromList('Open Files Threshold Breached for the processes {0}'.format(cemod_hdfs_user), breachedList)
                EmailUtils().frameEmailAndSend('Open Files Process(es) List',html)


def getLsofFilesForUser():
        status,output = commands.getstatusoutput('lsof -u {0} |wc -l'.format(cemod_hdfs_user))
        openFilesThresholdValue = getOpenFileThresholdValue()
        print 'Current open files count for user {0}'.format(cemod_hdfs_user)
        print 'Configured Value in the xml',getOpenFileThresholdValue()
        if int(output) > openFilesThresholdValue:
                breachedList = ['List of User(s),value']
                breachedList.append('{0},{1}'.format(cemod_hdfs_user,output))
                html = HtmlUtil().generateHtmlFromList('Open Files Threshold Breached for the user {0}'.format(cemod_hdfs_user), breachedList)
                if len(breachedList) > 1:
                        EmailUtils().frameEmailAndSend('Open Files Process(es) List',html)
        getLsofPerProcess()

def main():
        getLsofFilesForUser()

if __name__ == '__main__':
        main()
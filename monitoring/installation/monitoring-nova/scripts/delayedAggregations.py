#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script reads the configuration xml & checks whether
# aggregations for the current day has completed. If not completed, it raises
# an Alert.
# Date : 06-02-2019
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
from xml.dom import minidom
from datetime import timedelta,datetime
import subprocess, datetime, time, sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils
from propertyFileUtil import PropertyFileUtil
from dbConnectionManager import DbConnection

def getBoundary(jobNames):
        actualDelayedAggDict = dict()
        for jobName in jobNames:
                for job in jobName.split(","):
                        print('Getting the boundary for the job',job)
                        boundaryQuery = PropertyFileUtil('boundaryDelayedAggQuery','PostgresSqlSection').getValueForKey()
                        finalBoundaryQuery = boundaryQuery.replace("JOB_NAME",job)
                        currentBoundaryValue = DbConnection().getConnectionAndExecuteSql(finalBoundaryQuery, 'postgres')
                        actualDelayedAggDict[job] = currentBoundaryValue
        return actualDelayedAggDict

def checkDelay(delayAggDict):
        dayString = 'DAY'
        weekString = 'WEEK'
        curDate = datetime.date.today()
        delayedAggList = []
        completedAggList = []
        completedAggList.append("Job Name(s)")
        delayedAggList.append("Job Name(s)")
        print('Delay Agg Dict has',delayAggDict)
        expectedBoundaryValue = (datetime.datetime.now() - timedelta(1)).strftime("%Y-%m-%d")
        for key in list(delayAggDict.keys()):
                if dayString in key or dayString.lower() in key:
                        print('Its Day Job')
                        expectedBoundaryValue = (datetime.datetime.now() - timedelta(1)).strftime("%Y-%m-%d")
                else:
                        print('Its week Job')
                        expectedBoundaryValue = str(curDate - datetime.timedelta(days = curDate.weekday()+7))
                currentBoundaryValue = str(delayAggDict[key]).split(" ")[0]
                print('Current Boundary Value is',currentBoundaryValue)
                print('Expected Boundary value is',expectedBoundaryValue)
                if currentBoundaryValue == expectedBoundaryValue:
                        print('Aggregation is completed, sending an INFO alert')
                        completedAggList.append(key)
                else:
                        print('Aggregation is Delayed, raising the alert')
                        delayedAggList.append(key)
        sendInfoAlert(completedAggList)
        raiseAlert(delayedAggList)

def sendInfoAlert(completedAggList):
        if len(completedAggList) > 1:
                html = HtmlUtil().generateHtmlFromList('Completed Aggregation Jobs', completedAggList)
                EmailUtils().frameEmailAndSend("[INFO]: Completed Aggregation Jobs at {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)

def raiseAlert(delayedAggList):
        if len(delayedAggList) > 1:
                html = HtmlUtil().generateHtmlFromList('Below are the Delayed Aggregation Jobs', delayedAggList)
                EmailUtils().frameEmailAndSend("[MAJOR_ALERT]: Delayed Aggregation Jobs at {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)

def main():
        if len(sys.argv) > 1:
                jobNames = [sys.argv[1]]
                actualDelayedAggDict = getBoundary(jobNames)
                checkDelay(actualDelayedAggDict)
        else:
                print('Script Usage is in-correct\n')
                print('Usage: python {0} <JOB_NAME(S)>'.format(sys.argv[0]))
                exit(1)

if __name__ == '__main__':
        main()
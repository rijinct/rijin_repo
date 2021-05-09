############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script sends the email of Expected & Actual count content at
# table level & sends the failed Qs Information as well
# Date:  19-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1.
# 2.
#############################################################################
import subprocess, datetime, sys, time,traceback
from collections import OrderedDict
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from dbConnectionManager import DbConnection
from generalPythonUtils import PythonUtils
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from dateTimeUtil import DateTimeUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

def writeToCsv(actualQsDict, expectedQsDict,frequency,date):
        rightnow = time.time()
        curDateTime = datetime.datetime.utcfromtimestamp(rightnow).strftime("%Y-%m-%d %H:%M:%S")
        #curDateTimePod = datetime.datetime.fromtimestamp(rightnow).strftime("%Y-%m-%d %H:%M:%S")
        content = ""
        if actualQsDict or expectedQsDict:
                for key in list(expectedQsDict.keys()):
                        if key in actualQsDict:
                                try:
                                        content = '{0},{1},{2},{3},{4}'.format(date,curDateTime,key, expectedQsDict[key], actualQsDict[key])
                                except:
                                        content = '{0},{1},{2},{3},{4}'.format(date,curDateTime,key, expectedQsDict[key], "")
                        elif key == "aggregation_table_name" or key == "TableName":
                                continue
                        elif key not in actualQsDict:
                                content = '{0},{1},{2},{3},{4}'.format(date,curDateTime,key, expectedQsDict[key], "")
                        else:
                                content = '{0},{1},{2},{3},{4}'.format(date,curDateTime,key, expectedQsDict[key], actualQsDict[key])
                        csvWriter = CsvWriter('expectedActualQsTable', 'expectedActualQsInfo_{0}_{1}.csv'.format(frequency,datetime.date.today().strftime("%Y%m%d")), content)



def getFormattedDict(qsList):
        if qsList:
            qsFormattedDict = OrderedDict()
            for row in qsList.split("\n"):
                tableName = row.split(',')[0].strip().lower()
                qsCount = row.split(',')[1].strip()
                qsFormattedDict[tableName] = qsCount
            return qsFormattedDict
        else:
            return {}


def writeFailedQsInformation():
        failedQsJobs = {}
        todaysDate = datetime.datetime.now().strftime("%Y-%m-%d")
        failedQsSql = PropertyFileUtil('failedQsquery','PostgresSqlSection').getValueForKey()
        finalfailedQsSql = failedQsSql.replace("todaysDate",str(todaysDate))
        qsStatus = CsvUtil().executeAndWriteToCsv('failedQsTable',finalfailedQsSql,"mariadb","queryscheduler")


def pushDataToMariaDB(frequency):
        outputFilePath = PropertyFileUtil('expectedActualQsTable','DirectorySection').getValueForKey()
        fileName = outputFilePath + "expectedActualQsInfo_{0}_{1}.csv".format(frequency,datetime.datetime.today().strftime("%Y%m%d"))
        jsonFileName=JsonUtils().convertCsvToJson(fileName)
        DBUtil().pushToMariaDB(jsonFileName,"qscount{0}".format(frequency))


def writeTableLevelQsInformation(inputArg):
        if inputArg != "DIMENSION":
                date,startpartition,endpartition=DateTimeUtil().requiredTimeInfo(inputArg.lower())
                date = (datetime.datetime.strptime(date, '%Y-%m-%d %H-%M-%S')).strftime("%Y-%m-%d %H:%M:%S")
        else:
                date = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d %H:%M:%S")

        if inputArg == "DAY":
                actualQsSql = PropertyFileUtil('actualQsDayquery','PostgresSqlSection').getValueForKey()
                strTimeepoch,endTimeepoch = DateTimeUtil().get_start_and_end_of_last_week()
                strTimeMonthepoch,endTimeMonthepoch = DateTimeUtil().get_start_and_end_of_current_month()
                finalactualQsSql = actualQsSql.replace("startTime",str(startpartition)).replace("input",str(inputArg)).replace("strTime",str(strTimeepoch)).replace("endTime",str(endTimeepoch)).replace("strMonthTime",str(strTimeMonthepoch)).replace("endMonthTime",str(endTimeMonthepoch))
                actualQs = DbConnection().getConnectionAndExecuteSql(finalactualQsSql,'mariadb','webservice')
                expectedQsSql = PropertyFileUtil('expectedQsquery','PostgresSqlSection').getValueForKey()
                finalexpectedQsSql=expectedQsSql.replace("input",str(inputArg))
                expectedQs = DbConnection().getConnectionAndExecuteSql(finalexpectedQsSql,'mariadb','queryscheduler')
        elif inputArg == "WEEK":
                actualQsSql = PropertyFileUtil('actualQsWeekquery','PostgresSqlSection').getValueForKey()
                strTimeMonthepoch,endTimeMonthepoch = DateTimeUtil().get_start_and_end_of_current_month()
                finalactualQsSql = actualQsSql.replace("startTime",str(startpartition)).replace("input",str(inputArg)).replace("strMonthTime",str(strTimeMonthepoch)).replace("endMonthTime",str(endTimeMonthepoch))
                actualQs = DbConnection().getConnectionAndExecuteSql(finalactualQsSql,'mariadb','webservice')
                expectedQsSql = PropertyFileUtil('expectedQsquery','PostgresSqlSection').getValueForKey()
                finalexpectedQsSql=expectedQsSql.replace("input",str(inputArg))
                expectedQs = DbConnection().getConnectionAndExecuteSql(finalexpectedQsSql,'mariadb','queryscheduler')
        elif inputArg == "MONTH":
                actualQsSql = PropertyFileUtil('actualQsquery','PostgresSqlSection').getValueForKey()
                finalactualQsSql = actualQsSql.replace("startTime",str(startpartition)).replace("input",str(inputArg))
                actualQs = DbConnection().getConnectionAndExecuteSql(finalactualQsSql,'mariadb','webservice')
                expectedQsSql = PropertyFileUtil('expectedQsquery','PostgresSqlSection').getValueForKey()
                finalexpectedQsSql=expectedQsSql.replace("input",str(inputArg))
                expectedQs = DbConnection().getConnectionAndExecuteSql(finalexpectedQsSql,'mariadb','queryscheduler')
        elif inputArg == "DIMENSION":
                actualQsSql = PropertyFileUtil('actualQsDimensionquery','PostgresSqlSection').getValueForKey()
                actualQs = DbConnection().getConnectionAndExecuteSql(actualQsSql,'mariadb','webservice')
                finalexpectedQsSql = PropertyFileUtil('expectedQsDimensionquery','PostgresSqlSection').getValueForKey()
                expectedQs = DbConnection().getConnectionAndExecuteSql(finalexpectedQsSql,'mariadb','queryscheduler')
        else:
                logger.info('Not a valid Input Argument')
        writeToCsv(getFormattedDict(actualQs), getFormattedDict(expectedQs),inputArg,date)


def main():
    try:
            inputArg = sys.argv[1]
            writeFailedQsInformation()
            writeTableLevelQsInformation(inputArg)
            pushDataToMariaDB(inputArg)
    except IndexError:
        logger.exception(traceback.print_exc())
        logger.exception('Usage of the script is: python qsCacheCountTableLevel.py [DAY/WEEK/MONTH/DIMENSION]')


if __name__ == "__main__":
        main()

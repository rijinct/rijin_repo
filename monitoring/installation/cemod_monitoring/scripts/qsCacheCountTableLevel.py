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
import commands, datetime, sys, time
from collections import OrderedDict
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from writeCsvUtils import CsvWriter
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from propertyFileUtil import PropertyFileUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))

def writeToCsv(actualQsDict, expectedQsDict,frequency):
        content = ""
        if actualQsDict and expectedQsDict:
                for key in expectedQsDict.keys():
                        if key in actualQsDict:
                                try:
                                        content = '{0},{1},{2}'.format(key, expectedQsDict[key], actualQsDict[key])
                                except:
                                        content = '{0},{1},{2}'.format(key, expectedQsDict[key], "")
                        elif key == "aggregation_table_name" or key == "TableName":
                                continue
                        elif key not in actualQsDict:
                                content = '{0},{1},{2}'.format(key, expectedQsDict[key], "")
                                print 'Content is ', content
                        else:
                                content = '{0},{1},{2}'.format(key, expectedQsDict[key], actualQsDict[key])
                        print 'Content is', content
                        csvWriter = CsvWriter('expectedActualQsTable', 'expectedActualQsInfo_{0}_{1}.csv'.format(frequency,datetime.date.today().strftime("%Y%m%d")), content)
                        
def getDt():
        today = datetime.date.today()
        minTime = datetime.time().min
        maxTime = datetime.time().max
        minTime = datetime.datetime.combine(today, minTime)
        maxTime = datetime.datetime.combine(today, maxTime)
        minTime_epoch = int(time.mktime(time.strptime(str(minTime), '%Y-%m-%d %H:%M:%S'))) * 1000
        maxTime_epoch = int(time.mktime(time.strptime(str(maxTime), '%Y-%m-%d %H:%M:%S.%f'))) * 1000
        return minTime_epoch, maxTime_epoch


def getFormattedDict(qsList):
        qsFormattedDict = OrderedDict()
        for row in qsList:
                tableName = row.split(',')[0].strip().lower()
                qsCount = row.split(',')[1].strip()
                qsFormattedDict[tableName] = qsCount
        return qsFormattedDict


def writeFailedQsInformation():
        failedQsJobs = {}
        todaysDate = datetime.datetime.now().strftime("%Y-%m-%d")
        failedQueryCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"select rtrim(jobname) as jobname,count(*) from qs_report_tab where qsjobstarttime > \'{2} 00.00.00\' and status=\'FAILED\' group by jobname;\\"" | egrep -v "rows|row|--|rtrim"'.format(cemod_postgres_sdk_fip_active_host, "saiws", todaysDate)
        failedQuerys = commands.getoutput(failedQueryCmd).strip().replace("|", ",").split('\n')
        if len(failedQuerys) > 1:
                for failedQuery in failedQuerys:
                        if failedQuery.split(',')[0].strip() == 'jobname':
                                continue
                        content = '{0},{1}'.format(failedQuery.split(',')[0].strip(), failedQuery.split(',')[1].strip())
                        csvWriter = CsvWriter('failedQsTable', 'failedQsInfo_%s.csv' % (datetime.date.today().strftime("%Y%m%d")), content)

def pushDataToPostgresDB(frequency):
        outputFilePath = PropertyFileUtil('expectedActualQsTable','DirectorySection').getValueForKey()
        fileName = outputFilePath + "expectedActualQsInfo_{0}_{1}.csv".format(frequency,datetime.datetime.today().strftime("%Y%m%d"))
        jsonFileName=JsonUtils().convertCsvToJson(fileName)
        DBUtil().pushDataToPostgresDB(jsonFileName,"qscount{0}".format(frequency))
		
def writeTableLevelQsInformation(inputArg):
        startDateTime, endDateTime = getDt()
        if inputArg == "DAY":
                actualQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"select table_name, count(*) as actual from cache_tab where source=\'S\' and last_fetched_time > \'{2}\' and table_name ilike \'%{3}%\' group by table_name order by table_name asc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws", startDateTime, inputArg)
                expectedQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"select aggregation_table_name, count(*) from ws_scheduled_cache_tab where aggregation_table_name ilike \'%{2}%\' group by aggregation_table_name order by aggregation_table_name asc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws", inputArg)
        elif inputArg == "WEEK":
                expectedQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"select aggregation_table_name, count(*) from ws_scheduled_cache_tab where aggregation_table_name ilike \'%{2}%\' group by aggregation_table_name order by aggregation_table_name asc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws",inputArg)
                actualQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"select table_name, count(*) as actual from cache_tab where source=\'S\' and last_fetched_time > \'{2}\' and table_name ilike \'%{3}%\' group by table_name order by table_name asc;\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws", startDateTime,inputArg)
        elif inputArg == "DIMENSION":
                actualQsSql = PropertyFileUtil('actualQsDimensionquery','PostgresSqlSection').getValueForKey()
	        finalactualQsSql = actualQsSql.replace("startTime",str(startDateTime))
                actualQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"{2}\\" "| egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws",finalactualQsSql)
                finalexpectedQsSql = PropertyFileUtil('expectedQsDimensionquery','PostgresSqlSection').getValueForKey()
                expectedQsCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai {1} -c \\"{2}\\"" | egrep -v "rows|row|--"'.format(cemod_postgres_sdk_fip_active_host, "saiws", finalexpectedQsSql)
        else:
                print 'Not a valid Input Argument'
        actualQs = commands.getoutput(actualQsCmd).strip().replace("|", ",").split('\n')
        expectedQs = commands.getoutput(expectedQsCmd).strip().replace("|", ",").split('\n')
        writeToCsv(getFormattedDict(actualQs), getFormattedDict(expectedQs),inputArg)


def main():
    try:
            inputArg = sys.argv[1]
            writeFailedQsInformation()
            writeTableLevelQsInformation(inputArg)
            pushDataToPostgresDB(inputArg)
    except IndexError:
        print 'Usage of the script is: python qsCacheCountTableLevel.py [DAY/WEEK/DIMENSION]'


if __name__ == "__main__":
        main()

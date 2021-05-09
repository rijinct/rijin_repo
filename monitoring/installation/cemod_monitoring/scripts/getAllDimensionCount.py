############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script writes all dimension tables count into a CSV.
#
# Date:  20-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1.
# 2.
#############################################################################
import commands, re, sys, datetime
from datetime import date
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from writeCsvUtils import CsvWriter
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))
from dbUtils import DBUtil
from jsonUtils import JsonUtils

def getAllDimensionTables():
        return commands.getoutput('su - {0} -c "beeline -u \'{1}\' --silent=true --showHeader=false --outputformat=csv2 -e \'show tables like \\"{2}\\"; \' "'.format(cemod_hdfs_user,cemod_hive_url,'es*')).split('\n')

def writeDimensiontCount(dimensionTablesList):
        dimensionTables = [table for table in dimensionTablesList if re.match("^es",table)]
        for dimensionTable in dimensionTables:
                countDimensionCmd = 'su - {0} -c "beeline -u \'{1}\' --silent=true --showHeader=false --outputformat=csv2 -e \'SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring; select count(*) from {2}; \' 2>Logs.txt " | tail -1'.format(cemod_hdfs_user,cemod_hive_url,dimensionTable)
                if cemod_platform_distribution_type == "wandisco":
                        countDimension = commands.getoutput(countDimensionCmd)
                elif cemod_platform_distribution_type == "cloudera":
                        countDimension = commands.getoutput(countDimensionCmd)
                else: print 'Distribution is not wandisco or cloudera'
                print 'Dimension Count for the table ',dimensionTable,'is',countDimension
                content = '%s,%s,%s'%(date.today().strftime('%Y-%m-%d %H:%M:%S'),dimensionTable,countDimension)
                csvWriter = CsvWriter('DimensionCount','dimensionCount_%s.csv'%(datetime.datetime.now().strftime("%Y-%m-%d")),content)

def pushDataToPostgresDB():
        outputFilePath = PropertyFileUtil('dimensionCount','DirectorySection').getValueForKey()
        fileName = outputFilePath + "dimensionCount_{0}.csv".format(datetime.datetime.today().strftime("%Y-%m-%d"))
        jsonFileName=JsonUtils().convertCsvToJson(fileName)
        DBUtil().pushDataToPostgresDB(jsonFileName,"dimensionCount")



def main():
        dimensionTables = getAllDimensionTables()
        writeDimensiontCount(dimensionTables)
        pushDataToPostgresDB()

if __name__ == "__main__":
        main()

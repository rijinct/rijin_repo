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
#############################################################################
import subprocess, os, csv, re, datetime, time, logging, sys
from datetime import date, timedelta
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from dbConnectionManager import DbConnection
from generalPythonUtils import PythonUtils
from propertyFileUtil import PropertyFileUtil
from writeCsvUtils import CsvWriter
from csvUtil import CsvUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

def getAllDimensionTables():
        showtablesSql = PropertyFileUtil('showtablesdimension','hiveSqlSection').getValueForKey()
        showtableList=DbConnection().getHiveConnectionAndExecute(showtablesSql)
        return showtableList


def writeDimensiontCount(dimensionTablesList):
        dimensionTables = [table for table in dimensionTablesList.split("\n") if re.match("^es",table)]
        logger.info("Dimension Tables fetched from hive is '%s' ",dimensionTables)
        for dimensionTable in dimensionTables:
            dimensionCountSql = PropertyFileUtil('dimensionCountQuery','hiveSqlSection').getValueForKey()
            finalDimensionCountSql = dimensionCountSql.replace("tablename",str(dimensionTable))
            dimensionCountOutput=DbConnection().getHiveConnectionAndExecute(finalDimensionCountSql)
            content = '{0},{1},{2}'.format(datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S"),dimensionTable,dimensionCountOutput)
            logger.debug("Query for the table '%s' is '%s' and output is : '%s' ",dimensionTable,finalDimensionCountSql,dimensionCountOutput)
            CsvUtil().writeToCsv("dimensionCount",content)


def pushDataToMariaDB():
        outputFilePath = PropertyFileUtil('dimensionCount','DirectorySection').getValueForKey()
        fileName = outputFilePath + "dimensionCount_{0}.csv".format(datetime.datetime.today().strftime("%Y-%m-%d"))
        jsonFileName=JsonUtils().convertCsvToJson(fileName)
        DBUtil().pushToMariaDB(jsonFileName,"dimensionCount")
        logger.info("Inserted the data to Maria Db")



def main():
        dimensionTables = getAllDimensionTables()
        writeDimensiontCount(dimensionTables)
        logger.info("Csv Writing has been completed")
        pushDataToMariaDB()

if __name__ == "__main__":
        main()


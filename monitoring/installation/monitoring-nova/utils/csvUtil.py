#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class holds the csvUtil methods which can be used to write
# data to csv
# Date: 03-03-2019
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
import csv
import datetime, os

from dbConnectionManager import DbConnection
from propertyFileUtil import PropertyFileUtil
from writeCsvUtils import CsvWriter


class CsvUtil:

    def executeAndWriteToCsv(self,typeOfUtil,finalSql,dbType=None,dbName=None):
        curDate = datetime.date.today()
        dateFormat = curDate.strftime("%Y%m%d")
        if dbType is None and dbName is None:
            queryResult = DbConnection().getConnectionAndExecuteSql(finalSql,"postgres")
        else:
            queryResult = DbConnection().getConnectionAndExecuteSql(finalSql,dbType,dbName)
        if typeOfUtil in ["dayJobStatus","weekJobStatus","monthJobStatus"]:
            dateFormat = str(curDate.year) + '-' + str(curDate.strftime("%m"))
        elif typeOfUtil == "hourJobStatus":
            dateFormat = curDate.strftime("%Y-%m-%d")
        if len(queryResult) >= 1:
            csvWriter = CsvWriter(typeOfUtil,'%s_%s.csv'%(typeOfUtil,dateFormat),header="Yes")
            csvWriter = CsvWriter(typeOfUtil,'%s_%s.csv'%(typeOfUtil,dateFormat),queryResult)
            return queryResult

    def writeToCsv(self,typeOfUtil,output,fileName=None):
        curDate = datetime.date.today()
        if fileName is None:
            fileName = '{0}_{1}.csv'.format(typeOfUtil,curDate)
        filePath = PropertyFileUtil(typeOfUtil,'DirectorySection').getValueForKey()
        CsvWriter(typeOfUtil,fileName).getOutputDirectory(typeOfUtil)
        outputCsvWriter = open(filePath+fileName,'a')
        if os.path.getsize(filePath+fileName) == 0:
            csvWriter = CsvWriter(typeOfUtil,fileName,header="Yes")
        csvWriter = CsvWriter(typeOfUtil,fileName,output)

    def writeDictToCsv(self,typeOfUtil,dictOutput,fileName=None):
        currentUtcTime = " "
        content = datetime.datetime.now().strftime('%H:%M:%S')
        if typeOfUtil in ("backlog", "ETLTopologyStatus"):
            currentUtcTime = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            content=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            curDate=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        curDate = datetime.date.today()
        if not fileName:
            fileName = '{0}_{1}.csv'.format(typeOfUtil,curDate)
        filePath = PropertyFileUtil(typeOfUtil,'DirectorySection').getValueForKey()
        CsvWriter(typeOfUtil,fileName).getOutputDirectory(typeOfUtil)
        outputCsvWriter = open(filePath+fileName,'a')
        if os.path.getsize(filePath+fileName) == 0:
            csvWriter = CsvWriter(typeOfUtil,'%s_%s.csv'%(typeOfUtil,curDate),header="Yes")
        dictKeys = dictOutput.keys()
        for key in dictKeys:
            content = content + "," + str(dictOutput[key])
        content = currentUtcTime+","+content
        csvWriter = CsvWriter(typeOfUtil,'%s_%s.csv'%(typeOfUtil,curDate),content.strip().lstrip(","))

    @staticmethod
    def delete_field_in_csv_string(content, index):
        result = content.split(',')
        result.pop(index)
        return ','.join(result)

    def writeDictToCsvDictWriter(self,typeOfUtil,dictOutput,fileName=None):
        curDate = datetime.date.today()
        if not fileName:
            fileName = '{0}_{1}.csv'.format(typeOfUtil,curDate)
        filePath = PropertyFileUtil(typeOfUtil,'DirectorySection').getValueForKey()
        header = PropertyFileUtil(typeOfUtil,'HeaderSection').getValueForKey().split(',')
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        fileExist=os.path.exists(filePath+fileName)
        with open(filePath+fileName,'a') as outputfile:
            writer=csv.DictWriter(outputfile,fieldnames=header)
            if not fileExist:
                writer.writeheader()
            writer.writerow(dictOutput)

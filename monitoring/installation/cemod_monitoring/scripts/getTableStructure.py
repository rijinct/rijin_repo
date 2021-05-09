#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script writes all tables description to
# a CSV file
# Date:    23-11-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
import subprocess
from commands import *
from datetime import *
import os
import sys

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))

def writeSpecificTables(tablePattern):
        specificTablePath = '/var/local/monitoring/work/specificTables.txt'
        tablesFilePath = '/var/local/monitoring/work/allTablesList.txt'
        writeAllTables()
        specificTables = getoutput('cat %s | grep -i "%s" '%(tablesFilePath,str(tablePattern)))
        with open(specificTablePath,'w') as specificTablePathHandle:
                specificTablePathHandle.write(specificTables)
        getDescAndWriteToCsv(specificTablePath)

def writeAllTables():
        tablesFilePath = '/var/local/monitoring/work/allTablesList.txt'
        tablesFilePathHandle = open(tablesFilePath,'w')
        tablesFilePathHandle.write(getoutput ('su - %s -c "beeline -u \'%s\' -e \'show tables;\' 2>Logs.txt "|egrep -v \' DEBUG|INFO|tab_name \'| grep -v \'+--\' ' %(cemod_hdfs_user,cemod_hive_url)))
        tablesFilePathHandle.close()
        subprocess.call('sed -i \'s/|//g\' %s'%(tablesFilePath), shell=True)

def getDescAndWriteToCsv(filePath):
        outputCsvFile = '/var/local/monitoring/output/tableStructure/TableStructure_%s.csv'%(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
        fileReaderHandler = open(filePath,'r')
        outputCsvWriter = open(outputCsvFile,'a')
        for line in fileReaderHandler:
                print 'Getting Description for',line
                tableDescription = getoutput('su - %s -c "beeline -u \'%s\' -e \'desc %s;\' 2>Logs.txt "|grep -v DEBUG|grep -v INFO | grep -v \'+--\' ' %(cemod_hdfs_user,cemod_hive_url,line))
                print 'Table Description is',tableDescription
                outputCsvWriter.write('\n'+line+'\n')
                outputCsvWriter.write(tableDescription+'\n')
        outputCsvWriter.close()
        fileReaderHandler.close()

def main():
        if len(sys.argv) > 1:
                writeSpecificTables(sys.argv[1])
        else:
                writeAllTables()
                getDescAndWriteToCsv('/var/local/monitoring/work/allTablesList.txt')

if __name__ == "__main__":
        main()
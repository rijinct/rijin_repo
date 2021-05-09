#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class will have methods which cater json parsing utilities
#
# Date:   08-2-2019
#############################################################################
#############################################################################
# Code Modification History
# 1.
#
# 2.
#############################################################################
import json,csv,os,sys

class JsonUtils:

        def parseJsonKeyForData(self,jsonSearchString,jsonOutputString):
                 jsonList = json.loads(jsonOutputString)
                 print("Hits has",len(jsonList.get('hits').get('hits')))
                 if len(jsonList.get('hits').get('hits')) > 0:
                     return "True"
                 else:
                     return "False"

        def convertCsvToJson(self,filePathAndName):
                 with open(filePathAndName) as csvFileReader:
                     dictReader = csv.DictReader(csvFileReader)
                     rows = list(dictReader)
                 jsonFileName = os.path.dirname(filePathAndName) + "/" + os.path.basename(filePathAndName).split(".")[0] + ".json"
                 with open(jsonFileName,'w') as jsonFileWriter:
                     json.dump(rows,jsonFileWriter)
                 return jsonFileName

def main():
    try:
        jsonFileName=JsonUtils().convertCsvToJson(sys.argv[1])
        print(jsonFileName)
    except IndexError as error:
        print('The correct format to use the utility is:    python {} <csvFileNameWithPath>'.format(sys.argv[0]))

if __name__=='__main__':
    main()
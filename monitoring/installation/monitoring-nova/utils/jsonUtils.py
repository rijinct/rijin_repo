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
from typing import Callable
import json,csv,os

class JsonUtils:

        def parseJsonKeyForData(self,jsonSearchString: str,jsonOutputString: str) -> bool:
                 jsonList = json.loads(jsonOutputString)
                 if len(jsonList.get('hits').get('hits')) > 0:
                     return "True"
                 else:
                     return "False"

        def convertCsvToJson(self,filePathAndName: str) -> str:
                 with open(filePathAndName) as csvFileReader:
                     dictReader = csv.DictReader(csvFileReader)
                     rows = list(dictReader)
                 jsonFileName = os.path.dirname(filePathAndName) + "/" + os.path.basename(filePathAndName).split(".")[0] + ".json"
                 with open(jsonFileName,'w') as jsonFileWriter:
                     json.dump(rows,jsonFileWriter)
                 return jsonFileName
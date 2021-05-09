#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  basavaraj.pattanashetti@nokia.com
# Version: 0.1
# Purpose: This class calls the script to insert the records to elasticsearch
# CSV on instance creation
# Date:   05-12-2017
#############################################################################
#############################################################################
# Code Modification History
# 1.
# 2. 
# 3.
#############################################################################
import subprocess, os, datetime
from jsonUtils import JsonUtils

class ElasticSearchUtil:

        def insertToElasticSearch(self,filePath,indexName,header):
                scriptsPath='/opt/nsn/ngdb/monitoring/scripts/'
                print(subprocess.getoutput('python {0}csvToElasticSearchUtil.py {1} {2} {3} {4} {5}'.format(scriptsPath,header,filePath,indexName,os.environ['ELASTICSEARCH_ENDPOINT_NAME'],os.environ['ELASTICSEARCH_ENDPOINT_PORT'])))

        def checkDataInserted(self,searchPattern):             
                wgetCommandOutput = subprocess.getoutput('wget -q -O- http://{0}:{1}/health-monitoring-{2}/_search/'.format(os.environ['ELASTICSEARCH_ENDPOINT_NAME'].split(".")[0],os.environ['ELASTICSEARCH_ENDPOINT_PORT'],datetime.datetime.now().strftime("%Y.%m.%d")))
                jsonStatus = JsonUtils().parseJsonKeyForData(searchPattern,wgetCommandOutput)
                print("Json Status is",jsonStatus)
                return jsonStatus
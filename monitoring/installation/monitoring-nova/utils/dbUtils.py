#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: This class inserts data to mariadb
# Object
# Date: 30-09-2019
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################

import datetime,json,time
from dbConnectionManager import DbConnection
from dateTimeUtil import DateTimeUtil
class DBUtil:

    def pushToMariaDB(self,jsonFile,utilitytype):
        with open(jsonFile,'r') as jsonReader:
            temp_content=jsonReader.readline()
        self.jsonPushToMariaDB(temp_content,utilitytype)


    def jsonPushToMariaDB(self,jsonString,utilitytype):
        now_obj=datetime.datetime.now()
        now_str = now_obj.strftime("%Y-%m-%d %H:%M:%S")
        curDateTime = datetime.datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
        date_in_epoch = DateTimeUtil().getDateInEpoch(now_obj.strftime("%Y-%m-%d"))
        content=json.loads(jsonString)
        for item in content:
            itemString = str(item)
            if utilitytype == 'bucketStats':
                 itemString = str(item).replace('}',', \"utcDate\": \"'+curDateTime+'\",\"DateInLocalTZepoch\": \"'+str(date_in_epoch)+'\"}')
            sqlquery="insert into hm_stats values ('"+str(now_str)+"','"+utilitytype+"','"+itemString.replace('\'','\"')+"')"
            DbConnection().getMonitoringConnectionObject(sqlquery)
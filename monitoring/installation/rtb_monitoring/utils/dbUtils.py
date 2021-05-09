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

import datetime,json,os,sys
import unicodedata
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(","(\"").replace(")","\")"))
class DBUtil:

    def pushDataToPostgresDB(self,jsonFile,utilityType):
        with open(jsonFile,'r') as jsonReader:
            temp_content=jsonReader.readline()
        self.jsonPushToPostgresDB(temp_content,utilityType)

    def jsonPushToPostgresDB(self,jsonString,utilityType):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content=json.loads(jsonString)
        for item in content:
            itemEncode = {}
            for key in item.keys():
                tmpkey = key.encode('ascii','ignore')
                itemEncode[tmpkey] = item[key].encode('ascii','ignore')
            sqlquery="insert into hm_stats values ('"+str(now)+"','"+utilityType+"','"+str(itemEncode).replace('\'','\\"')+"')"
            os.system('psql -h {0} -U {1} -d {2} -c "{3}" '.format(cemod_postgres_sdk_fip_active_host,cemod_sdk_schema_name,cemod_sdk_db_name,sqlquery))


        
def main():
    try:
        DBUtil().pushDataToPostgresDB(sys.argv[1],sys.argv[2])
    except IndexError as error:
        print('The correct format to use the utility is:    python {} <jsonFileNameWithPath> <utilityNameInDB>'.format(sys.argv[0]))
    

if __name__=='__main__':
    main()

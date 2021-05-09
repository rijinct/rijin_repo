#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:Monitoring Team
# Version: 0.1
# Purpose:Script to check missing Bucket Information
#
# Date:    16-02-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import sys, commands, datetime
from xml.dom import minidom
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils

def getTableBucketInfoFromSdk():
        cmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select table_name from (select  replace(replace(replace(replace(paramname,\'PERF\',\'PS\'),\'_CLUSTERED_BY\',\'\'),\'_SORT_BY\',\'\'),\'_NUMBER_OF_BUCKETS\',\'\') as table_name, case when paramname like \'%CLUSTERED_BY\' then paramvalue else NULL end as clustered_by, case when paramname like \'%SORT_BY\' then paramvalue else NULL end as sort_by, case when paramname like \'%NUMBER_OF_BUCKETS\' then paramvalue else NULL end as number_of_buckets from adapt_prop where adaptationid in (select distinct adaptationid from adapt_prop where paramname like \'%CLUSTERED_BY\') and (paramname like \'%CLUSTERED_BY\' or paramname like \'%SORT_BY\' or paramname like \'%NUMBER_OF_BUCKETS\')) t group by table_name order by 1;\\"" | egrep -v "rows|row|--|table_name"'.format(cemod_postgres_sdk_fip_active_host)
        result = commands.getoutput(cmd).upper()
        sdktablename = result.split('\n')
        return sdktablename

def getBucketAutoCorrectProperty():
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('BucketInformation')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                bucketAutoCorrectionValue = propertyelements.attributes['autocorrection'].value
        return bucketAutoCorrectionValue


def statusCheck(tableName,status):
        if status == 0:
                content = 'Altered the table %s successfully \n'%(tableName)
        else:
                content = 'There are errors in altering the table %s \n'%(tableName)
        fileHandler = open('/var/local/monitoring/work/AlterBucketLog_%s.log'%(datetime.datetime.now().strftime("%Y-%m-%d")),'a')
        fileHandler.write(content)
        fileHandler.close()


def getHivePostgres():
        return commands.getoutput('ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo \$cemod_postgres_active_fip_host"'%(cemod_hive_hosts.split(' ')[0]))

def getTableBucketInfoFromMetaStore():
        cmd = 'ssh {0} \'/opt/nsn/ngdb/pgsql/bin/psql metastore hive -c " select \\"TBL_NAME\\" from (select \\"BUCKET_COL_NAME\\", sds.\\"SD_ID\\", \\"NUM_BUCKETS\\", \\"LOCATION\\" from \\"BUCKETING_COLS\\" tbls join \\"SDS\\" sds on tbls.\\"SD_ID\\"=sds.\\"SD_ID\\" ) temp join \\"TBLS\\" tbls on temp.\\"SD_ID\\"=tbls.\\"SD_ID\\"; "\' | egrep -v "rows|row|--|TBL_NAME" '.format(getHivePostgres())
        result = commands.getoutput(cmd).upper()
        hivemetastoretable = result.split('\n')
        return hivemetastoretable

def alterTables(tablesWithNegativeValues):
        print 'Altering Tables has started..'
        for table in tablesWithNegativeValues:
                specificTableCmd = 'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select max(clustered_by) clustered_by,max(sort_by) sort_by,max(number_of_buckets) number_of_buckets from (select  replace(replace(replace(replace(paramname,\'PERF\',\'PS\'),\'_CLUSTERED_BY\',\'\'),\'_SORT_BY\',\'\'),\'_NUMBER_OF_BUCKETS\',\'\') as table_name, case when paramname like \'%CLUSTERED_BY\' then paramvalue else NULL end as clustered_by, case when paramname like \'%SORT_BY\' then paramvalue else NULL end as sort_by, case when paramname like \'%NUMBER_OF_BUCKETS\' then paramvalue else NULL end as number_of_buckets from adapt_prop where adaptationid in (select distinct adaptationid from adapt_prop where paramname like \'%CLUSTERED_BY\') and (paramname like \'%CLUSTERED_BY\' or paramname like \'%SORT_BY\' or paramname like \'%NUMBER_OF_BUCKETS\')) t where table_name=\'{1}\' group by table_name order by 1;\\"" | egrep -v "rows|row|--|table_name|clustered_by"'.format(cemod_postgres_sdk_fip_active_host,table.strip())
                specificTableDetails = commands.getoutput(specificTableCmd).strip().replace("|",",").split('\n')
                if len(specificTableDetails) >= 1:
                        alterCmd = 'su - %s -c "beeline -u \'%s\' --silent=true --showHeader=false -e \'alter table %s clustered by (%s) sorted by (%s) into %s buckets;\'"'%(cemod_hdfs_user,cemod_hive_url,table,specificTableDetails[0].split(',')[0],specificTableDetails[0].split(',')[1],specificTableDetails[0].split(',')[2])
                        print 'Aleter Cmd is ',alterCmd
                        status,alterOutput = commands.getstatusoutput(alterCmd)
                        statusCheck(table.split(',')[0],status)
                        print 'Altering Tables has completed'
        sendMail('Auto-corrected Buckets Info:No action required',tablesWithNegativeValues)


def getTablesWithNegativeValues():
        tablesWithNegativeValues = list(set(getTableBucketInfoFromSdk()) - set(getTableBucketInfoFromMetaStore()))
        if len(tablesWithNegativeValues) > 0:
                sendMail('Missing Bucket Alert', tablesWithNegativeValues)
                if getBucketAutoCorrectProperty().lower() == 'true':
                        alterTables(tablesWithNegativeValues)
        else: print "No missing Bucket for Table "

def sendMail(label,logs,summary=None):
        header = '<th bgcolor="#C7D7F1"> Tables </th>'
        html = '''<html><head><style>
              table, th, td {
              border: 1px solid black;
              border-collapse: collapse;
            }
            </style></head><body>'''

        for row in logs:
                html = html + '<tr>'
                items = row.split(',')
                for item in items:
                        html = html + '<td>' + item + '</td>'
                html = html + '\n</tr>'
        html +=  '''<br></body></html>'''
        if summary:
                htmlFormat = '<h3>%s</h3><p><h4>%s</h4></p><table><tr>%s</tr>%s</table><br />' %(label,summary,header,html)
        else: htmlFormat = '<h3>%s</h3><table><tr>%s</tr>%s</table><br />' %(label,header,html)
        EmailUtils().frameEmailAndSend(label,htmlFormat)

getTablesWithNegativeValues()
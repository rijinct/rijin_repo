#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script checks whether Query is Hung state for Spark Thrift
# Portal/Flexi and App and restarts the respective instances, logs in a csv
# and alerts the user.
# Date : 29-03-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
import commands, multiprocessing, time, datetime, sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
from xml.dom import minidom
from htmlUtil import HtmlUtil
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")

def executeSparkQuery(sparkHost,sparkUrl):
        print 'Running the query on the host {0}'.format(sparkHost)
        sampleQueryOutput = commands.getoutput('ssh {0} "su - {1} -c \\"beeline -u \'{2}\' --silent=true --showHeader=false -e \'select count(*) from tab;\' 2>Logs.txt \\" " | egrep -v "+-"'.format(sparkHost,cemod_hdfs_user,sparkUrl)).replace("|"," ").strip()
        print 'On the host {0} for Spark Thrift Portal, result received is {1}'.format(sparkHost,sampleQueryOutput)

def getSparkStatus(typeOfSpark):
        print 'Type of Spark received is',typeOfSpark
        if typeOfSpark == "sparkThriftPortal":
                sparkHosts = cemod_spark_thrift_hosts.split(' ')
                sparkUrl = cemod_spark_thrift_url
        elif typeOfSpark == "sparkThriftFlexi":
                sparkHosts = cemod_spark_thrift_flexi_hosts.split(' ')
                sparkUrl = cemod_spark_thrift_flexi_url
        elif typeOfSpark == "sparkThriftApp":
                sparkHosts = cemod_spark_thrift_app_hosts.split(' ')
                sparkUrl = cemod_spark_thrift_app_url
        for sparkHost in sparkHosts:
                executeSparkQuery(sparkHost,sparkUrl)

def getRestartPropertyFromConfigXml(sparkTypeFlag):
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('ServiceRestart')
        propertyTag=parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                return [ propertyTag[propertyNum].attributes['value'].value for propertyNum in range(len(propertyTag)) if sparkTypeFlag == propertyTag[propertyNum].attributes['Name'].value ][0]

def statusCheck(typeOfSpark):
        print 'Restart Successful for {0}'.format(typeOfSpark)
        content = '{0}, Restart successful for {1} \n'.format(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),typeOfSpark)
        restartedInstances.append(typeOfSpark)
        fileHandler = open('/var/local/monitoring/work/sparkRestartLog_%s.log'%(datetime.datetime.now().strftime("%Y-%m-%d")),'a')
        fileHandler.write(content)
        fileHandler.close()

def triggerRestart(typeOfSpark):
        status,restartOutput = commands.getstatusoutput('ssh {0} "python /opt/nsn/ngdb/ifw/bin/platform/Cloudera/service_scripts/service.py \"{1}\" {2} "'.format(cemod_hive_hosts.split(' ')[0],cemod_hive_hosts.split(' ')[0],typeOfSpark))
        if status == 0:
                statusCheck(typeOfSpark)

def restartSparkIfNeeded(hungFlag,typeOfSpark):
        if hungFlag == "yes" and typeOfSpark == "sparkThriftPortal":
                sparkPortalRestartFlag = getRestartPropertyFromConfigXml("sparkThriftPortalRestart")
                if sparkPortalRestartFlag.lower() == "true":
                        print 'Restart is required for the type {0}'.format(typeOfSpark)
                        triggerRestart("SPARK_THRIFT")
                else: print 'Spark Thrift Portal Restart Flag in monitoring.xml is False, so restart of Spark Thrift Portal is not required'
        elif hungFlag == "yes" and typeOfSpark == "sparkThriftFlexi":
                sparkFlexiRestartFlag = getRestartPropertyFromConfigXml("sparkThriftFlexiRestart")
                if sparkFlexiRestartFlag.lower() == "true":
                        print 'Restart is required for the type {0}'.format(typeOfSpark)
                        triggerRestart("SPARK_THRIFT_FLEXIREPORT")
                else: print 'Spark Thrift Flexi Restart Flag in monitoring.xml is False, so restart of Spark Thrift Flexi is not required'
        elif hungFlag == "yes" and typeOfSpark == "sparkThriftApp":
                sparkThriftAppRestartFlag = getRestartPropertyFromConfigXml("sparkThriftAppRestart")
                if sparkThriftAppRestartFlag.lower() == "true":
                        print 'Restart is required for the type {0}'.format(typeOfSpark)
                        triggerRestart("SPARK_THRIFT_APPREPORT")
                else: print 'Spark Thrift App Flag in monitoring.xml is False, so restart of Spark Thrift App is not required'
        else:   print 'Restart is not required'

def runSparkAsProcessAndGetStatus(typeOfSpark):
        waitTime = 120
        sparkMultiProcessThread = multiprocessing.Process(target=getSparkStatus, name="getSparkStatus",args=(typeOfSpark,))
        sparkMultiProcessThread.start()
        time.sleep(waitTime)
        if sparkMultiProcessThread.is_alive():
                hungFlag = "yes"
                sparkMultiProcessThread.terminate()
                print 'Response not received for Spark Thrift Portal in {0} seconds and hung status is {1}'.format(waitTime,hungFlag)
        else: hungFlag = "no"
        sparkMultiProcessThread.join()
        return hungFlag


def main():
        global restartedInstances
        restartedInstances = []
        restartedInstances.append("Spark Types")
        sparkTypes = ['sparkThriftPortal','sparkThriftFlexi','sparkThriftApp']
        for sparkType in sparkTypes:
                hungStatus = runSparkAsProcessAndGetStatus(sparkType)
                restartSparkIfNeeded(hungStatus,sparkType)
        if len(restartedInstances) > 1:
                html = HtmlUtil().generateHtmlFromList('Below are the spark Instance(s) restarted automatically as query is in Hung State for 120 seconds', restartedInstances)                
		EmailUtils().frameEmailAndSend("[MAJOR_ALERT]:Spark Restart Alert on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),html)

if __name__ == '__main__':
        main()
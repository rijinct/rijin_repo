#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This checks the different postgres user connections and on breach
# of configured threshold value, raises an alert through email
# Date : 27-03-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
#
#############################################################################
import subprocess, sys, datetime
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dbConnectionManager import DbConnection
from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from logger_util import *
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from snmpUtils import SnmpUtils
from writeCsvUtils import CsvWriter

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

ALARM_KEY = AlarmKeys.POSTGRES_USERS_CONNECTION_ALARM.value


def getUsersThresholdValue():
        xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
        parentTopologyTag = xmlparser.getElementsByTagName('PostgresUserInformation')
        propertyTag = parentTopologyTag[0].getElementsByTagName('property')
        for propertyelements in propertyTag:
                usersThresholdValue = propertyelements.attributes['userconnectionthreshold'].value
        return usersThresholdValue


def frameEmailAndSend(thresholdBreachedDict):
        severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
        htmlMessage = HtmlUtil().generateHtmlFromDictList("Postgres User's Connections Information", [thresholdBreachedDict])
        EmailUtils().frameEmailAndSend("[{0} ALERT]:Postgres User's Connection Alert on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), htmlMessage, ALARM_KEY)
        SnmpAlarm.send_alarm(ALARM_KEY, thresholdBreachedDict)


def checkThresholdAndSendAlert(userConnections):
        thresholdBreachedDict = {}
        configuredThresholdValue = getUsersThresholdValue()
        logger.debug("Configured Threshold value in XML is '%s'", configuredThresholdValue)
        for user in userConnections.split("\n"):
                if int(user.split(',')[2]) >= int(configuredThresholdValue):
                        thresholdBreachedDict['User'] = user.split(',')[1].strip()
                        thresholdBreachedDict['Current Connections'] = user.split(',')[2]
                        thresholdBreachedDict['Threshold Value'] = str(configuredThresholdValue)
        return thresholdBreachedDict


def main():
        postgresUserConnectionsSql = PropertyFileUtil('postgresUserConnections', 'PostgresSqlSection').getValueForKey()
        postgresUserResultSetStatus = DbConnection().getConnectionAndExecuteSql(postgresUserConnectionsSql, "postgres")
        CsvUtil().executeAndWriteToCsv('postgresUsersCount', postgresUserConnectionsSql)
        thresholdBreachedDict = checkThresholdAndSendAlert(postgresUserResultSetStatus)
        if thresholdBreachedDict:
                frameEmailAndSend(thresholdBreachedDict)
                logger.info("Breached Postgre user connection list : '%s' ", thresholdBreachedDict)
        else:
                logger.info('Postgres User Connections are ok')


if __name__ == "__main__":
        main()

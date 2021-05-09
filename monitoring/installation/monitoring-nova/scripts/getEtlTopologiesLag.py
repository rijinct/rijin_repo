#!/usr/bin/env python
#############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:Monitoring Team
# Version: 0.1
# Purpose:To check the Lag of ETL Topologies
#
# Date:    08-03-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

from collections import OrderedDict
import subprocess
import sys, datetime, subprocess, os, csv , time
from xml.dom import minidom

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from jsonUtils import JsonUtils
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from logger_util import *
from monitoring_utils import XmlParserUtils
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm
from writeCsvUtils import CsvWriter
from writeCsvUtils import CsvWriter

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))
configurationXml = '/opt/nsn/ngdb/monitoring/conf/monitoring.xml'

### Variables assignment ###

thresholdValueConfigured = 0
htmlUtil = HtmlUtil()
htmlBody = ''
typeOfUtil="etlTopologiesLag"
thresholdBreachedDict = {}
dirPath = ''

rightnow = time.time()
curDateTime = datetime.datetime.utcfromtimestamp(rightnow).strftime("%Y-%m-%d %H:%M:%S")
curDateTimePod = datetime.datetime.fromtimestamp(rightnow).strftime("%Y-%m-%d %H:%M:%S")

ALARM_KEY = AlarmKeys.ETL_LAG_ALARM.value


def getTopologiesConfigured(application=None):
        return readConf('Name',application)

def readConf(value,application):
        global parentTopologyTag
        if application == 'AE':
                parentTagName = 'TOPOLOGIESAE'
        else: parentTagName = 'TOPOLOGIES'
        return ['%s_1' % topology for topology in topologies.keys()]

def raiseLagAlert():
        if htmlBody != '':
                EmailUtils().frameEmailAndSend("[CRITICAL_ALERT]:Breached Lag Connectors on {0}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),htmlBody)

def getLagCount(topologyName):
    global htmlBody
    finalStr = ""
    lagSum = 0
    currentOffset = 0
    endOffset = 0

    try:

        topic = (topologyName.rsplit('_',1)[0]).lower()+'.data'
        group = 'connect-'+topologyName+'_SINK_CONN'
        bootstrapServers = os.environ['KAFKA_BROKER_ENDPOINT']

        consumer = KafkaConsumer(
        bootstrap_servers=bootstrapServers,
        group_id=group,
        enable_auto_commit=False
        )

        partitions = consumer.partitions_for_topic(topic)

        if partitions is not None:
            for p in partitions:
                tp = TopicPartition(topic, p)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                lastOffset = consumer.position(tp)
                endOffset += lastOffset

        admin = KafkaAdminClient(
        bootstrap_servers = os.environ['KAFKA_BROKER_ENDPOINT']
        )

        dict = admin.list_consumer_group_offsets(group,group_coordinator_id=None, partitions=None)
        for item in dict.values():
            currentOffset+=int(str(item).partition("offset")[2].partition("metadata")[0].replace('=','').replace(',',''))

        if currentOffset is not None:
            if endOffset is not None:
                 lagSum = endOffset - currentOffset

        consumer.close(autocommit=False)
        logger.debug("currentOffset is '%s' endOffset is '%s' and lag is '%s' " , currentOffset, endOffset, endOffset - currentOffset)
        checkLagThreshold(lagSum, topologyName)
        finalStr = str(curDateTime) + ',' + str(curDateTimePod) + ',' + topologyName + ',' + str(lagSum)
        return finalStr

    except:
        logger.exception('Exception in getting Lag for %s' % topologyName)
        finalStr = str(curDateTime) + ',' + str(curDateTimePod) + ',' + topologyName + ',' + '-1'
        return finalStr

def checkLagThreshold(lag, topoName):
        threshold_breached_dict = {}
        thresholdValueFromConf = getLagThresholdValue(topoName)

        if lag >= thresholdValueFromConf:
            threshold_breached_dict['Connector'] = topoName
            threshold_breached_dict['Current Lag'] = str(lag)
            threshold_breached_dict['Threshold'] = str(thresholdValueFromConf)
            lag_threshold_breached_list.append(threshold_breached_dict)
        else:
            return ''


def getLagThresholdValue(topologyName):
        return int(topologies[topologyName.replace(
                '_1', '')]['KafkaLagThreshold'])

def checkTopology(topologiesInConfig):
        if len(topologiesInConfig) == 1 and len(topologiesInConfig[0]) == 0:
                logger.exception('Configure atleast one topology in monitoring.xml')

def lagCountImpl():
        global htmlBody,outputFile
        summaryStr=""
        topologiesInConfig=getTopologiesConfigured()
        checkTopology(topologiesInConfig)
        for topology in topologiesInConfig:
            logger.info("connector name is --> connect-'%s'_SINK_CONN", topology)
            summaryStr += getLagCount(topology) + '\n'

        outputFile=typeOfUtil+''+datetime.datetime.today().strftime("%Y%m%d%H%M")+'.csv'
        summaryStr = os.linesep.join([s for s in summaryStr.splitlines() if s])
        CsvWriter(typeOfUtil,outputFile,summaryStr)
        logger.info("summary of lag count: '%s' ", summaryStr)


def sendAlert():
        if len(lag_threshold_breached_list) > 0:
            severity = SnmpAlarm.get_severity_for_email(ALARM_KEY)
            html = str(HtmlUtil().generateHtmlFromDictList("Topologies Breached Lag Threshold", lag_threshold_breached_list))
            EmailUtils().frameEmailAndSend("[{0} ALERT]:ETL topology lag exceeding threshold Alert on {1}".format(severity, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html, ALARM_KEY)
            SnmpAlarm.send_alarm(ALARM_KEY, lag_threshold_breached_list)


def pushDataToMariaDB():
        global dirPath
        header=PropertyFileUtil(typeOfUtil,'HeaderSection').getValueForKey()
        dirPath = PropertyFileUtil(typeOfUtil,'DirectorySection').getValueForKey()
        outputFilePath = PropertyFileUtil('dimensionCount','DirectorySection').getValueForKey()
        outputFilePath = dirPath + outputFile
        jsonFileName=JsonUtils().convertCsvToJson(outputFilePath)
        DBUtil().pushToMariaDB(jsonFileName,"lagcount")


def main():
        global topologies, lag_threshold_breached_list
        lag_threshold_breached_list = []
        topologies = XmlParserUtils.get_topologies_from_xml()
        lagCountImpl()
        pushDataToMariaDB()
        sendAlert()

if __name__ == "__main__":
    main()

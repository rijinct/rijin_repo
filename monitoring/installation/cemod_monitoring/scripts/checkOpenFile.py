#!/usr/bin/python
############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  shivam.1.sharma@nokia.com
# Version: 0.1
# Purpose: Check the number of open files
#
# Date:    02-02-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft
# 2.
#############################################################################

import commands,logging,datetime

def createOutputCSV():
        global filepath
#        status,makeDIR = commands.getstatusoutput('mkdir -p /opt/nsn/ngdb/monitoring/output/checkOpenFile')
        filename = '/var/local/monitoring/output/checkOpenFile/checkOpenFile_'
        date = datetime.date.today()
        filepath = filename + str(date) + '.csv'
        status1,output1 = commands.getstatusoutput('ls -ltr ' + filepath)
        if status1 != 0:
                a = 'touch ' + filepath
                createFile = commands.getoutput('touch ' +  filepath)

def appendHeader():
        header = 'Date,Component,LSOF'
        header = header + '\n'
        new_file = open(filepath,'a')
        new_file.write(header)
        new_file.close()

def getNodeName(nodeName):
        if nodeName == 'Hive':
                get_hive_nodes = cemod_hive_hosts
                get_hive_nodes = get_hive_nodes.split()
                print get_hive_nodes[0]
                print get_hive_nodes[1]
                for node in get_hive_nodes:
                        getLSOF(node,nodeName)
        elif nodeName == 'SparkThriftFlexi' or nodeName == 'SparkThriftPortal':
                get_spark_nodes = cemod_spark_thrift_hosts
                get_spark_nodes = get_spark_nodes.split()
                print 'List of spark nodes is',get_spark_nodes

                for node in get_spark_nodes:
                        getLSOF(node,nodeName)
        elif nodeName == 'DAL':
                get_dal = cemod_dalserver_active_fip_host
                print get_dal
                getLSOF(get_dal,nodeName)
        elif nodeName == 'KafkaBroker':
                kafka_broker_hosts_space = cemod_kakfa_broker_hosts
                kafka_broker_hosts = kafka_broker_hosts_space.strip().split(" ")
                print 'Kafka Broker Hosts is',kafka_broker_hosts
                for kafka_broker_host in kafka_broker_hosts:
                        getLSOF(kafka_broker_host,nodeName)
        elif nodeName == 'CouchBase':
                couchbase_hosts = cemod_couchbase_hosts.split(' ')
                print 'CouchBase Hosts is',couchbase_hosts
                for couchbase_host in couchbase_hosts:
                        getLSOF(couchbase_host,nodeName)

def getLSOF(hostname,process):
        get_pID_cmd = 'ssh %s "ss -nlp | grep :%s | gawk -F\\",\\" \'{print \\$2}\' |gawk -F \\"=\\" \'{print \\$2}\' | awk -F \\"/\\" \'{print \\$1}\'"' %(hostname,port[process])
        print 'lsof command for ' + hostname + ': ' + get_pID_cmd
        status,get_pID = commands.getstatusoutput(get_pID_cmd)
        print 'pID for ' + hostname + ': ' + get_pID
        if get_pID != '':
                lsof_count_cmd = 'ssh %s "lsof -p %s | wc -l"' %(hostname,get_pID)
                print 'lsof_count_cmd', lsof_count_cmd
                status,lsof_count = commands.getstatusoutput(lsof_count_cmd)
                print 'lsof count for ' + hostname + ': ' + lsof_count
                log = process +'_' + hostname + ',' + lsof_count
                print 'Log for ' + hostname + ': ' + log
                print '------------------------------------'
                logIntoFile(log)
        else:
                log = process +'_' + hostname + ',NA'
                print 'Log for ' + hostname + ': ' + log
                print '------------------------------------'
                logIntoFile(log)

def logIntoFile(log):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s,%(message)s',
                    datefmt='%d %b %Y %H:%M:%S',
                    filename=filepath,
                    filemode='a')
        logging.info(log)
        l = logging.getLogger()
        for hdlr in l.handlers[:]:  # remove all old handlers
                l.removeHandler(hdlr)

port = {'Hive':'10000','SparkThriftPortal':'20000','SparkThriftFlexi':'20001','DAL':'10548','KafkaBroker':'9092','CouchBase':'8091'}
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
createOutputCSV()
appendHeader()
for key in port:
        getNodeName(key)

############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script checks the status of source & sink connectors for different
# adaptations configured in monitoring.xml
# Date:    28-09-2017
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality of status check, write to csv & email alert
#############################################################################
from xml.dom import minidom
from collections import OrderedDict, defaultdict
import commands, datetime, json, os, csv, sys, ast, ConfigParser, StringIO

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from sendMailUtil import EmailUtils
from htmlUtil import HtmlUtil
from snmpUtils import SnmpUtils
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from enum_util import CheckConnectors
from loggerUtil import loggerUtil

htmlUtil = HtmlUtil()

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)

global topologiesConfigured
configurationXml = '/opt/nsn/ngdb/monitoring/conf/monitoring.xml'
applicationXml = '/opt/nsn/ngdb/ifw/etc/application/application_config.xml'
commonXml = '/opt/nsn/ngdb/ifw/etc/common/common_config.xml'
logPath = '/var/local/monitoring/output/topology_status/'
exec(
    open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
    read()).replace("(", "(\"").replace(")", "\")")
currentDate = datetime.date.today()
currentTime = datetime.datetime.now().strftime("%H:%M:%S")
topologiesToStart = set()
topologiesToStartAE = set()
tasks_status_list = []
connector_status_dict_list = []
type_of_util = "etlTopologiesLag"


def readConf(value, application):
    global parentTopologyTag
    if application == 'AE':
        parentTagName = 'TOPOLOGIESAE'
    else:
        parentTagName = 'TOPOLOGIES'
    topologiesConfigured = []
    xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentTopologyTag = xmlparser.getElementsByTagName(parentTagName)
    propertyTag = parentTopologyTag[0].getElementsByTagName('property')
    return [
        element.attributes[value].value for element in propertyTag
        if element.hasAttribute(value)
    ]


def getLagThresholdValue(topologyName):
    xmlparser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentTopologyTag = xmlparser.getElementsByTagName('TOPOLOGIES')
    propertyTags = parentTopologyTag[0].getElementsByTagName('property')
    return int([
        propertyTag.attributes['lagThreshold'].value
        for propertyTag in propertyTags
        if propertyTag.hasAttribute('lagThreshold')
        and topologyName == propertyTag.attributes['Name'].value
    ][0])


def getTopologiesConfigured(application=None):
    return readConf('Name', application)


def checkEnableTopologyParam(application=None):
    return ast.literal_eval(readConf('enableRestart', application)[0].title())


def getApplicationHost():
    applicationHost = cemod_kafka_app_hosts
    return applicationHost.split()


def getConnectorsPort(topologyInConfig):
    global kafkaAdapTag
    xmlparser = minidom.parse(applicationXml)
    kafkaAdapTag = xmlparser.getElementsByTagName('Kafka-adap-port')
    propertyKafkaTag = kafkaAdapTag[0].getElementsByTagName('property')
    for kafkaAdapList in propertyKafkaTag:
        for kafkaAdap in range(len(propertyKafkaTag)):
            if topologyInConfig == propertyKafkaTag[kafkaAdap].attributes[
                    'name'].value:
                return propertyKafkaTag[kafkaAdap].attributes['value'].value


def getTopologyName(topology):
    return commands.getoutput(
        'ssh {0} "/opt/nsn/ngdb/pgsql/bin/psql sai sairepo -c \\"select adaptationid from adapt_cata where id in (( select adaptationid from adap_entity_rel where usagespecid in ((select id from usage_spec where specid in (\'{1}\')))));\\"" | egrep -v "rows|row|--|adaptationid"|head -1|awk \'$1=$1\''
        .format(cemod_postgres_sdk_fip_active_host, topology))


def restartTopology(topologiesToStart, application=None):
    print('Restarting topologies since Flag is Enabled')
    print('TopologiesToStart', topologiesToStart)
    for topology in topologiesToStart:
        if application == 'AE':
            commands.getoutput(
                '/opt/nsn/actionEngine/etl/scripts/etl_rtb.sh stop {0} &>/dev/null'
                .format(topology))
            print('{0} killed. Starting now'.format(topology))
            commands.getoutput(
                '/opt/nsn/actionEngine/etl/scripts/etl_rtb.sh start {0} &>/dev/null'
                .format(topology))
        else:
            commands.getoutput(
                '/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -kill {0}'
                .format(topology))
            print('{0} killed. Starting now'.format(topology))
            adaptationname = getTopologyName(topology)
            commands.getoutput(
                '/opt/nsn/ngdb/etl/app/bin/etl-management-service.sh -adaptation {0} -version 1 -topologyname {2}'
                .format(adaptationname, topology))
        print('{0} Started'.format(topology))


def getAdaptationStatus(topology):
    adaptationStatus = OrderedDict()
    adaptationStatus['name'] = topology
    adaptationStatus['state'] = 'STOPPED'
    return [adaptationStatus]


def getConnectorStatus(statusDict):
    connectorStatus = OrderedDict()
    connectorStatus['name'] = statusDict['name']
    connectorStatus['state'] = statusDict['connector']['state']
    return connectorStatus


def getTaskStatus(statusDict, state):
    name = statusDict['name']
    tasks = statusDict['tasks']
    taskList = []
    for task in tasks:
        if task['state'] == state:
            d = OrderedDict()
            d['source'] = name
            d['id'] = str(task['id'])
            d['state'] = task['state']
            taskList.append(d)
    return taskList


def getLagCount(prop, topologyName):
    global htmlBody
    lag_count = OrderedDict()
    print('Getting lag_count for {0}'.format(prop['group']))
    if cemod_platform_distribution_type == "cloudera":
        cmd = "/usr/bin/kafka-consumer-groups -bootstrap-server {0}:9092  --describe --group connect-{1} | awk '{{ sum+=$5 }} END {{ print sum }}'".format(
            cemod_kakfa_broker_hosts.split(' ')[0], prop['group'])
    else:
        cmd = "/usr/bin/kafka-consumer-groups -bootstrap-server {0}:9092  --describe --group connect-{1} | awk '{{ sum+=$6 }} END {{ print sum }}'".format(
            cemod_kakfa_broker_hosts.split(' ')[0], prop['group'])
    lag_sum = commands.getoutput(cmd).split('\n')[-1]
    htmlBody += checkLagThreshold(lag_sum, prop['group'], topologyName)
    lag_count['Time'] = currentTime
    lag_count['Connector'] = prop['group']
    if lag_sum:
        lag_count['LagCount(Messages)'] = lag_sum
        lag_count['LagCount(Records)'] = int(lag_sum) * 1000
    else:
        lag_count['LagCount(Messages)'] = -1
        lag_count['LagCount(Records)'] = 0
    return lag_count


def checkLagThreshold(lagSum, sinkOrSourceName, topoName):
    print('Lag Sum has', lagSum)
    if not lagSum:
        lagSum = -1
    thresholdValueFromConf = getLagThresholdValue(topoName)
    if int(lagSum) >= thresholdValueFromConf:
        breachedThreshold['Connector Name'] = sinkOrSourceName
        breachedThreshold['Count'] = lagSum
        return str(
            htmlUtil.generateHtmlFromDictList(
                "Breached Lag Threshold Connectors", [breachedThreshold]))
    else:
        return ''


def raiseLagAlert():
    if htmlBody != '':
        EmailUtils().frameEmailAndSend(
            "[CRITICAL_ALERT]:Breached Lag Connectors on {0}".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            htmlBody)


def writeToCsv(log, filePath, fileName):
    print('Writing data to %s' % (filePath))
    if not os.path.exists(filePath):
        commands.getoutput('mkdir -p %s' % (filePath))
    with open(filePath + fileName, 'ab') as f:
        writer = csv.DictWriter(f, log[0].keys())
        fields = log[0].keys()
        writer.writerow(dict(zip(fields, fields)))
        writer.writerows(log)


def checkTopology(topologiesInConfig):
    if len(topologiesInConfig) == 1 and len(topologiesInConfig[0]) == 0:
        print('Configure atleast one topology in monitoring.xml')
        exit(0)


def getConnectorName(host, port):
    status, connectorName = commands.getstatusoutput(
        'ssh %s "curl -s http://%s:%s/connectors"| gawk -F"\\"" \'{print $2}\''
        % (host, cemod_kafka_app_hosts.split()[0], port))
    return connectorName


def restartTopoOnTrap(flag):
    if flag == "Error":
        restartAfterXmlCheck(flag)
        exit(2)


def restartAfterXmlCheck(flag):
    if checkEnableTopologyParam():
        restartTopology(topologiesToStart)
        topologyHtml = str(
            htmlUtil.generateHtmlFromDictList(
                'List of Topologies Restarted',
                topologyRestartList(topologiesToStart)))
        print('Topologies in stopped state', topologiesToStart)
        print('Sending Topology Restart Email')
        EmailUtils().frameEmailAndSend(
            "[INFO]:Kafka Topology Restart/Resume Status on {0}".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            topologyHtml)


def getTopologyOrConnectorsPortAE(topology, topologyFlag=None):
    aeEtlPropFile = '/opt/nsn/actionEngine/etl/config/ae_etl_kafka_startup.properties'
    aeEtl = '[aeSection]\n' + open(aeEtlPropFile, 'r').read()
    aeEtlStr = StringIO.StringIO(aeEtl)
    configLoad = ConfigParser.RawConfigParser()
    configLoad.readfp(aeEtlStr)
    topologyName = topology.split("_1")[0].lower()
    if topologyFlag:
        return topologyName
    else:
        return configLoad.get(
            'aeSection',
            '{0}.rest.port.source'.format(topologyName)), configLoad.get(
                'aeSection', '{0}.rest.port.sink'.format(topologyName))


def connectorStatusImplAE():
    application = 'AE'
    html = ''
    topologiesInConfig = getTopologiesConfigured(application)
    checkTopology(topologiesInConfig)

    for topology in topologiesInConfig:
        ports = getTopologyOrConnectorsPortAE(topology)
        for port in ports:
            connector = getConnectorName(applicationHost[0], port)
            if len(connector) == 0:
                print('Adapation {0} is Stopped'.format(topology))
                label = 'Adaptation Status for {0}'.format(topology)
                html += str(
                    htmlUtil.generateHtmlFromDictList(
                        label, getAdaptationStatus(topology)))
                topologiesToStartAE.add(
                    getTopologyOrConnectorsPortAE(topology, 'yes'))
                break
            elif connector == 'error_code':
                print('Adapation {0} is in Error State'.format(topology))
                label = 'Adaptation Status for {0}'.format(topology)
                adaptationStatus = [
                    OrderedDict([('name', topology), ('state', 'ERROR')])
                ]
                html += str(
                    htmlUtil.generateHtmlFromDictList(label, adaptationStatus))
                topologiesToStartAE.add(
                    getTopologyOrConnectorsPortAE(topology, 'yes'))
            else:
                html += connectorJsonParser(topology, port, connector,
                                            application)
    if html != '':
        print('Sending Alert Email')
        EmailUtils().frameEmailAndSend(
            "[CRITICAL_ALERT]:Kafka Connector Fail Status for AE on {0}".
            format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            html)
        snmpIp, snmpPort, snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
        status, sendSnmpTrap = commands.getstatusoutput(
            '/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::toplogyAEStatus SAI-MIB::Connectors "{3}"'
            .format(snmpCommunity, snmpIp, snmpPort, topologiesToStart))
        if str(status) == '0':
            print('SNMP Traps sent successfully.')
        else:
            print('Error in sending SNMP Trap.')
    else:
        print('All CONNECTORS/TASKS are RUNNING and adaptations are RUNNING')

    if len(topologiesToStartAE) > 0 and checkEnableTopologyParam(application):
        restartTopology(topologiesToStartAE, application)
        topologyHtml = str(
            htmlUtil.generateHtmlFromDictList(
                'List of Topologies Restarted',
                topologyRestartList(topologiesToStartAE)))
        print('Sending Topology Restart Email for AE')
        EmailUtils().frameEmailAndSend(
            "[INFO]:Kafka Topology Restart/Resume Status for AE on {0}".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            topologyHtml)
        exit(2)
    else:
        print('No Toplogies Connectors are stopped')
        exit(0)


def topologyRestartList(topologiesToStart):
    return [{'Topology': topology} for topology in topologiesToStart]


def connectorJsonParser(topology, port, connector, application=None):
    connectorHtml = ''
    getStatusCmd = 'ssh {0} "curl -s http://{1}:{2}/connectors/{3}/status"'.format(
        applicationHost[0],
        cemod_kafka_app_hosts.split()[0], port, connector)
    status = commands.getoutput(getStatusCmd)
    print(status)
    if len(status) == 0:
        print('Adapation {0} is Stopped'.format(topology))
        label = 'Adaptation Status for {0}'.format(topology)
        connectorHtml += str(
            htmlUtil.generateHtmlFromDictList(label,
                                              getAdaptationStatus(topology)))
        if application == 'AE':
            topologiesToStartAE.add(
                getTopologyOrConnectorsPortAE(topology, 'yes'))
        else:
            topologiesToStart.add(topology)
    else:
        statusDict = json.loads(status)
        print(statusDict)
        if 'error_code' not in statusDict.keys():
            connectorStatus = getConnectorStatus(statusDict)
            connectorHtml += checkTaskState(topology, connectorStatus,
                                            statusDict, port, application)
    return connectorHtml


def checkTaskState(topology,
                   connectorStatus,
                   statusDict,
                   port,
                   application=None):
    taskHtml = ''
    if connectorStatus['state'] == 'RUNNING' and len(statusDict['tasks']) == 0:
        print('No tasks running for {0}'.format(topology))
        label = 'Adaptation Status for {0}'.format(topology)
        taskHtml += str(
            htmlUtil.generateHtmlFromDictList(label,
                                              getAdaptationStatus(topology)))
        if application == 'AE':
            topologiesToStartAE.add(
                getTopologyOrConnectorsPortAE(topology, 'yes'))
        else:
            topologiesToStart.add(topology)
    elif connectorStatus['state'] == 'RUNNING' and len(
            statusDict['tasks']) > 0:
        taskHtml += getTaskHtml(statusDict, port, topology, application)
    elif connectorStatus['state'] == 'PAUSED':
        print('Connector {0} and its Tasks are Paused'.format(
            connectorStatus['name']))
        label = 'Connector {0} and its Tasks are Paused'.format(
            connectorStatus['name'])
        taskHtml += str(
            htmlUtil.generateHtmlFromDictList(label, [connectorStatus]))
        resumePausedState(connectorStatus['name'], port)
    else:
        print('Adapation {0} is Failed'.format(topology))
        label = 'Adaptation Status for {0}'.format(topology)
        taskHtml += str(
            htmlUtil.generateHtmlFromDictList(label,
                                              getAdaptationStatus(topology)))
        if application == 'AE':
            topologiesToStartAE.add(
                getTopologyOrConnectorsPortAE(topology, 'yes'))
        else:
            topologiesToStart.add(topology)

    return taskHtml


def getTaskHtml(statusDict, port, topology, application=None):
    taskHtml = ''
    failedTasks = getTaskStatus(statusDict, 'FAILED')
    label = 'Connector {0} has failed Tasks'.format(statusDict['name'])
    if len(failedTasks) > 0:
        taskHtml += str(htmlUtil.generateHtmlFromDictList(label, failedTasks))
        if application == 'AE':
            topologiesToStartAE.add(
                getTopologyOrConnectorsPortAE(topology, 'yes'))
        else:
            topologiesToStart.add(topology)
    pausedTasks = getTaskStatus(statusDict, 'PAUSED')
    if len(pausedTasks) > 0:
        taskHtml += str(htmlUtil.generateHtmlFromDictList(label, pausedTasks))
        resumePausedState(statusDict['name'], port)
    return taskHtml


def resumePausedState(connectorName, port):
    print('Resuming {0}'.format(connectorName))
    resumeCmd = 'ssh {0} "curl -X put http://{1}:{2}/connectors/{3}/resume"'.format(
        applicationHost[0],
        cemod_kafka_app_hosts.split()[0], port, connectorName)
    commands.getoutput(resumeCmd)


def frameSnmpAndSend(lagList):
    snmpIp, snmpPort, snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
    status, sendSnmpTrap = commands.getstatusoutput(
        '/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::lagInfo SAI-MIB::Connectors s "{3}"'
        .format(snmpCommunity, snmpIp, snmpPort, lagList))
    if status == '0':
        print('SNMP Traps sent successfully.')
    else:
        print('Error in sending SNMP Trap.')


def pushDataToPostgresDB():
    global dirPath
    output_file = fileName = 'lagCount_{0}.csv'.format(currentDateAndTime)
    dir_path = PropertyFileUtil(type_of_util,
                                'DirectorySection').getValueForKey()
    output_file_path = dir_path + output_file
    jsonFileName = JsonUtils().convertCsvToJson(output_file_path)
    DBUtil().pushDataToPostgresDB(jsonFileName, "lagcount")


def lagCountImpl():
    global currentDateAndTime
    global lagList
    propDict = {}
    currentDateAndTime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    lagList = list()
    topologiesInConfig = getTopologiesConfigured()
    checkTopology(topologiesInConfig)
    global htmlBody
    for topology in topologiesInConfig:
        propDict['group'] = topology + "_SINK_CONN"
        lagList.append(getLagCount(propDict, topology))
    fileName = 'lagCount_{0}.csv'.format(currentDateAndTime)
    if len(lagList) > 0:
        writeToCsv(lagList, '/var/local/monitoring/output/topology_status/',
                   fileName)
        pushDataToPostgresDB()
    else:
        print('No lag found')


def write_connectors_status_info_to_csv():
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    topologiesInConfig = getTopologiesConfigured()
    for topology in topologiesInConfig:
        fetch_and_write_connectors_tasks_status(current_time, topology)


def fetch_and_write_connectors_tasks_status(current_time, topology):
    try:
        list_of_connectors_status = get_connector_type_and_status(topology)
        if list_of_connectors_status:
            fetch_status_data_and_write_to_csv(current_time,
                                               list_of_connectors_status)
    except AttributeError as e:
        LOGGER.info(e)


def fetch_status_data_and_write_to_csv(current_time,
                                       list_of_connectors_status):
    for data in list_of_connectors_status:
        status, json_data = data[1]
        if not status:
            write_connector_task_status_to_csv(current_time, data, json_data)
        else:
            write_connector_tasks_status_down_to_csv(current_time, data)
            tasks_status_list.append([data[0], 'DOWN'])


def write_connector_tasks_status_down_to_csv(current_time, data):
    data_to_write = {
        'Time': current_time,
        'Connector_Type': data[0],
        'Connector_Status': str(CheckConnectors.RUNNING_STATUS_NOK.value),
        'Tasks_Status': str(CheckConnectors.RUNNING_STATUS_NOK.value),
        'Active_Tasks': '0',
        'Connector_Running_On_Node': '',
        'Tasks_Running_on_Nodes': '',
        'Total_Tasks': '0'
    }
    CsvUtil().writeDictToCsv('SourceSinkTasksStatus', data_to_write)
    data_to_push.append(json.dumps(data_to_write))


def write_connector_task_status_to_csv(current_time, data, status):
    status_data = json.loads(status)
    if status_data.get('error_code'):
        data_to_write = {
            'Time': current_time,
            'Connector_Type': data[0],
            'Connector_Status': str(CheckConnectors.RUNNING_STATUS_NOK.value),
            'Tasks_Status': str(CheckConnectors.RUNNING_STATUS_NOK.value),
            'Active_Tasks': '0',
            'Connector_Running_On_Node': '',
            'Tasks_Running_on_Nodes': '',
            'Total_Tasks': '0'
        }
        tasks_status_list.append([data[0], 'DOWN'])
    else:
        data_to_write = get_data_to_write(current_time, status_data)
    CsvUtil().writeDictToCsv('SourceSinkTasksStatus', data_to_write)
    data_to_push.append(json.dumps(data_to_write))


def get_data_to_write(current_time, status_data):
    active_tasks, total_tasks, tasks_status = get_active_and_total_tasks(
        status_data)
    tasks_running_on_nodes = ''
    add_tasks_status_to_list(tasks_status, str(status_data['name']))
    for worker_id in get_tasks_worker_id(status_data['tasks']).items():
        tasks_running_on_nodes += ' %s-%s' % (worker_id[0], str(worker_id[1]))
    if str(status_data['connector']['state']) == 'RUNNING':
        connector_state = str(CheckConnectors.RUNNING_STATUS_OK.value)
    else:
        connector_state = str(CheckConnectors.RUNNING_STATUS_NOK.value)
    data_to_write = {
        'Time': current_time,
        'Connector_Type': str(status_data['name']),
        'Connector_Status': connector_state,
        'Tasks_Status': str(tasks_status),
        'Active_Tasks': str(active_tasks),
        'Connector_Running_On_Node':
        str(status_data['connector']['worker_id']),
        'Tasks_Running_on_Nodes': str(tasks_running_on_nodes),
        'Total_Tasks': str(total_tasks)
    }
    return data_to_write


def add_tasks_status_to_list(tasks_status, connector):
    for status in [(1, 'PARTIAL RUNNING'), (2, 'NOT RUNNING')]:
        if status[0] == int(tasks_status):
            tasks_status_list.append([connector, status[1]])


def get_active_and_total_tasks(status_data):
    if len(status_data['tasks']) == 0:
        active_tasks, tasks_status = (0,
                                      CheckConnectors.RUNNING_STATUS_NOK.value)
        return active_tasks, len(status_data['tasks']), tasks_status
    else:
        active_tasks, tasks_status = get_task_status(status_data['tasks'])
        return active_tasks, len(status_data['tasks']), tasks_status


def get_task_status(tasks_data):
    active_tasks = defaultdict(int)
    RUNNING_STATE = 'RUNNING'
    for data in tasks_data:
        if data.items()[0][1] == RUNNING_STATE:
            active_tasks[RUNNING_STATE] += 1
    if active_tasks[RUNNING_STATE] == 0:
        return active_tasks[RUNNING_STATE], str(
            CheckConnectors.RUNNING_STATUS_NOK.value)
    elif active_tasks[RUNNING_STATE] < len(tasks_data):
        return active_tasks[RUNNING_STATE], str(
            CheckConnectors.RUNNING_STATUS_PARTIAL_OK.value)
    else:
        return active_tasks[RUNNING_STATE], str(
            CheckConnectors.RUNNING_STATUS_OK.value)


def get_tasks_worker_id(tasks_data):
    worker_id = defaultdict(int)
    for task in tasks_data:
        worker_id[task['worker_id']] += 1
    return worker_id


def get_connector_type_and_status(topology):
    ports = getConnectorsPort(topology)
    source_and_sink_conn_list = [
        ('%s_SOURCE_CONN' % topology, ports.split(",")[0]),
        ('%s_SINK_CONN' % topology, ports.split(",")[1])
    ]
    connector_status = []
    for item in source_and_sink_conn_list:
        status_cmd = 'ssh {0} "curl -s http://{1}:{2}/connectors/{3}/status"'.format(
            applicationHost[0],
            cemod_kafka_app_hosts.split()[0], item[1], item[0])
        status = commands.getstatusoutput(status_cmd)
        connector_status.append((item[0], status))
    return connector_status


def get_status_cmd(port, source_and_sink_conn_list):
    for connector_port_data in source_and_sink_conn_list:
        if port == connector_port_data[1]:
            status_cmd = 'ssh {0} "curl -s http://{1}:{2}/connectors/{3}/status"'.format(
                applicationHost[0],
                cemod_kafka_app_hosts.split()[0], port, connector_port_data[0])
            return connector_port_data[0], status_cmd


def get_connector_status_dict_list():
    for connector_item in tasks_status_list:
        connector_status = OrderedDict()
        if connector_item[1] in ('PARTIAL RUNNING', 'NOT RUNNING'):
            connector_status['connector_name'] = str(connector_item[0])
            connector_status['connector_status'] = 'RUNNING'
            connector_status['tasks_status'] = connector_item[1]
        else:
            connector_status['connector_name'] = str(connector_item[0])
            connector_status['connector_status'] = 'STOPPED'
            connector_status['tasks_status'] = 'NOT RUNNING'
        connector_status_dict_list.append(connector_status)
    return connector_status_dict_list


def connectorStatusImpl():
    label = 'Below Connectors (tasks) are not running'
    html = str(
        htmlUtil.generateHtmlFromDictList(label,
                                          get_connector_status_dict_list()))

    if html and tasks_status_list:
        send_email_alert_for_connector_status_down(html)
    else:
        LOGGER.info("No topologies/connectors down")


def send_email_alert_for_connector_status_down(html):
    EmailUtils().frameEmailAndSend(
        "[CRITICAL_ALERT]:Kafka Connector Fail Status on {0}".format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), html)
    snmpIp, snmpPort, snmpCommunity = SnmpUtils.getSnmpDetails(SnmpUtils())
    status, sendSnmpTrap = commands.getstatusoutput(
        '/usr/bin/snmptrap -v 2c -c {0} {1}:{2} \'\' SAI-MIB::toplogyStatus SAI-MIB::Connectors s "{3}"'
        .format(snmpCommunity, snmpIp, snmpPort, topologiesToStart))
    if not status:
        LOGGER.info('SNMP Traps sent successfully.')
    else:
        LOGGER.info('Error in sending SNMP Trap.')
    restartTopoOnTrap('Error')


def push_connector_status_to_postgres():
    HM_TYPE = 'SourceSinkTasksStatus'
    json_data = '[%s]' % (','.join(data_to_push))
    DBUtil().jsonPushToPostgresDB(json_data, HM_TYPE)


def main():
    global ports
    global applicationHost
    global breachedThreshold, htmlBody
    global data_to_push
    data_to_push = []
    breachedThreshold = OrderedDict()
    htmlBody = ''
    try:
        applicationHost = getApplicationHost()
        paramType = sys.argv[1]
        if paramType == 'status':
            write_connectors_status_info_to_csv()
            push_connector_status_to_postgres()
            connectorStatusImpl()
            if paramType == 'status' and cemod_application_content_pack_ICE_status == 'yes':
                connectorStatusImplAE()
        elif paramType == 'lag':
            lagCountImpl()
            raiseLagAlert()
            frameSnmpAndSend(lagList)
    except IndexError:
        print('[USAGE: python checkConnector.py status/lag]')


if __name__ == "__main__":
    main()

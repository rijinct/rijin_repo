import sys
import os, re, csv
import datetime
from commands import getoutput
from xml.dom import minidom
from collections import defaultdict, OrderedDict

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from jsonUtils import JsonUtils
from dbUtils import DBUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name, currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)
list_of_topology_status_on_nodes, node_down = [], []


def get_topologies():
    topologies = []
    xmlparser = minidom.parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parentTag = xmlparser.getElementsByTagName('TOPOLOGIES')[0]
    propertyTag = parentTag.getElementsByTagName('property')
    for property in propertyTag:
        try:
            topologies.append((property.attributes.get('Name').value).split('_1')[0])
        except Exception as e:
            LOGGER.info(e)
    return topologies


def get_ping_status(node):
    command_str = 'ping \\-c 1 %s | grep "packet loss" | gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' ' % (
        node, "%")
    return getoutput(command_str)


def get_pid_list(toplology, hostname):
    command_str = 'ssh %s ps -eaf | grep -i ETLServiceStarter | grep %s' % (hostname, toplology)
    output_str = getoutput(command_str)
    if output_str:
        try:
            return get_source_sink_pid_list(output_str)
        except Exception as e:
            LOGGER.info(e)


def get_source_sink_pid_list(output_str):
    connectortype_data_list = list(
        filter(lambda x: True if not 'nohup' in x else False, output_str.strip().splitlines()))
    source_sink_pid_list = []
    try:
        for connectortype_data in connectortype_data_list:
            connectortype_data = connectortype_data.split()
            add_source_sink_pid(connectortype_data, source_sink_pid_list)
    except Exception as e:
        LOGGER.info(e)
    return source_sink_pid_list


def add_source_sink_pid(connectortype_data, source_sink_pid_list):
    for index, item in enumerate(connectortype_data):
        if 'connectortype' in item:
            source_sink_pid_list.append((connectortype_data[1], connectortype_data[index + 1]))


def get_mem_of_connector_type(pid, host_name):
    ou_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$8}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    eu_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$6}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    s0u_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$3}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    s1u_value = getoutput(
        """ssh {0} " jstat -gc {1} | gawk -F\\" \\" '{{print \\$4}}' " """.format(host_name,
                                                                                  pid)).split("\n")[1]
    return (float(ou_value) + float(eu_value) + float(s0u_value) + float(s1u_value)) / 1000


def get_connnector_type_stats(pid, node):
    cpu_use = getoutput(
        ''' ssh %s top -p %s -bn1 | grep "Cpu(s)" | awk '{print 100-$8}' ''' % (node, pid))
    open_files = getoutput('ssh %s "lsof -p %s | wc -l"' % (node, pid))
    mem = get_mem_of_connector_type(pid, node)
    return cpu_use, mem, open_files


def write_required_stats(topology, connector_type, node, pid=None):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    if pid is not None:
        cpu_use, mem, open_files = get_connnector_type_stats(pid, node)
        write_to_csv(topology, connector_type, node, current_time, cpu_use, mem, open_files)
    else:
        write_to_csv(topology, connector_type, node, current_time)


def write_to_csv(topology, connector_type, node, current_time, cpu_use=None, mem=None, open_files=None):
    data_to_write = {'Time': current_time, 'Node': node, 'Reachability_Status': 'OK', 'Topology': topology,
                     'Topology_Status': 'OK', 'Connector_type': connector_type}
    if not cpu_use and not mem and not open_files:
        write_topology_connector_type_status_down(cpu_use, data_to_write, mem, open_files)
    else:
        write_topology_connector_type_status_up(cpu_use, data_to_write, mem, open_files)
    LOGGER.info('Writing cpu, mem, openfiles of %s of %s running on %s to csvfile' % (connector_type, topology, node))
    CsvUtil().writeDictToCsv('TopologyStats', data_to_write)


def write_topology_connector_type_status_up(cpu_use, data_to_write, mem, open_files):
    data_to_write.update({'CPU_USE': cpu_use, 'TotalMEM': mem, 'OpenFiles': open_files})
    list_of_topology_status_on_nodes.append(
        (data_to_write['Node'], '%s_%s' % (data_to_write['Topology'], data_to_write['Connector_type']), 'OK'))
    for data_type in (cpu_use, mem, open_files):
        check_if_error(data_to_write, data_type)


def write_topology_connector_type_status_down(cpu_use, data_to_write, mem, open_files):
    for data_type in (cpu_use, mem, open_files):
        data_to_write[data_type] = 'NA'
        list_of_topology_status_on_nodes.append(
            (data_to_write['Node'], '%s_%s' % (data_to_write['Topology'], data_to_write['Connector_type']), 'NOK'))


def check_if_error(data_to_write, data_type):
    pattern = '[a-zA-Z]'
    try:
        if re.match(pattern, data_type):
            LOGGER.info(data_type)
            for key, value in data_to_write.items():
                if value == data_type:
                    data_to_write[key] = 'NA'
    except:
        pass


def write_topology_down_to_csv(topology, node):
    data_to_write = {'Time': current_time, 'Node': node, 'Reachability_Status': 'OK', 'Topology': topology,
                     'Topology_Status': 'Down', 'Connector_type': 'NA', 'CPU_USE': 'NA', 'TotalMEM': 'NA',
                     'OpenFiles': 'NA'}
    for connector_type in 'source', 'sink':
        list_of_topology_status_on_nodes.append(
            (data_to_write['Node'], '%s_%s' % (data_to_write['Topology'], connector_type), 'NOK'))
    CsvUtil().writeDictToCsv('TopologyStats', data_to_write)


def write_node_down_to_csv(node):
    data_to_write = {'Time': current_time, 'Node': node, 'Reachability_Status': 'Down', 'Topology': 'NA',
                     'Topology_Status': 'NA', 'Connector_type': 'NA', 'CPU_USE': 'NA', 'TotalMEM': 'NA',
                     'OpenFiles': 'NA'}
    node_down.append(node)
    CsvUtil().writeDictToCsv('TopologyStats', data_to_write)


def topology_status_on_nodes(topology):
    collect_status_data = defaultdict(list)
    for node, topology_connector, status in list_of_topology_status_on_nodes:
        if topology in topology_connector:
            collect_status_data[topology_connector].append((node, status))
    topology_status_info(OrderedDict(collect_status_data))


def topology_status_info(collect_status_data):
    status_to_write = OrderedDict()
    for topology_connector in collect_status_data:
        status_to_write['Topology_ConnectorType'] = topology_connector
        for node, status in collect_status_data[topology_connector]:
            status_to_write[node] = status
        for node in node_down:
            status_to_write[node] = 'NOK'
        status_to_write['Time'] = current_time
        write_topology_status_on_nodes(status_to_write)


def write_topology_status_on_nodes(topology_status):
    FIELD_NAMES = ['Time', 'Topology_ConnectorType'] + cemod_kafka_app_hosts.split()
    outPutDirectory = PropertyFileUtil('TopologyStats', 'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % ('TopologyStatusOnNodes', datetime.date.today())
    file_exist = os.path.exists(os.path.join(outPutDirectory, file_name))
    with open(os.path.join(outPutDirectory, file_name), 'a') as file_object:
        writer = csv.DictWriter(file_object, fieldnames=FIELD_NAMES)
        if not file_exist:
            writer.writeheader()
        writer.writerow(topology_status)


def write_topology_status(node, topology):
    topology_pid_list = get_pid_list(topology, node)
    if topology_pid_list:
        for data_item in topology_pid_list:
            write_required_stats(topology, data_item[1], node, data_item[0])
        check_connector_type_status(node, topology, topology_pid_list)
    else:
        write_topology_down_to_csv(topology, node)


def check_connector_type_status(node, topology, topology_pid_list):
    for connector_type in 'source', 'sink':
        if not connector_type in [data_item[1] for data_item in topology_pid_list]:
            write_to_csv(topology, connector_type, node, current_time)


def write_all_topologies_stats(node, topologies):
    for topology in topologies:
        write_topology_status(node, topology)


def topology_stats(topologies):
    for node in cemod_kafka_app_hosts.split():
        if get_ping_status(node):
            write_all_topologies_stats(node, topologies)
        else:
            write_node_down_to_csv(node)


def topology_status(topologies):
    for topology in topologies:
        topology_status_on_nodes(topology)


def push_to_postgres():
    UTIL_TYPE = 'TopologyStats'
    outPutDirectory = PropertyFileUtil(UTIL_TYPE, 'DirectorySection').getValueForKey()
    for util_name in UTIL_TYPE, 'TopologyStatusOnNodes':
        file_name = '%s_%s.csv' % (util_name, datetime.date.today())
        jsonFileName = JsonUtils().convertCsvToJson(os.path.join(outPutDirectory, file_name))
        LOGGER.info('Pushing all topologies statistics to Postgres DB ')
        DBUtil().pushDataToPostgresDB(jsonFileName, util_name)


def main():
    global topologies
    topologies = get_topologies()
    topology_stats(topologies)
    topology_status(topologies)
    push_to_postgres()


if __name__ == '__main__':
    main()

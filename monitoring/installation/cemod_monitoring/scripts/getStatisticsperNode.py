import csv
import datetime
import commands
import os
from collections import defaultdict
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from jsonUtils import JsonUtils
from dbUtils import DBUtil
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")
file_list = os.listdir(PropertyFileUtil('ServiceStability', 'DirectorySection').getValueForKey())
for file in file_list:
    if file.startswith('ServiceStability_%s' % datetime.datetime.today().strftime('%Y-%m-%d_%H')) and file.endswith(
            '.csv'):
        service_stability_file = file
        break

OK, NOT_OK, NOT_APPLICABLE, PARTIAL_OK = range(0, 4)


def set_nodes():
    global application_nodes
    global name_nodes
    global data_nodes
    application_nodes = cemod_application_hosts.split(' ')
    name_nodes = nodes_on_parent_host('cemod_hdfs_master')
    data_nodes = nodes_on_parent_host('cemod_hdfs_slaves')


def nodes_on_parent_host(hostname):
    list_of_nodes = commands.getoutput(
        'ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh; echo \\${%s[@]} "' % (
            cemod_hive_hosts.split(' ')[0], hostname)).split(' ')
    return list_of_nodes


def get_node_statistics(node, parent_node=None):
    global node_statistics
    time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:00')
    if parent_node:
        cpu_use = commands.getoutput(
            ''' ssh %s ssh %s top -bn1 | grep "Cpu(s)" | awk '{{print 100-$8}}' ''' % (parent_node, node))
        mem_use = commands.getoutput(
            "ssh %s ssh %s free | grep Mem | awk '{print $3/$2 * 100.0}' " % (parent_node, node))
        disk_use = commands.getoutput(
            "ssh %s ssh %s df --total -h | grep total | awk '{print $5}' " % (parent_node, node))
    else:
        cpu_use = commands.getoutput(''' ssh %s top -bn1 | grep "Cpu(s)" | awk '{print 100-$8}' ''' % node)
        mem_use = commands.getoutput("ssh %s free | grep Mem | awk '{print $3/$2 * 100.0}' " % node)
        disk_use = commands.getoutput("ssh %s df --total -h | grep total | awk '{print $5}' " % node)
    node_statistics = {'Time': time, 'Node': node, 'CPU_USE%': cpu_use, 'MEM_USE%': mem_use, 'DISK_USE%': disk_use}
    get_services_status(node)


def get_services_status(node):
    with open(os.path.join(PropertyFileUtil('ServiceStability', 'DirectorySection').getValueForKey(),
                           service_stability_file), 'r') as file:
        reader = csv.DictReader(file)
        services_status = {}
        boxi_count = {}
        count = 0
        for row in reader:
            if row['Host'] == node:
                count, boxi_count, services_status = get_service_status_on_node(boxi_count, count, row, services_status)
        for boxi_service in 'BoxiWebiServer', 'Boxi-Adaptive':
            if boxi_service in boxi_count.keys():
                services_status.update(get_boxiservice_status(boxi_count, services_status, boxi_service, count))
        get_required_data(node, services_status)


def get_service_status_on_node(boxi_count, count, row, services_status):
    service_name = row['Service']
    last_restart_time = row['Last_Restart_Time']
    if service_name.startswith('BoxiWebiServer') or service_name.startswith('Boxi-Webi Intelligent'):
        count += 1
        if last_restart_time != 'Service is down':
            boxi_count['BoxiWebiServer'] = boxi_count.get('BoxiWebiServer', 0) + 1
        elif "BoxiWebiServer" not in boxi_count:
            boxi_count['BoxiWebiServer'] = 0
    elif service_name.startswith('Boxi-Adaptive'):
        if last_restart_time != 'Service is down':
            boxi_count['Boxi-Adaptive'] = boxi_count.get('Boxi-Adaptive', 0) + 1
        elif 'Boxi-Adaptive' not in boxi_count:
            boxi_count['Boxi-Adaptive'] = 0
    else:
        if last_restart_time != 'Service is down':
            services_status[service_name] = OK
        elif last_restart_time == 'Service is down':
            services_status[service_name] = NOT_OK
    return count, boxi_count, services_status


def get_boxiservice_status(boxi_count, services_status, boxi_service, count):
    BOXI_ADAPTIVE_COUNT = 2
    if boxi_service == 'BoxiWebiServer':
        mid = count // 2
        if boxi_count[boxi_service] < mid:
            services_status[boxi_service] = NOT_OK
        elif boxi_count[boxi_service] in range(mid, count):
            services_status[boxi_service] = PARTIAL_OK
        else:
            services_status[boxi_service] = OK
    else:
        if boxi_count[boxi_service] == BOXI_ADAPTIVE_COUNT:
            services_status[boxi_service] = OK
        else:
            services_status[boxi_service] = NOT_OK
    return services_status


def get_required_data(node, services_status):
    HEADER_SECTION = 'HeaderSection'
    services_status.update(node_statistics)
    if node in application_nodes:
        write_to_csv(services_status, 'AppNodesStats', HEADER_SECTION)
    elif node in name_nodes:
        write_to_csv(services_status, 'NameNodeStats', HEADER_SECTION)
    else:
        write_to_csv(services_status, 'DataNodeStats', HEADER_SECTION)


def write_to_csv(services_status, header_variable, header_section):
    for service in PropertyFileUtil(header_variable, header_section).getValueForKey().split(','):
        if service not in services_status:
            services_status[service] = NOT_APPLICABLE
    CsvUtil().writeDictToCsv(header_variable, services_status)


def main():
    set_nodes()
    for node in application_nodes:
        get_node_statistics(node)
    for node in name_nodes + data_nodes:
        get_node_statistics(node, cemod_hive_hosts.split(' ')[0])
    curDate = datetime.date.today()
    for utiltype in ['AppNodesStats', 'NameNodeStats', 'DataNodeStats']:
        fileName = '{0}_{1}.csv'.format(utiltype, curDate)
        filePath = PropertyFileUtil(utiltype, 'DirectorySection').getValueForKey()
        push_to_postgres(os.path.join(filePath, fileName), utiltype)


def push_to_postgres(filepath, utiltype):
    jsonFileName = JsonUtils().convertCsvToJson(filepath)
    DBUtil().pushDataToPostgresDB(jsonFileName, utiltype)


if __name__ == '__main__':
    main()

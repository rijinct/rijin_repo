import datetime
import commands
import os
import re
import sys
import json
import csv
from collections import defaultdict

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
current_time = datetime.datetime.now().strftime(TIME_FORMAT)
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_time)
LOGGER = loggerUtil.__call__().get_logger(log_file)

exec(
    open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
    read()).replace("(", "(\"").replace(")", "\")")

OK, PARTIAL_OK, NOT_APPLICABLE, NOT_OK = ['0', '1', '2', '3']
app_nodes_up, name_nodes_up, data_nodes_up, kafka_nodes_up = [], [], [], []
app_nodes_down, name_nodes_down, data_nodes_down, kafka_nodes_down = \
    [], [], [], []


def fetch_nodes():
    global application_nodes
    global name_nodes
    global data_nodes
    application_nodes = cemod_application_hosts.split(' ')
    name_nodes = get_nodes('cemod_hdfs_master')
    data_nodes = get_nodes('cemod_hdfs_slaves')


def get_nodes(hostname):
    list_of_nodes = commands.getoutput(
        'ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/'
        'platform_definition.sh; echo \\${%s[@]} "' %
        (cemod_hive_hosts.split(' ')[0], hostname)).split(' ')
    return list_of_nodes


def write_node_statistics(time, node, parent_node=None):
    global node_statistics
    if parent_node:
        cpu_use, disk_use, mem_use = get_parent_node_statistics(
            node, parent_node)
    else:
        cpu_use, disk_use, mem_use = get_node_statistics(node)
    node_statistics = {
        'Time': time,
        'Node': node,
        'Reachability_Status': OK,
        'CPU_USE%': str(cpu_use),
        'MEM_USE%': str(mem_use),
        'DISK_USE%': str(disk_use)
    }
    check_error(node_statistics, cpu_use, mem_use, disk_use)
    fetch_and_write_services_status_on_node(node)


def get_node_statistics(node):
    cpu_use = commands.getoutput(''' ssh %s top -bn1 |
    grep "Cpu(s)" | awk '{print 100-$8}' ''' % node)
    mem_use = commands.getoutput("ssh %s free | "
                                 "grep Mem | awk '{print $3/$2 * 100.0}' " %
                                 node)
    disk_use = commands.getoutput("ssh %s df --total -h | grep total | "
                                  "awk '{print $5}' " % node)
    write_file_system_usage_of_node(node)
    return cpu_use, disk_use, mem_use


def get_parent_node_statistics(node, parent_node):
    cpu_use = commands.getoutput(''' ssh %s ssh %s top -bn1 | grep "Cpu(s)" |
        awk '{{print 100-$8}}' ''' % (parent_node, node))
    mem_use = commands.getoutput(
        "ssh %s ssh %s free | grep Mem | awk '{print $3/$2 * 100.0}' " %
        (parent_node, node))
    disk_use = commands.getoutput(
        "ssh %s ssh %s df --total -h | grep total | awk '{print $5}' " %
        (parent_node, node))
    write_file_system_usage_of_node(node, parent_node)
    return cpu_use, disk_use, mem_use


def check_error(node_statistics, cpu_use, mem_use, disk_use):
    for data_type in (cpu_use, mem_use, disk_use):
        pattern = '[a-zA-Z]'
        try:
            write_usage_statistics(data_type, node_statistics, pattern)
        except Exception as e:
            LOGGER.info(e)


def write_usage_statistics(data_type, node_statistics, pattern):
    if re.match(pattern, data_type):
        LOGGER.info(data_type)
        for key, value in node_statistics.items():
            if value == data_type:
                node_statistics[key] = 'NA'


def fetch_and_write_services_status_on_node(node):
    services_status = {}
    total_boxi_services = {}
    count = 0
    for data in services_status_data:
        if data['Host'] == node:
            count, total_boxi_services, services_status = \
                get_services_status_on_node(total_boxi_services,
                                            count, data, services_status)
    for boxi_service in 'BoxiWebiServer', 'Boxi-Adaptive':
        if boxi_service in total_boxi_services.keys():
            services_status.update(
                fetch_and_write_boxi_service_status(total_boxi_services,
                                                    services_status,
                                                    boxi_service, count))
    write_services_status_on_node(node, services_status)


def get_services_status_on_node(total_boxi_services, count, data,
                                services_status):
    service_name = data['Service']
    last_restart_time = data['Last_Restart_Time']
    BOXI_WEBI_SERVER = 'BoxiWebiServer'
    BOXI_ADAPTIVE_SERVICE = 'Boxi-Adaptive'
    count = get_count_of_boxi_webi_servers(BOXI_ADAPTIVE_SERVICE,
                                           BOXI_WEBI_SERVER, count,
                                           last_restart_time, service_name,
                                           services_status,
                                           total_boxi_services)
    return count, total_boxi_services, services_status


def get_count_of_boxi_webi_servers(BOXI_ADAPTIVE_SERVICE, BOXI_WEBI_SERVER,
                                   count, last_restart_time, service_name,
                                   services_status, total_boxi_services):
    if service_name.startswith(BOXI_WEBI_SERVER) or \
            service_name.startswith('Boxi-Webi Intelligent'):
        count += 1
        if last_restart_time != 'Service is down':
            total_boxi_services[BOXI_WEBI_SERVER] = \
                total_boxi_services.get(BOXI_WEBI_SERVER, 0) + 1
        elif "BoxiWebiServer" not in total_boxi_services:
            total_boxi_services[BOXI_WEBI_SERVER] = 0
    elif service_name.startswith(BOXI_ADAPTIVE_SERVICE):
        if last_restart_time != 'Service is down':
            total_boxi_services[BOXI_ADAPTIVE_SERVICE] = \
                total_boxi_services.get(BOXI_ADAPTIVE_SERVICE, 0) + 1
        elif BOXI_ADAPTIVE_SERVICE not in total_boxi_services:
            total_boxi_services[BOXI_ADAPTIVE_SERVICE] = 0
    else:
        if last_restart_time != 'Service is down':
            services_status[service_name] = OK
        elif last_restart_time == 'Service is down':
            services_status[service_name] = NOT_OK
    return count


def fetch_and_write_boxi_service_status(boxi_count, services_status,
                                        boxi_service, count):
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


def write_services_status_on_node(node, services_status):
    HEADER_SECTION = 'HeaderSection'
    services_status.update(node_statistics)
    if node in application_nodes:
        write_to_csv(services_status, 'AppNodesStats', HEADER_SECTION)
    elif node in name_nodes:
        write_to_csv(services_status, 'NameNodeStats', HEADER_SECTION)
    else:
        write_to_csv(services_status, 'DataNodeStats', HEADER_SECTION)


def write_to_csv(services_status, header_variable, header_section):
    for service in PropertyFileUtil(header_variable, header_section). \
            getValueForKey().split(','):
        if service == 'Postgres SDK' or service == 'Postgres HIVE':
            services_status = get_postgres_hosts_status(
                service, services_status)

        if service not in services_status:
            services_status[service] = NOT_APPLICABLE
    LOGGER.info('writing %s Reachability_Status and statistics info to csv' %
                services_status['Node'])
    CsvUtil().writeDictToCsv(header_variable, services_status)
    data_to_push_to_db[header_variable].append(json.dumps(services_status))


def get_postgres_hosts_status(service, services_status):
    POSTGRES_HOSTS = commands.getoutput(
        'ssh %s "source /opt/nsn/ngdb/ifw/lib/platform/utils/'
        'platform_definition.sh; echo \\${%s[@]} "' % (
            cemod_hive_hosts.split(' ')[0], 'cemod_postgres_hosts')). \
                         split(' ') + cemod_postgres_hosts.split()
    for data in services_status_data:
        if data['Service'] == service:
            write_postgres_service_status(POSTGRES_HOSTS, data, service,
                                          services_status)
    return services_status


def write_postgres_service_status(POSTGRES_HOSTS, data, service,
                                  services_status):
    if data['Last_Restart_Time'] != 'Service is down':
        if services_status['Node'] == commands.getoutput(
                'ssh %s "hostname -f"' % data['Host']):
            services_status[service] = OK
        else:
            services_status[service] = NOT_APPLICABLE
    elif services_status['Node'] in POSTGRES_HOSTS:
        services_status[service] = NOT_OK
    else:
        services_status[service] = NOT_APPLICABLE


def get_parent_node_status(parent_node):
    if int(get_ping_status(parent_node)) == 0:
        return True


def get_ping_status(node):
    command_str = 'ping \\-c 1 %s | grep "packet loss" | ' \
                  'gawk -F" " \'{print $6}\' | gawk -F"%s" \'{print $1}\' ' % \
                  (node, "%")
    return commands.getoutput(command_str)


def write_data_and_name_nodes_status(time, node, parent_node):
    if get_parent_node_status(parent_node):
        command_str = 'ssh %s ping \\-c 1 %s | grep "packet loss" | ' \
                      'gawk -F" " \'{print $6}\' | ' \
                      'gawk -F"%s" \'{print $1}\' ' % (parent_node, node, "%")
        if int(commands.getoutput(command_str)) == 0:
            add_data_name_nodes_count_and_status(node)
            write_node_statistics(time, node, parent_node)
        else:
            add_data_name_nodes_count_and_status(node, 'DOWN')
            LOGGER.info('%s on %s not reachable' % node, parent_node)
            write_node_down_to_csv(time, node)
    else:
        LOGGER.info('parent node %s not reachable' % parent_node)
        write_node_down_to_csv(time, node, 'Parent Host Down')


def add_data_name_nodes_count_and_status(node, status_down=None):
    if node in name_nodes:
        if status_down:
            name_nodes_down.append(node)
        else:
            name_nodes_up.append(node)
    else:
        if status_down:
            data_nodes_down.append(node)
        else:
            data_nodes_up.append(node)


def write_app_node_status(time, node):
    if int(get_ping_status(node)) == 0:
        app_nodes_up.append(node)
        if node in cemod_kafka_app_hosts.split(' '):
            kafka_nodes_up.append(node)
        write_node_statistics(time, node)
    else:
        app_nodes_down.append(node)
        if node in cemod_kafka_app_hosts.split(' '):
            kafka_nodes_down.append(node)
        LOGGER.info('%s not reachable' % node)
        write_node_down_to_csv(time, node)


def write_node_down_to_csv(time, node, parent_node_status=None):
    HEADER_SECTION = 'HeaderSection'
    output_to_write = {
        'Time': time,
        'Node': node,
        'Reachability_Status': NOT_OK
    }
    if parent_node_status:
        output_to_write['Reachability_Status'] = parent_node_status
    if node in application_nodes:
        write_service_down_to_csv(output_to_write, 'AppNodesStats',
                                  HEADER_SECTION)
    elif node in name_nodes:
        write_service_down_to_csv(output_to_write, 'NameNodeStats',
                                  HEADER_SECTION)
    else:
        write_service_down_to_csv(output_to_write, 'DataNodeStats',
                                  HEADER_SECTION)


def write_service_down_to_csv(output_to_write, header_variable,
                              header_section):
    for data_type in PropertyFileUtil(header_variable, header_section). \
            getValueForKey().split(','):
        if data_type not in output_to_write:
            output_to_write[data_type] = 'NA'
    CsvUtil().writeDictToCsv(header_variable, output_to_write)
    data_to_push_to_db[header_variable].append(json.dumps(output_to_write))


def write_nodes_count_and_status():
    for nodes_type in ['app_nodes', app_nodes_up, app_nodes_down], \
                      ['name_nodes', name_nodes_up, name_nodes_down], \
                      ['data_nodes', data_nodes_up, data_nodes_down], \
                      ['kafka_nodes', kafka_nodes_up, kafka_nodes_down]:
        write_nodes_count_and_status_to_csv(nodes_type[0], nodes_type[1],
                                            nodes_type[2])


def write_nodes_count_and_status_to_csv(nodes_type, nodes_up, nodes_down):
    data_to_write = {
        'Time': current_time,
        'Nodes_Type': nodes_type,
        'Total_Nodes': str(len(nodes_up + nodes_down)),
        'Nodes_Up': str(len(nodes_up)),
        'Nodes_down': str(len(nodes_down))
    }
    if nodes_down:
        data_to_write['Remarks'] = ', '.join(nodes_down) + 'are Down'
    else:
        data_to_write['Remarks'] = 'NA'
    CsvUtil().writeDictToCsv('Nodes_Status', data_to_write)
    data_to_push_to_db['Nodes_Status'].append(json.dumps(data_to_write))


def write_file_system_usage_of_node(node, parent_node=None):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if parent_node:
        file_system_usage_data = commands.getoutput(
            "ssh {0} ssh {1} df --total".format(parent_node,
                                                node)).strip().splitlines()[-1]
    else:
        file_system_usage_data = commands.getoutput(
            "ssh {} df --total".format(node)).strip().splitlines()[-1]
        if node in application_nodes:
            write_detailed_file_sys_usage_of_application_nodes(node)
    data_to_write = get_file_sys_usage_data_to_write(current_time,
                                                     file_system_usage_data,
                                                     node)
    CsvUtil().writeDictToCsv('AllNodesFileSystemUsage', data_to_write)
    data_to_push_to_db['AllNodesFileSystemUsage'].append(
        json.dumps(data_to_write))


def write_detailed_file_sys_usage_of_application_nodes(node):
    UTIL_NAME = 'ApplicationNodesFileSysUse'
    file_name = node.split(
        '.')[0] + '_file_sys_use_%s.csv' % (datetime.date.today())
    out_put_directory = PropertyFileUtil(UTIL_NAME,
                                         'DirectorySection').getValueForKey()
    file_exist = os.path.exists(os.path.join(out_put_directory, file_name))
    FIELD_NAMES = [
        'Time', 'File_System', 'Mounted_On', 'Disk_Size(GB)', 'Disk_Use(GB)',
        'Disk_Available(GB)', 'Disk_Use%'
    ]
    list_of_usage_data = get_application_file_sys_data(node)
    with open(os.path.join(out_put_directory, file_name), 'a') as file_object:
        writer = csv.DictWriter(file_object, fieldnames=FIELD_NAMES)
        if not file_exist:
            writer.writeheader()
        for data_to_write in list_of_usage_data:
            writer.writerow(data_to_write)


def get_application_file_sys_data(node):
    list_of_file_sys_data = []
    list_of_data_to_push = []
    current_time = datetime.datetime.now().strftime(TIME_FORMAT)
    file_sys_data = commands.getoutput(
        'ssh {} df --total'.format(node)).strip().splitlines()
    for data in file_sys_data[1:]:
        file_system, size, use, available, use_per, mounted_on = data.split()
        data_to_write = {
            'Time': current_time,
            'File_System': file_system,
            'Disk_Size(GB)': str(get_usage_in_gb(size)),
            'Disk_Use(GB)': str(get_usage_in_gb(use)),
            'Disk_Available(GB)': str(get_usage_in_gb(available)),
            'Disk_Use%': use_per.split('%')[0],
            'Mounted_On': mounted_on
        }
        list_of_file_sys_data.append(data_to_write)
        list_of_data_to_push.append(json.dumps(data_to_write))
    push_application_file_sys_usage(node.split('.')[0], list_of_data_to_push)
    return list_of_file_sys_data


def push_application_file_sys_usage(hm_type, data_to_postgres):
    json_data = '[%s]' % (','.join(data_to_postgres))
    DBUtil().jsonPushToPostgresDB(json_data, hm_type)


def get_file_sys_usage_data_to_write(current_time, file_system_usage_data,
                                     node):
    total_disk_size = get_usage_in_gb(file_system_usage_data.split()[1])
    disk_used = get_usage_in_gb(file_system_usage_data.split()[2])
    disk_available = get_usage_in_gb(file_system_usage_data.split()[3])
    percentage_disk_used = file_system_usage_data.split()[4].split('%')[0]
    return {
        'Time': current_time,
        'Node': node,
        'Disk_Size(GB)': str(total_disk_size),
        'Disk_Used(GB)': str(disk_used),
        'Disk_Available(GB)': str(disk_available),
        'Percentage_Disk_Used': percentage_disk_used
    }


def get_usage_in_gb(usage):
    return round(float(usage) / (1024**2), 3)


def push_data_to_postgres():
    for hm_type in [
            'AppNodesStats', 'NameNodeStats', 'DataNodeStats', 'Nodes_Status',
            'AllNodesFileSystemUsage'
    ]:
        json_data = '[%s]' % (','.join(data_to_push_to_db[hm_type]))
        DBUtil().jsonPushToPostgresDB(json_data, hm_type)


def write_nodes_status(time):
    for node in application_nodes:
        write_app_node_status(time, node)
    for node in name_nodes + data_nodes:
        write_data_and_name_nodes_status(time, node,
                                         cemod_hive_hosts.split(' ')[0])


def main(services_data):
    global data_to_push_to_db, services_status_data
    services_status_data = []
    for data in services_data:
        services_status_data.append(json.loads(data))
    data_to_push_to_db = defaultdict(list)
    fetch_nodes()
    time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:00')
    write_nodes_status(time)
    write_nodes_count_and_status()
    push_data_to_postgres()

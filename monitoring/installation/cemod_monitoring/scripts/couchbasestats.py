#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script gets buckets information for all entities
# and alerts if anything reaches configured threshold.
# Date : 03-11-2019
#############################################################################
#############################################################################
import datetime
import json
import os
import commands
import sys
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from writeCsvUtils import CsvWriter
from loggerUtil import loggerUtil

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")


def main():
    declare_global_variables()
    declare_global_file_names()
    collect_couchbase_stats()


def declare_global_variables():
    global total_ram_used, total_ram_quota, logger, date
    date = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    total_ram_used = 0
    total_ram_quota = 0
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(os.path.basename(sys.argv[0]).replace('.py', ''), date))


def declare_global_file_names():
    global output_couchbase_stats_file, output_couchbase_cpu_summary_file, output_couchbase_memory_summary_file
    output_couchbase_stats_file = 'couchBaseStats_{0}.csv'.format(date)
    output_couchbase_cpu_summary_file = 'couchbaseCpuSummary_{0}.csv'.format(date)
    output_couchbase_memory_summary_file = 'couchbaseMemorySummary_{0}.csv'.format(date)


def collect_couchbase_stats():
    rest_response = get_bucket_details()
    parse_json_response(json.loads(rest_response))
    parse_cpu_summary(json.loads(rest_response))
    write_ram_summary_csv()


def get_bucket_details():
    decrypted_password = commands.getoutput(
        """ perl /opt/nsn/ngdb/ifw/lib/common/decryption_util.pl {0} """.format(cemod_couchbase_admin_user_password))
    rest_response = commands.getoutput(
        """ ssh {0} 'curl -u {1}:{2} -sb -H "Accept: application/json" http://127.0.0.1:8091/pools/default/buckets/ ' """.format(
            cemod_couchbase_hosts.split(" ")[0], cemod_couchbase_admin_user_name, decrypted_password))
    return rest_response


def parse_json_response(rest_response_list):
    for item_json in rest_response_list:
        parse_response(item_json, final_json={})
    insert_data_to_postgres(output_couchbase_stats_file, 'couchBaseStats')


def parse_response(item_json, final_json):
    final_json['name'] = item_json['name']
    final_json = parse_basic_stats(final_json, item_json)
    final_json = parse_quota_stats(final_json, item_json)
    write_couchbase_stats_to_csv(final_json)


def parse_basic_stats(final_json, item_json):
    for key in item_json['basicStats'].keys():
        final_json = get_basic_stats(final_json, item_json['basicStats'], key)
    return final_json


def get_basic_stats(final_json, basic_stats_json, key):
    if 'diskUsed' in key or 'dataUsed' in key or 'memUsed' in key:
        final_json[key] = round((basic_stats_json[key] / 1024 / 1024), 2)
    elif 'itemCount' in key:
        final_json[key] = basic_stats_json[key]
    return final_json


def parse_quota_stats(final_json, item_json):
    for key in item_json['quota'].keys():
        parse_quota_stat(final_json, item_json['quota'], key)
    return final_json


def parse_quota_stat(final_json, quota_json, key):
    if 'ram' in key:
        final_json['ramQuota'] = round((quota_json[key] / 1024 / 1024), 2)


def write_couchbase_stats_to_csv(final_json):
    global total_ram_used, total_ram_quota
    total_ram_used, total_ram_quota = calculate_total_ram(total_ram_used, total_ram_quota, final_json)
    ram_perc = round(((final_json['memUsed'] / final_json['ramQuota']) * 100), 2)
    content = wrap_content_for_couchbase_stats(final_json, ram_perc)
    CsvWriter("couchBaseStats", output_couchbase_stats_file, content)


def calculate_total_ram(total_ram_used, total_ram_quota, final_json):
    total_ram_used += final_json['memUsed']
    total_ram_quota += final_json['ramQuota']
    return total_ram_used, total_ram_quota


def wrap_content_for_couchbase_stats(final_json, ram_perc):
    header = [datetime.strptime(date, '%Y-%m-%d-%H-%M-%S').strftime('%Y-%m-%d %H:%M:%S'), final_json['name'],
              final_json['itemCount'], final_json['memUsed'], final_json['ramQuota'],
              final_json['dataUsed'], final_json['diskUsed'], ram_perc]

    content = ','.join(map(str, header))
    return content


def parse_cpu_summary(rest_response_list):
    unique_nodes = []
    for item in rest_response_list:
        unique_nodes = get_nodes_cpu_summary(item, unique_nodes)
    insert_data_to_postgres(output_couchbase_cpu_summary_file, 'couchBaseCpuStats')


def get_nodes_cpu_summary(item, unique_nodes):
    for node in item['nodes']:
        node_name = extract_host_name(node)
        if node_name not in unique_nodes: write_cpu_summary_for_unique_node(node, node_name, unique_nodes)
    return unique_nodes


def extract_host_name(node):
    node_name = node['hostname'][:node['hostname'].index('.')]
    return node_name


def write_cpu_summary_for_unique_node(node, node_name, unique_nodes):
    node_json = get_cpu_summary_from_system_stats(node, node_name)
    unique_nodes.append(node_name)
    write_cpu_summary_csv(node_json)


def get_cpu_summary_from_system_stats(node, node_name):
    node_json = {'node_name': node_name}
    system_stats = ['swap_total', 'swap_used', 'mem_total', 'mem_free']
    for stat in system_stats:
        node_json[stat] = round((node['systemStats'][stat] / 1024 / 1024), 2)
    node_json['cpu_utilization_rate'] = round(node['systemStats']['cpu_utilization_rate'], 2)
    return node_json


def write_cpu_summary_csv(node_json):
    header = [datetime.strptime(date, '%Y-%m-%d-%H-%M-%S').strftime('%Y-%m-%d %H:%M:%S'), node_json['node_name'],
              node_json['cpu_utilization_rate'], node_json['swap_total'], node_json['swap_used'],
              node_json['mem_total'], node_json['mem_free']]
    content = ','.join(map(str, header))
    CsvWriter("couchBaseCpuSummary", output_couchbase_cpu_summary_file, content)


def insert_data_to_postgres(output_file_name, hm_type):
    json_file_name = JsonUtils().convertCsvToJson(
        PropertyFileUtil("couchBaseStats", "DirectorySection").getValueForKey() + output_file_name)
    DBUtil().pushDataToPostgresDB(json_file_name, hm_type)


def write_ram_summary_csv():
    global total_ram_used, total_ram_quota
    ram_perc = round(((float(total_ram_used) / float(total_ram_quota)) * 100), 2)
    content = wrap_content_for_ram_summary(ram_perc, total_ram_quota, total_ram_used)
    CsvWriter("couchBaseRamSummary", output_couchbase_memory_summary_file, content)
    insert_data_to_postgres(output_couchbase_memory_summary_file, 'couchBaseRamStats')


def wrap_content_for_ram_summary(ram_perc, total_ram_quota, total_ram_used):
    header = [datetime.strptime(date, '%Y-%m-%d-%H-%M-%S').strftime('%Y-%m-%d %H:%M:%S'), total_ram_used,
              total_ram_quota, ram_perc]
    content = ','.join(map(str, header))
    return content


if __name__ == '__main__':
    main()

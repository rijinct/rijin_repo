import sys
import os
import json
from datetime import datetime
from lxml.html import fromstring
from xml.dom.minidom import parse
from commands import getoutput
import re
from collections import OrderedDict

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from loggerUtil import loggerUtil
from dbUtils import DBUtil
from csvUtil import CsvUtil


def get_logger():
    file = os.path.basename(sys.argv[0]).replace('.py', '')
    time = datetime.now().strftime('%Y-%m-%d')
    return loggerUtil.__call__().get_logger('%s_%s' % (file, time))


def get_html_response_from_address():
    address = get_url()
    logger.info('Getting repsonse from %s' % address)
    response = getoutput('curl -k %s/cluster/scheduler?openQueues=default' %
                         address)
    return fromstring(response)


def get_url():
    logger.info("Parsing yarn-site.xml and core-conf.xml")
    global yarn_conf
    yarn_conf = parse_conf_from_xml("/etc/hadoop/conf/yarn-site.xml")
    core_conf = parse_conf_from_xml("/etc/hadoop/conf/core-site.xml")
    rm_id = get_active_rm_id(yarn_conf['yarn.resourcemanager.ha.rm-ids'])
    ssl_enabled = json.loads(core_conf['hadoop.ssl.enabled'])
    if ssl_enabled:
        address = 'https://%s' % yarn_conf[
            'yarn.resourcemanager.webapp.https.address.%s' % rm_id]
    else:
        address = 'http://%s' % yarn_conf[
            'yarn.resourcemanager.webapp.address.%s' % rm_id]
    return address


def parse_conf_from_xml(xml_file):
    conf = {}
    xml = parse(xml_file)
    for tag in xml.getElementsByTagName('property'):
        conf[tag.getElementsByTagName('name')[0].firstChild.
             data] = tag.getElementsByTagName('value')[0].firstChild.data
    return conf


def get_active_rm_id(rm_ids):
    for rm_id in rm_ids.split(','):
        cmd = 'su - ngdb -c "yarn rmadmin -getServiceState %s "' % rm_id
        if getoutput(cmd) == 'active':
            logger.info('Active rm: %s' % rm_id)
            return rm_id


def get_queue_utilisations(html):
    queue_usage = {}
    logger.info('Parsing html response')
    queue_name = html.find_class('q')
    queue_util = html.find_class('qstats')
    for name, util in zip(queue_name, queue_util):
        queue_usage[parse_queue_name(name)] = parse_queue_util(util)
    return [get_updated_queue_usage(queue_usage)]


def parse_queue_name(name):
    return name.text_content().split('.')[-1].capitalize()


def parse_queue_util(util):
    return float(util.text_content().split('%')[0])


def get_updated_queue_usage(queue_usage):
    queue_usage['DATE'] = datetime.now().strftime('%Y-%d-%m %H:%M:00')
    for key in 'Root', 'Nokia':
        queue_usage.pop(key)
    return queue_usage

def write_vcores_usage_data(queue_usage):
    cmd_str= 'hdfs dfsadmin -report'
    live_node = 0
    for data in getoutput(cmd_str).splitlines():
        if data.startswith('Live datanodes'):
            live_node = int(re.search(r"(\d+)", data).group(1))
    vcores = int(yarn_conf['yarn.scheduler.maximum-allocation-vcores'])
    total_cores = vcores * live_node
    write_to_csv_and_db(queue_usage, total_cores)

def write_to_csv_and_db(queue_usage, total_cores):
    queue_usage.pop('DATE')
    data_to_push_to_db  = []
    typeOfUtil = 'yarnQueue'
    time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    csv_use_per_row= OrderedDict({"Date": time})
    csv_vcores_row = OrderedDict({"Date": time})
    for key in queue_usage.keys():
        cal = round((queue_usage[key]/100) * total_cores,2)
        data = {"Date": time,"queue": key, "use(%)": str(queue_usage[key]), "vcores": str(cal)}
        data_to_push_to_db.append(json.dumps(data))
        csv_use_per_row[key] = str(queue_usage[key])
        csv_vcores_row[key] = str(cal)
    
    time = datetime.now().strftime('%Y-%m-%d')
    fileName = '{}_{}_{}.csv'.format(typeOfUtil,'USED_PER', time)
    CsvUtil().writeDictToCsv(typeOfUtil,csv_use_per_row, fileName)
    fileName = '{}_{}_{}.csv'.format(typeOfUtil,'USED_VCORE', time)
    CsvUtil().writeDictToCsv(typeOfUtil,csv_vcores_row, fileName)
    push_data_to_postgres(data_to_push_to_db, typeOfUtil)

def push_data_to_postgres(data_to_push, hm_type):
    json_data = '[%s]' % (','.join(data_to_push))
    DBUtil().jsonPushToPostgresDB(json_data, hm_type)


def main():
    global logger
    logger = get_logger()
    html = get_html_response_from_address()
    queue_usage = get_queue_utilisations(html)
    write_vcores_usage_data(queue_usage[0])
    
if __name__ == '__main__':
    main()

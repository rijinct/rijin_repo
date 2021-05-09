import json
import os
from subprocess import getoutput
import sys
from xml.dom.minidom import parse

import requests, time, datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from dbUtils import DBUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def get_html_response_from_address(address, pattern, vcore=None):
    logger.debug('Getting repsonse from %s' % address)
    if vcore:
        return getoutput('sudo wget --no-check-certificate -O- -q %s/cluster/scheduler?openQueues=default | grep "/cluster/nodes"' %address)
    else:
        return getoutput('''sudo wget --no-check-certificate -O- -q {0}/cluster/scheduler?openQueues=default | grep "Steady Fair Share" |grep "{1}" '''.format(address,pattern))


def get_url(name=None):
    global vcores
    response_used = ""
    response_vcore = ""
    logger.debug('Parsing yarn-site.xml and core-conf.xml')
    yarn_conf = parse_conf_from_xml("/etc/hive/conf/yarn-site.xml")
    core_conf = parse_conf_from_xml("/etc/hive/conf/core-site.xml")
    vcores = (yarn_conf['yarn.scheduler.maximum-allocation-vcores'])
    rm_ids = (yarn_conf['yarn.resourcemanager.ha.rm-ids'])
    ssl_enabled = json.loads(core_conf['hadoop.ssl.enabled'])
    queue_pattern = ["root.nokia.ca4ci","root.default"]
    for rm_id in rm_ids.split(','):
        if ssl_enabled:
            address = 'https://%s' % yarn_conf['yarn.resourcemanager.webapp.https.address.%s' % rm_id]
        else:
            address = 'http://%s' % yarn_conf['yarn.resourcemanager.webapp.address.%s' % rm_id]
        if name:
            complete_response = get_vcore_response(address, name)
            if "error" not in complete_response or "HTTP/1.1 307" not in complete_response:
                return complete_response
        vcore = get_vcore_response(address)
        response_vcore = response_vcore + str(vcore) + "\n"
        for pattern in queue_pattern:
            response = get_html_response_from_address(address,pattern)
            if "error" in response or "HTTP/1.1 307" in response:
                logger.debug('The Resource Manager is not active')
                continue
            else:
                response_used = response_used + response + "\n"
    return response_used,response_vcore

def get_vcore_response(address, name=None):
    pattern = "vcore"
    if name:
        return getoutput('''sudo wget --no-check-certificate -O- -q {0}/cluster/scheduler?openQueues=default '''.format(address))
    vcore = get_html_response_from_address(address,pattern,"Yes")
    if "error" not in vcore or "HTTP/1.1 307" not in vcore:
        return vcore



def parse_conf_from_xml(xml_file):
    conf = {}
    xml = parse(xml_file)
    for tag in xml.getElementsByTagName('property'):
        conf[tag.getElementsByTagName('name')[0].firstChild.
             data] = tag.getElementsByTagName('value')[0].firstChild.data
    return conf


def get_current_data_nodes(vcore_html):
    html_list = vcore_html.split("\n")
    for item in html_list[2:]:
        if '''<a href="/cluster/nodes">''' in item:
            cluster_nodes = int(item.split(">")[1].split("<")[0])
        if "decommissioning" in item:
            decommisioning_nodes = int(item.split(">")[1].split("<")[0])
        if "decommissioned" in item:
            decommisioned_nodes = int(item.split(">")[1].split("<")[0])
        if "lost" in item:
            lost_nodes = int(item.split(">")[1].split("<")[0])
    current_data_nodes = cluster_nodes - (decommisioning_nodes+decommisioned_nodes+lost_nodes)
    return current_data_nodes


def get_queue_utilisations(html, vcore_resp):
    queue_usage = {}
    finalJsonList = []
    current_time = time.time()
    cur_date_time = datetime.datetime.utcfromtimestamp(current_time).strftime("%Y-%m-%d %H:%M:00")
    curDateTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00")
    cur_date_time_pod = int(time.mktime(time.strptime(str(curDateTime), '%Y-%m-%d %H:%M:00')))
    data_nodes = get_current_data_nodes(vcore_resp)
    logger.debug("data_nodes are %s",data_nodes)
    for item in html.rstrip("\n").split("\n"):
        xml=item.split("class=\"q\"")[1]
        queuename=xml.split("</span")[0].strip(">")
        used=xml.split("\">")[-1].split(" ")[0].strip('\'')
        queue_usage['Date'] = cur_date_time
        queue_usage['DateInLocalTz'] = str(cur_date_time_pod)
        queue_usage['queuename'] = queuename
        queue_usage['Used'] = used.strip("%")
        queue_usage['Vcore'] = round((float(used.strip("%")) * int(vcores)*data_nodes)/100, 2)
        jsonObj = json.dumps(queue_usage)
        finalJsonList.append(jsonObj.strip())

    finalList = "[%s]" % (','.join(finalJsonList))
    logger.debug("Final List of Json's are %s",finalList)
    DBUtil().jsonPushToMariaDB(finalList,"queueUsage")

def get_vcore_output(name):
    return get_url(name)

def main():
    html,vcore_resp = get_url()
    get_queue_utilisations(html,vcore_resp)
    logger.info("Queue Utilization has been inserted to db")

if __name__ == "__main__":
        main()


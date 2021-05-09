'''
Created on 17-Apr-2020

@author: aman
'''
import json
from datetime import datetime
from lxml.html import fromstring
from xml.dom.minidom import parse
from subprocess import getoutput
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def get_html_response_from_address():
    address = get_url()
    logger.info('Getting repsonse from %s' % address)
    response = getoutput('curl -k %s/cluster/scheduler?openQueues=default' %
                         address)
    return fromstring(response)


def get_url():
    logger.info("Parsing yarn-site.xml and core-conf.xml")
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


def main():
    html = get_html_response_from_address()
    return get_queue_utilisations(html)


if __name__ == '__main__':
    main()

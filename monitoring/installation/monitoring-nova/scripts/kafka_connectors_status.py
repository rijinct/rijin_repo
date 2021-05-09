import os
import multiprocessing
import sys
import json
from subprocess import getstatusoutput
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from enum_util import CheckConnectors
from csvUtil import CsvUtil
from dbUtils import DBUtil
from multiprocessing_util import multiprocess
from monitoring_utils import XmlParserUtils
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def main():
    declare_globals()
    logger.info("Collecting Info of Topologies")
    topologies = get_list_of_topologies()
    get_connector_and_task_status(topologies, results)
    write_status_to_csv(results)
    push_to_maria_db(results)
    logger.info("Writing to csv & inserting to maria db completed")


def declare_globals():
    global date, identifier, namespace, results
    results = multiprocessing.Manager().list()
    date = datetime.now().strftime('%Y:%m:%d %H:%M:%S')
    identifier = 'ConnectorStatus'
    namespace = os.environ['RELEASE_NAMESPACE']


def get_list_of_topologies():
    topology_list = []
    logger.debug('Getting all configured topologies.')
    topologies = XmlParserUtils.get_topologies_from_xml()
    for topology in topologies.keys():
        topology_list.extend([(topology, 'SOURCE'),
                         (topology, 'SINK')])
    return topology_list


@multiprocess(timeout=10)
def get_connector_and_task_status(topology_and_connector):
    topology, connector = topology_and_connector
    logger.debug("Getting status for : %s", topology)
    results.append(get_topology_info(topology, connector))


def get_topology_info(topology, connector):
    status, output = get_topology_status(topology, connector)
    if not status:
        response = json.loads(output)
    else:
        logger.info('No response from: %s_%s' % (topology, connector))
        response = {
            "connector": {
                "state": "NOT_RUNNING",
                "worker_id": ""
            },
            "tasks": []
        }
    return parse_response(topology, connector, response)


def get_topology_status(topology, connector):
    topology_name = topology.replace('_', '-')
    cmd = '''wget etltopology-{0}-{2}.{3}.svc.cluster.local:8083/connectors/"{1}_1_{2}_CONN"/status -O- -q --timeout=5'''.format(# noqa: 501
        topology_name, topology, connector, namespace)
    return getstatusoutput(cmd)


def parse_response(topology, connector, response):
    status = {
        'Date': date,
        'Name': '%s_%s' % (topology, connector),
        'ConnectorNode': response['connector']['worker_id'],
        'TaskNode': get_task_node(response),
        'ConnectorStatus': str(get_connector_status(response)),
        'TotalTasks': str(len(response['tasks'])),
        'RunningTasks': str(get_running_tasks_count(response))
    }
    status['TaskStatus'] = str(get_task_status(response))
    return status


def get_task_node(response):
    if response['tasks']:
        return response['tasks'][0].get('worker_id', '')
    else:
        return ''


def get_connector_status(response):
    if response['connector']['state'] == 'RUNNING':
        return CheckConnectors.STATUS_OK.value
    else:
        return CheckConnectors.STATUS_NOK.value


def get_running_tasks_count(response):
    return len(
        [task for task in response['tasks'] if task['state'] == 'RUNNING'])


def get_task_status(response):
    total_tasks, running_tasks = len(
        response['tasks']), get_running_tasks_count(response)
    if not running_tasks:
        return CheckConnectors.STATUS_NOK.value
    elif total_tasks == running_tasks:
        return CheckConnectors.STATUS_OK.value
    else:
        return CheckConnectors.STATUS_PARTIAL_OK.value


def write_status_to_csv(topology_details):
    logger.debug('Writing data to csv')
    for topology_status in topology_details:
        CsvUtil().writeDictToCsv(identifier, topology_status)


def push_to_maria_db(topologies_details):
    logger.debug('Inserting data to db')
    topologies_details_json = str(topologies_details).replace("'", '"')
    DBUtil().jsonPushToMariaDB(topologies_details_json, identifier)


if __name__ == '__main__':
    main()

############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring team
# Version: 1.0
# Date:26-11-2017
# Purpose :  This script checks the memory usage of all disks present in all
# the node of the Portal Cluster
#############################################################################
import sys
import os
import json
from commands import getoutput
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from loggerUtil import loggerUtil

exec(open('/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh').read()).replace('(', '("').replace(')',
                                                                                                                  '")')


def main():
    create_logger()
    write_portal_connections()


def create_logger():
    global logger
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_file_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(log_file_name, log_file_time))


def write_portal_connections():
    if len(cemod_portal_hosts.split(' ')) >= 1:
        result_json = process_portal_connections(json_list=[])
        insert_into_postgres(result_json, 'portalConnections')
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


def process_portal_connections(json_list):
    for portal_host in cemod_portal_hosts.split(" "):
        portal_dict = get_required_dict(portal_host)
        logger.info('For host {0} : {1}'.format(portal_host, portal_dict))
        json_list.append(json.dumps(portal_dict))
    return '[%s]' % (','.join(json_list))


def get_required_dict(portal_host):
    portal_dict = dict(Date=datetime.now().strftime('%Y-%m-%d %H:00:00'), Host=portal_host,
                       LoadBalancer=get_portal_connections(portal_host, '443'),
                       PortalService=get_portal_connections(portal_host, '8443'),
                       PortalDb=get_portal_connections(portal_host, '5432'))
    CsvUtil().writeDictToCsv('portalConnections', portal_dict)
    return portal_dict


def get_portal_connections(portal_host, pattern):
    return getoutput('''ssh {0} "ss -nap | grep ':{1}' | wc -l" '''.format(portal_host, pattern))


def insert_into_postgres(result_json, hm_type):
    DBUtil().jsonPushToPostgresDB(result_json, hm_type)


if __name__ == '__main__':
    main()

#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring team
# Version: 0.1
# Purpose: This script writes portal freeMemory and freeBufferCache to db.
# input from user
# Date : 29-10-2019
#############################################################################
import sys
import os
import json
from datetime import datetime
from commands import getoutput

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from loggerUtil import loggerUtil

exec(open('/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh').read()).replace('(', '("').replace(')',
                                                                                                                  '")')


def main():
    create_logger()
    write_portal_memory()


def create_logger():
    global logger
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_file_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(log_file_name, log_file_time))


def write_portal_memory():
    if len(cemod_portal_hosts.split(' ')) >= 1:
        result_json = calculate_portal_memory(json_list=[])
        insert_into_postgres(result_json, 'memoryUsage')
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


def calculate_portal_memory(json_list):
    for portal_host in cemod_portal_hosts.split(' '):
        portal_dict = calculate_memory(portal_host)
        logger.info('For host {0} : {1}'.format(portal_host, portal_dict))
        json_list.append(json.dumps(portal_dict))
    return '[%s]' % (','.join(json_list))


def calculate_memory(portal_host):
    portal_dict = dict(Date=datetime.now().strftime('%Y-%m-%d %H:00:00'), Host=portal_host)
    portal_dict['TotalMemory'] = getoutput(
        '''ssh %s "free -g | gawk -F' ' \'{print \\$2}\' | grep -v shared | head -2 | tail -1" ''' % portal_host)
    portal_dict['FreeMemory'] = getoutput(
        '''ssh %s "free -g | gawk -F' ' \'{print \\$4}\' | grep -v shared | head -1" ''' % portal_host)
    portal_dict['FreeBufferCache'] = getoutput(
        '''ssh %s "free -g | gawk -F' ' '{print \\$4}' | grep -v shared | head -2 | tail -1" ''' % portal_host)
    CsvUtil().writeDictToCsv('portalMemory', portal_dict)
    return portal_dict


def insert_into_postgres(result_json, hm_type):
    DBUtil().jsonPushToPostgresDB(result_json, hm_type)


if __name__ == '__main__':
    main()

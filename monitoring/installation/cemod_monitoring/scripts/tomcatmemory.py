############################################################################
#                               MONITORING
#############################################################################
#############################################################################
# (c)2016 Nokia
# Author: Monitoring team
# Version: 1.0
# Date:26-11-2017
# Purpose :  This script calcuates the portal and webservice tomcat memory.
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

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")


def main():
    create_logger()
    write_portal_tomcat_memory(json_list=[])
    write_webservice_tomcat_memory()


def create_logger():
    global logger
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_file_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(log_file_name, log_file_time))


def write_portal_tomcat_memory(json_list):
    for portal_host in cemod_portal_hosts.split(" "):
        json_list.append(json.dumps(write_portal_host_tomcat_memory(portal_host)))
    result_json = "[%s]" % (','.join(json_list))
    insert_into_postgres(result_json, 'portalTomcat')


def write_portal_host_tomcat_memory(portal_host):
    portal_pid = getoutput(
        '''ssh {0} " ps -eaf | grep ^tomcat | gawk -F' ' '{{print \\$2}}' " '''.format(portal_host))
    tomcat_dict = get_portal_tomcat_dict(portal_host, portal_pid)
    return tomcat_dict


def get_portal_tomcat_dict(portal_host, portal_pid):
    tomcat_dict = {'Date': datetime.now().strftime('%Y-%m-%d %H:00:00'),
                   'Host': portal_host, 'Memory': calculate_memory(portal_host, portal_pid, memory={})}
    logger.info('Portal tomcat memory  {0} : {1}'.format(portal_host, tomcat_dict['Memory']))
    CsvUtil().writeDictToCsv('portalTomcat', tomcat_dict)
    return tomcat_dict


def calculate_memory(host, pid, memory):
    output = getoutput('''ssh {0} " jstat -gc {1} | gawk -F' ' '{{print}}' " '''.format(host, pid)).split('\n')
    for key, value in zip(list(filter(None, output[0].split(' '))), list(filter(None, output[1].split(' ')))):
        memory[key] = float(value)
    total_memory = (memory['OU'] + memory['EU'] + memory['S0U'] + memory['S1U']) / 1000
    return str(round(total_memory, 2))


def write_webservice_tomcat_memory():
    tomcat_pid = getoutput('''ssh {0} " ps -eaf | grep tomcat | grep ^ngdb | gawk -F' ' '{{print \\$2}}' " '''.format(
        cemod_webservice_active_fip_host))
    tomcat_dict = get_webservice_tomcat_dict(tomcat_pid)
    result_json = "[%s]" % (','.join([json.dumps(tomcat_dict)]))
    insert_into_postgres(result_json, 'webserviceTomcat')


def get_webservice_tomcat_dict(tomcat_pid):
    tomcat_dict = {'Date': datetime.now().strftime('%Y-%m-%d %H:00:00'),
                   'Host': getoutput(''' ssh {0} "hostname" '''.format(cemod_webservice_active_fip_host)),
                   'Memory': calculate_memory(cemod_webservice_active_fip_host, tomcat_pid, memory={})}
    logger.info('Webservice tomcat memory  {0}'.format(tomcat_dict['Memory']))
    CsvUtil().writeDictToCsv('webserviceTomcat', tomcat_dict)
    return tomcat_dict


def insert_into_postgres(result_json, hm_type):
    DBUtil().jsonPushToPostgresDB(result_json, hm_type)


if __name__ == '__main__':
    main()

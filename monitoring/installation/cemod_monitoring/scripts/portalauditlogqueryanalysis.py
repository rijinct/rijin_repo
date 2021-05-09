#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring team
# Version: 0.1
# Purpose: This script parses Audit Logs for No Data Available by taking
# input from user
# Date : 29-10-2019
#############################################################################
import sys
import os
import json
from commands import getoutput
from datetime import timedelta, datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from loggerUtil import loggerUtil
from linuxutils import LinuxUtility

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")",
                                                                                                                   "\")")


def main():
    create_logger()
    try:
        log_result_for_specific_time()
    except IndexError:
        log_result_for_last_hour()
    except ValueError:
        logger.info('Incorrect date Format.')
    finally:
        log_after_script_execution()


def create_logger():
    global logger
    log_file_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_file_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    logger = loggerUtil.__call__().get_logger('{0}_{1}'.format(log_file_name, log_file_time))


def log_result_for_specific_time():
    global date
    if sys.argv[1]:
        date = datetime.strptime(sys.argv[1], '%Y/%m/%d-%H:%M:%S').strftime('%Y-%m-%d ,%H')
        logger.info('Running the script for <Date ,hour> {0}'.format(date))
        write_count_of_queries()


def log_result_for_last_hour():
    global date
    date = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d ,%H')
    logger.info('Running the script for <Date , hour> {0}'.format(date))
    result_json = write_count_of_queries()
    insert_into_postgres(result_json, 'portalQueries')


def write_count_of_queries():
    if len(cemod_portal_hosts.split(' ')) >= 1:
        result_json = write_portal_query_count(json_list=[])
        return result_json
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


def write_portal_query_count(json_list):
    for portal_host in cemod_portal_hosts.split(' '):
        dirs = LinuxUtility('str_list', portal_host).list_of_dirs(
            '/var/log/portal/audit')
        logger.info("Organizations present are : %s", dirs)
        write_portal_directory_query_count(dirs, json_list, portal_host)
    return '[%s]' % (','.join(json_list))


def write_portal_directory_query_count(dirs, json_list, portal_host):
    for dir in dirs:
        logger.info("Geting the Queries count for the organization: %s", dir)
        log_file = get_latest_log_file(dir, portal_host)
        if "No such file or directory" not in log_file:
            write_query_count(dir, json_list, portal_host, log_file)
        else:
            logger.info('Log file not present in organization: %s', dir)


def get_latest_log_file(dir, portal_host):
    log_file = LinuxUtility('str', portal_host).latest_file_from_path(
        '/var/log/portal/audit/{0}/cemAuditLog_{0}*'.format(dir))
    return log_file


def write_query_count(dir, json_list, portal_host, log_file):
    portal_dict = get_required_dict(dir, portal_host)
    required_fields = {"TotalQueries": "Res:#####", "CacheQueries": "WS-CACHE", "DbQueries": "WS-DB",
                       "NoDataQueries": "SAI_ERROR_CODE_NO_DATA", "WSException": "FromWS-> Exception"}
    write_required_query_count(portal_host, portal_dict, log_file, required_fields, json_list)


def get_required_dict(dir, portal_host):
    portal_dict = {'Date': datetime.strptime(date, '%Y-%m-%d ,%H').strftime('%Y-%m-%d %H:00:00'),
                   'Host': portal_host, 'Organisation': dir}
    return portal_dict


def write_required_query_count(portal_host, portal_dict, log_file, required_fields, json_list):
    for field in required_fields:
        portal_dict[field] = get_count(portal_host, required_fields[field], log_file)
    logger.info('For host {0}: {1}'.format(portal_host, portal_dict))
    CsvUtil().writeDictToCsv('portalQueries', portal_dict)
    json_list.append(json.dumps(portal_dict))


def get_count(portal_host, pattern, file):
    return getoutput(
        '''ssh {0} "grep -c '{1}.*{3}' {2} " '''.format(portal_host, date, file, pattern))


def insert_into_postgres(result_json, hm_type):
    DBUtil().jsonPushToPostgresDB(result_json, hm_type)


def log_after_script_execution():
    logger.info(
        'Script: python {0} <Date: %Y/%m/%d-%H:%M:%S> or use python {0} to run the script to fetch values for last hour.'.format(
            sys.argv[0]))
    logger.info('View log files at : /var/local/monitoring/log/{0}.log'.format(
        sys.argv[0].replace('.py', ''), datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))


if __name__ == '__main__':
    main()

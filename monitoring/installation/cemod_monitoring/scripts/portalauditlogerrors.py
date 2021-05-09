#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring team
# Version: 0.1
# Purpose: This script writes popular reports queried by users and accessed
# from audit logs.
# input from user
# Date : 29-10-2019
#############################################################################
import sys
import os
import json
from commands import getoutput
from datetime import datetime, timedelta

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
        date = datetime.strptime(sys.argv[1], '%Y/%m/%d-%H:%M:%S').strftime('%d/%b/%Y:%H')
        logger.info('Running the script for <Date ,hour> {0}'.format(date))
        write_http_and_timeout_errors()


def log_result_for_last_hour():
    global date
    date = (datetime.now() - timedelta(hours=1)).strftime('%d/%b/%Y:%H')
    logger.info('Running the script for <Date , hour> {0}'.format(date))
    http_result, timeout_result = write_http_and_timeout_errors()
    insert_into_postgres(http_result, 'httpErrors')
    insert_into_postgres(timeout_result, 'timeoutErrors')


def write_http_and_timeout_errors():
    http_result = write_errors({'401Errors': 'HTTP/1.1\\" 401', '403Errors': 'HTTP/1.1\\" 403'})
    timeout_result = write_errors({'timeoutError': 'The timeout specified has expired: proxy: error'})
    return http_result, timeout_result


def write_errors(error_codes):
    if len(cemod_portal_hosts.split(' ')) >= 1:
        result = write_portal_errors(error_codes, json_list=[])
        return result
    else:
        logger.info('Portal Nodes are empty, nothing to parse')


def write_portal_errors(error_codes, json_list):
    for portal_host in cemod_portal_hosts.split(' '):
        log_file = get_log_file(portal_host)
        if "No such file or directory" not in log_file:
            write_file_errors(json_list, log_file, portal_host, error_codes)
        else:
            logger.info('Log file not present')
    return "[%s]" % (','.join(json_list))


def get_log_file(portal_host):
    return LinuxUtility('str', portal_host).latest_file_from_path(
        '/etc/httpd/logs/*ssl_access_log*')


def write_file_errors(json_list, log_file, portal_host, error_codes):
    for error_code in error_codes.keys():
        count, errors = get_count_and_errors(portal_host, log_file, error_codes[error_code])
        process_errors(count, error_code, errors, json_list, portal_host)


def get_count_and_errors(portal_host, latest_log_file, pattern):
    count = getoutput(
        '''ssh {0} "grep -c '{1}.*{3}' {2} " '''.format(portal_host, date, latest_log_file, pattern))
    errors = getoutput(
        '''ssh {0} "grep '{1}.*{3}' {2} " '''.format(portal_host, date, latest_log_file, pattern))
    return count, errors


def process_errors(count, error_code, errors, json_list, portal_host):
    portal_dict = get_required_dict(portal_host)
    if int(count):
        process_error(error_code, errors, json_list, portal_dict, portal_host)
    else:
        process_no_error(error_code, json_list, portal_dict, portal_host)


def get_required_dict(portal_host):
    portal_dict = {'Date': datetime.strptime(date, '%d/%b/%Y:%H').strftime('%Y-%m-%d %H:00:00'),
                   'Host': portal_host}
    return portal_dict


def process_error(error_code, errors, json_list, portal_dict, portal_host):
    for error in errors.split('\n'):
        portal_dict['ErrorCode'] = error_code
        portal_dict['ErrorDescription'] = error.split('] ')[1]
        logger.info('For host {0}: {1}'.format(portal_host, portal_dict))
        CsvUtil().writeDictToCsv('portalAuditLogErrors', portal_dict)
        json_list.append(json.dumps(portal_dict))


def process_no_error(error_code, json_list, portal_dict, portal_host):
    portal_dict['ErrorCode'], portal_dict['ErrorDescription'] = error_code, 'No error observed'
    logger.info('For host {0}: {1}'.format(portal_host, portal_dict))
    CsvUtil().writeDictToCsv('portalAuditLogErrors', portal_dict)
    json_list.append(json.dumps(portal_dict))


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

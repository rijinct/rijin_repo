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
        log_result_for_last_day()
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
        date = datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y-%m-%d')
        logger.info('Running the script for <Date ,hour> {0}'.format(date))
        get_popular_use_cases()


def log_result_for_last_day():
    global date
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info('Running the script for <Date , hour> {0}'.format(date))
    result_json = get_popular_use_cases()
    insert_into_postgres(result_json, 'popularUseCases')


def get_popular_use_cases():
    if len(cemod_portal_hosts.split(" ")) >= 1:
        json_list = write_portal_host_activity(json_list=[], use_cases={})
        result_json = "[%s]" % (','.join(json_list))
        return result_json
    else:
        logger.info('No portal hosts available')


def write_portal_host_activity(json_list, use_cases={}):
    for portal_host in cemod_portal_hosts.split(" "):
        dirs = LinuxUtility('str_list', portal_host).list_of_dirs(
            '/var/log/portal/audit')
        logger.info("Organizations present are : %s", dirs)
        use_cases = get_portal_popular_cases(dirs, portal_host, use_cases)
    json_list = write_popular_use_cases(json_list, use_cases)
    return json_list


def get_portal_popular_cases(dirs, portal_host, use_cases):
    logger.info('Getting popular use cases for : %s' % portal_host)
    for dir in dirs:
        use_cases = get_popular_use_case(portal_host, dir, use_cases)
    return use_cases


def get_popular_use_case(portal_host, dir, use_cases):
    lines = get_use_cases_output(portal_host, dir)
    for line in list(filter(None, lines.split('\n'))):
        line = list(filter(None, line.split(' ')))
        writing_use_cases_dict('%s' % (' '.join(line[1:])), int(line[0]), use_cases)
    return use_cases


def writing_use_cases_dict(use_case, count, use_cases):
    if use_case in use_cases:
        use_cases[use_case] += count
    elif use_case != 'CGF':
        use_cases[use_case] = count


def get_use_cases_output(portal_host, dir):
    log_file = LinuxUtility('str', portal_host).latest_file_from_path(
        '/var/log/portal/audit/{0}/cemAuditLog_{0}*'.format(dir))
    if "No such file or directory" not in log_file:
        return getoutput(
        '''ssh {0} "grep '{1}.*Query->:#####' {2} | gawk -F',' '{{print \\$9}}'| sort | uniq -c"'''.format(  # noqa:501
            portal_host, date, log_file))
    else:
        logger.info('Log file not present in organization: %s', dir)
        return ''


def write_popular_use_cases(json_list, use_cases):
    for use_case in use_cases:
        portal_dict = {'Date': date, 'UseCase': use_case, 'Count': str(use_cases[use_case])}
        logger.info('Aggregated Count for {0} is {1}'.format(use_case, use_cases[use_case]))
        CsvUtil().writeDictToCsv('portalPopularReports', portal_dict)
        json_list.append(json.dumps(portal_dict))
    return json_list


def insert_into_postgres(result_json, hm_type):
    DBUtil().jsonPushToPostgresDB(result_json, hm_type)


def log_after_script_execution():
    logger.info(
        'Script: python {0} <Date: %Y-%m-%d> or use python {0} to run the script to fetch values for last hour.'.format(
            sys.argv[0]))
    logger.info('View log files at : /var/local/monitoring/log/{0}.log'.format(
        sys.argv[0].replace('.py', ''), datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))


if __name__ == '__main__':
    main()

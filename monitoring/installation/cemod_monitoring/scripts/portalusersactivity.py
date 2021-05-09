#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring team
# Version: 0.1
# Purpose: This script writes user login/logout and their respective duration.
# input from user
# Date : 29-10-2019
#############################################################################
import sys
import json
import os
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


def log_result_for_last_day():
    global date
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info('Running the script for <Date , hour> {0}'.format(date))
    log_json, duration_json = get_portal_users_activity()
    insert_into_postgres(log_json, 'portalLogin')
    insert_into_postgres(duration_json, 'portalLoginDuration')


def log_result_for_specific_time():
    global date
    if sys.argv[1]:
        date = datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y-%m-%d')
        logger.info('Running the script for <Date ,hour> {0}'.format(date))
        get_portal_users_activity()


def get_portal_users_activity():
    if len(cemod_portal_hosts.split(" ")) >= 1:
        duration_list, log_list = get_portal_hosts_activity(duration_list=[], log_list=[])
        duration_json = "[%s]" % (','.join(duration_list))
        log_json = "[%s]" % (','.join(log_list))
        return log_json, duration_json
    else:
        logger.info('No portal hosts available')


def get_portal_hosts_activity(duration_list, log_list):
    for portal_host in cemod_portal_hosts.split(" "):
        dirs = LinuxUtility('str_list', portal_host).list_of_dirs('/var/log/portal/audit')
        logger.info("Organizations present are : %s",dirs)
        get_portal_host_activity(dirs, duration_list, log_list, portal_host)
    return duration_list, log_list


def get_portal_host_activity(dirs, duration_list, log_list, portal_host):
    for dir in dirs:
        count, output = get_grep_output(portal_host, dir, 'LOGGED')
        write_portal_host_duration(count, dir, duration_list, log_list, output, portal_host)


def get_grep_output(portal_host, dir, pattern):
    log_file = get_latest_log_file(dir, portal_host)
    if "No such file or directory" not in log_file:
        count = getoutput(
            '''ssh {0} "grep -c '{1}.*{3}' {2} "'''.format(portal_host, date, log_file, pattern))  # noqa:501
        output = getoutput('''ssh {0} "grep -n '{1}.*{3}' {2}  "'''.format(portal_host, date, log_file, pattern))  # noqa:501
    else:
        logger.info('Log file not present in organization: %s', dir)
        count, output = '0', ''
    return count, output


def get_latest_log_file(dir, portal_host):
    return LinuxUtility('str', portal_host).latest_file_from_path(
        '/var/log/portal/audit/{0}/cemAuditLog_{0}*'.format(dir))


def write_portal_host_duration(count, dir, duration_list, log_list, output, portal_host):
    if int(count):
        log_result = process_output(output)
        write_log_and_duration(dir, portal_host, duration_list, log_list, log_result)


def process_output(output):
    output = [i.split(',')[:6] for i in output.split('\n')]
    log_result = {}
    for login in output:
        process_output_for_login(log_result, login, output)
    return log_result


def process_output_for_login(log_result, login, output):
    if login[5] == 'LOGGEDIN':
        found = process_logout_in_output(log_result, login, output, found=False)
        if not found:
            process_no_logout_in_output(log_result, login)


def process_logout_in_output(log_result, login, output, found):
    for logout in output[output.index(login) + 1:]:
        if login[2] in logout and login[4] in logout and 'LOGGEDIN' in logout:
            break
        elif login[2] in logout and login[4] in logout:
            writing_logout_from_output(log_result, login, logout)
            found = True
            break
    return found


def writing_logout_from_output(log_result, login, logout):
    if str(login[2] + ',' + login[4]) in log_result:
        log_result[str(login[2] + ',' + login[4])].append(
            str(login[1].strip() + ',' + logout[1].strip()))
    else:
        log_result[str(login[2] + ',' + login[4])] = [
            str(login[1].strip() + ',' + logout[1].strip())]


def process_no_logout_in_output(log_result, login):
    if str(login[2] + ',' + login[4]) in log_result:
        log_result[str(login[2] + ',' + login[4])].append(str(login[1].strip() + ',No Logout'))
    else:
        log_result[str(login[2] + ',' + login[4])] = [str(login[1].strip() + ',No Logout')]


def write_log_and_duration(dir, portal_host, duration_list, log_list, log_result):
    for item in log_result.keys():
        duration_dict = get_required_dict(dir, portal_host, item)
        log_dict = get_required_dict(dir, portal_host, item)
        write_log_and_duration_dict(duration_dict, duration_list, item, log_dict, log_list, log_result)


def get_required_dict(dir, portal_host, item):
    temp_dict = {'Date': date, 'Host': portal_host, 'Organisation': dir,
                 'User': item.split(',')[0], 'IP': item.split(',')[1]}
    return temp_dict


def write_log_and_duration_dict(duration_dict, duration_list, item, log_dict, log_list, log_result):
    duration = get_duration_and_write_log(item, log_dict, log_list, log_result, duration=0)
    duration_dict['Duration'] = represent_time(duration)
    CsvUtil().writeDictToCsv('portalUserDuration', duration_dict)
    duration_list.append(json.dumps(duration_dict))


def get_duration_and_write_log(item, log_dict, log_list, log_result, duration):
    for log in log_result[item]:
        log_dict['Login'], log_dict['Logout'] = log.split(',')
        logger.info('%s' % log_dict)
        duration += calculate_time_diff(log_dict['Login'], log_dict['Logout'])
        CsvUtil().writeDictToCsv('portalUserLogs', log_dict)
        log_list.append(json.dumps(log_dict))
    return duration


def calculate_time_diff(time1, time2):
    if time2 == 'No Logout':
        return 0
    else:
        delta = datetime.strptime(time2, '%H:%M:%S:%f') - datetime.strptime(time1, '%H:%M:%S:%f')
        return delta.seconds


def represent_time(seconds):
    hours = seconds // 3600
    minutes = seconds // 60 - hours * 60
    seconds = seconds % 60
    return str('{0} hours {1} minutes {2} seconds'.format(hours, minutes, seconds))


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

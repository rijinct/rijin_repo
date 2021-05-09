'''
Created on 25-Aug-2020

@author: a4yadav
'''
from calendar import monthrange
from datetime import datetime
from datetime import timedelta
from subprocess import getoutput
import sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from enum_util import AlarmKeys
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils
from sendMailUtil import EmailUtils
from send_alarm import SnmpAlarm



create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

DIR_PATH = '/mnt/staging/import/COMMON_DIMENSION_1'
CMD_OUT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S %z'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MISSING_DIM_FILES_ALARM = AlarmKeys.DIMENSION_FILES_NOT_ARRIVING.value
SEVERITY = SnmpAlarm.get_severity_for_email(MISSING_DIM_FILES_ALARM)


def main():
    dimensions = get_list_of_dimensions()
    data = get_dimension_timestamps(dimensions)
    breached_data = get_breached_limit_list(data, dimensions)
    if breached_data:
        send_email_alert(breached_data)
        send_snmp_alarm(breached_data)


def get_list_of_dimensions():
    logger.info("Getting Dimensions Info")
    info = {}
    tags = XmlParserUtils.extract_child_tags('Dimensions', 'Dimension')
    for tag in tags:
        data = {attr: value for (attr, value) in tag.attributes.items()}
        intervals = filter_and_construct_interval(data['Interval'],
                                                  data['IntervalType'])
        if intervals:
            name, path = data['Name'], data['Input']
            start, end = [x.strftime(TIME_FORMAT) for x in intervals]
            info[name] = dict(
                zip(['Input', 'Start', 'End'], [path, start, end]))
    return info


def filter_and_construct_interval(values, interval_type):
    if interval_type.lower() == 'hour':
        return get_hour_intervals(values)
    elif interval_type.lower() == 'day':
        return get_day_intervals(values)


def get_hour_intervals(values):
    now = datetime.now()
    values = [interval.split('-') for interval in values.split(',')]
    for start, end in values:
        if (start, end) == ('23', '0') and now.hour == 0:
            return ((now - timedelta(1)).replace(hour=23, minute=0, second=0),
                    now.replace(hour=0, minute=0, second=0))
        elif now.hour == int(end):
            return (now.replace(hour=int(start), minute=0, second=0),
                    now.replace(hour=int(end), minute=0, second=0))


def get_day_intervals(values):
    now = datetime.now()
    values = [interval for interval in values.split(',')]
    for date in values:
        if ((now.day == int(date) + 1) or (date == monthrange(
                now.year, now.month)[1] and now.day == 1)) and (now.hour == 10):
            return (now.replace(day=int(date), hour=0, minute=0, second=0),
                    now.replace(day=int(date), hour=23, minute=59, second=59))


def get_dimension_timestamps(dimensions):
    result = {}
    for name, values in dimensions.items():
        result[name] = get_timestamp(values['Input'])
    return result


def get_timestamp(dir_name):
    cmd = 'ls --full-time %s | grep %s ' % (DIR_PATH, dir_name)
    output = extract_timestamp(getoutput(cmd))
    timestamp = datetime.strptime(output.strip(), CMD_OUT_DATE_FORMAT)
    return timestamp.strftime(TIME_FORMAT)


def extract_timestamp(output):
    return ' '.join(list(filter(None, output.split(' ')))[5:8])


def get_breached_limit_list(files_data, dimensions):
    result = []
    for name, dir_time in files_data.items():
        values = dimensions[name]
        start_time, end_time = values['Start'], values['End']
        if not time_in_interval(dir_time, start_time, end_time):
            res = {
                'Dimension': name,
                'Directory': '%s/%s' % (DIR_PATH, values['Input']),
                'Last Dimension File Arrival Time': dir_time,
                'Configured Time': '%s to %s' % (start_time, end_time)
            }
            result.append(res)
    return result


def time_in_interval(dir_time, start_time, end_time):
    dir_time, start_time, end_time = [
        datetime.strptime(x, TIME_FORMAT)
        for x in [dir_time, start_time, end_time]
    ]
    return start_time <= dir_time <= end_time


def send_email_alert(breached_data):
    subject = "Following dimension files are not arriving at configured time"
    html = str(HtmlUtil().generateHtmlFromDictList(subject, breached_data))
    EmailUtils().frameEmailAndSend(
        "[{0} ALERT]: Dimension files Missing at {1}".format(
            SEVERITY,
            datetime.now().strftime(TIME_FORMAT)), html, MISSING_DIM_FILES_ALARM)


def send_snmp_alarm(breached_data):
    SnmpAlarm.send_alarm(MISSING_DIM_FILES_ALARM, breached_data)


if __name__ == '__main__':
    main()

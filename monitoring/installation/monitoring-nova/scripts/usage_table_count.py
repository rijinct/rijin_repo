#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:
# Version: 0.1
# Purpose:
#
# Date:
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
import sys
import os
import traceback
from xml.dom import minidom
from datetime import date, datetime, timedelta, time
from time import mktime, strptime
from healthmonitoring.collectors.tablecountstats.bapd import _execute
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from dbConnectionManager import DbConnection
from propertyFileUtil import PropertyFileUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from dateTimeUtil import DateTimeUtil
from monitoring_utils import MonitoringUtils
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def main():
    logger.info("Fetching Usage Table Count")
    try:
        declare_globals()
        usage_tables = get_usage_tables()
        usage_table_counts = get_usage_table_counts(usage_tables)
        write_content_to_csv(usage_table_counts, usage_tables)
        push_data_to_maria_db()
        push_graph_data_to_db(usage_table_counts, usage_tables)
        logger.info("Triggering bapd utility to collect bapd data")
        _execute()
    except Exception:
        traceback.print_exc()
    logger.info("Writring to csv & Inserting to db completed")


def declare_globals():
    global Date, output_csv, min_dt, max_dt, tz, HOURS_PER_DAY
    global MIN_EPOCH, MAX_EPOCH
    Date = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
    output_csv = get_output_csv('usageOutputDir')
    min_dt, max_dt = get_report_time()
    tz = os.environ['LOCAL_TZ']
    HOURS_PER_DAY = 24
    MIN_EPOCH, MAX_EPOCH = get_epoch_thresholds()


def get_output_csv(util):
    path = PropertyFileUtil(util, 'DirectorySection').getValueForKey()
    if not os.path.exists(path):
        os.makedirs(path)
    return '{0}total_count_usage_{1}.csv'.\
        format(path, datetime.today().strftime("%Y%m%d"))


def get_epoch_thresholds():
    return DateTimeUtil().get_yesterday_in_epoch(0), \
        DateTimeUtil().get_yesterday_in_epoch(HOURS_PER_DAY) - 1


def get_report_time():
    yesterday = date.today() - timedelta(1)
    min_time = datetime.combine(yesterday, time().min)
    max_time = datetime.combine(yesterday, time().max)
    min_dt = int(mktime(strptime(str(min_time), '%Y-%m-%d %H:%M:%S'))) * 1000
    max_dt = int(mktime(strptime(str(max_time),
                                 '%Y-%m-%d %H:%M:%S.%f'))) * 1000
    return str(min_dt), str(max_dt)


def get_usage_tables():
    logger.debug('Getting all usage tables.')
    xml = get_required_xml()
    parent_tag = xml.getElementsByTagName('USAGETABLE')
    property_tag = parent_tag[0].getElementsByTagName('property')
    usages = {usage.attributes['Name'].value: usage.attributes['type'].value
              for usage in property_tag
              if usage.attributes['value'].value.lower() == "yes"}
    return usages


def get_required_xml():
    return minidom.parse("/opt/nsn/ngdb/monitoring/conf/monitoring.xml")


def get_usage_table_counts(usage_tables):
    usage_table_counts = []
    for usage_table in usage_tables.keys():
        logger.debug("Getting count for: '%s' ", usage_table)
        usage_count = get_usage_count(usage_table)
        usage_table_counts.append((usage_table, usage_count))
    return usage_table_counts


def get_usage_count(usage_table):
    sql = PropertyFileUtil('usageTablecountquery',
                           'hiveSqlSection').getValueForKey()
    query = sql.replace("usageTable", usage_table).replace("TZ_VAR", tz)
    final_sql = query.replace("minDt", min_dt).replace("maxDt", max_dt)
    content = DbConnection().getHiveConnectionAndExecute(final_sql)
    return process_usage_table_count(content)


def process_usage_table_count(usage_info):
    usage_count = {hour: 0 for hour in range(HOURS_PER_DAY)}
    for usage_values in filter(None, usage_info.split('\n')):
        hour, count = map(int, usage_values.split(','))
        usage_count[hour] = count
    usage_count[HOURS_PER_DAY] = sum(usage_count.values())
    return usage_count


def write_content_to_csv(usage_table_counts, usages):
    logger.debug('Writing content to csv.')
    with open(output_csv, 'w') as output_csv_writer:
        header = 'Date,DateInLocalTZ,Hour,%s,Mobile_Usage_Per_Hour,\
FL_Usage_Per_Hour\n' % (','.join([usage[0] for usage in usage_table_counts]))
        output_csv_writer.write(header)
        write_usage_counts(output_csv_writer, usage_table_counts, usages)


def write_usage_counts(output_csv_writer, usage_table_counts, usages):
    for hour in range(HOURS_PER_DAY + 1):
        usage_per_hour = [
            usage_count[1][hour] for usage_count in usage_table_counts]
        usage_per_hour.extend(get_agg_usages_per_hour(usage_table_counts,
                                                      hour, usages))
        content_by_hour = get_content_by_hour(hour, usage_per_hour)
        output_csv_writer.write(content_by_hour)


def get_agg_usages_per_hour(usage_table_counts, hour, usages):
    sum_mobile = sum([counts[hour] for usage, counts in usage_table_counts
                      if usages[usage] == 'mobile'])
    sum_fl = sum([counts[hour] for usage, counts in usage_table_counts
                  if usages[usage] == 'fixed_line'])
    return [sum_mobile, sum_fl]


def get_content_by_hour(hour, usage_per_hour):
    return '%s\n' % ','.join([Date,
                              get_yesterday_in_epoch(hour),
                              str(hour),
                              ','.join(map(str, usage_per_hour))])


def push_data_to_maria_db():
    logger.debug('Pushing the data to mariadb.')
    json_file = JsonUtils().convertCsvToJson(output_csv)
    DBUtil().pushToMariaDB(json_file, "usageTableCount")


def push_graph_data_to_db(usage_table_counts, usage_types):
    logger.debug('Pushing the data for usage graph.')
    for usage, counts in usage_table_counts:
        content = get_content_for_graph(usage, counts, usage_types[usage])
        DBUtil().jsonPushToMariaDB(content, "usageTableCountGraph")


def get_content_for_graph(usage, counts, usage_type):
    keys = ['Date', 'DateInLocalTZ', 'Hour', 'TableName', 'Type', 'Count']
    data = [[Date, get_yesterday_in_epoch(hour), str(hour), usage, usage_type,
            str(counts[hour])] for hour in range(HOURS_PER_DAY + 1)]
    return str([dict(zip(keys, values)) for values in data]).replace("'", '"')


def get_yesterday_in_epoch(hour):
    epoch = DateTimeUtil().get_yesterday_in_epoch(hour)
    return str(MonitoringUtils.clamp(MIN_EPOCH, epoch, MAX_EPOCH))


if __name__ == "__main__":
    main()

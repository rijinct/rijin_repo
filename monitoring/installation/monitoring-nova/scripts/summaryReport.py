'''
Created on 18-Aug-2020

@author: Monitoring Team
'''

from collections import OrderedDict
from datetime import datetime, date, timedelta
import os, sys

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from dateTimeUtil import DateTimeUtil
from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from enum_util import AlarmKeys
from generalPythonUtils import PythonUtils
from htmlUtil import HtmlUtil
from logger_util import *
from monitoring_utils import XmlParserUtils
from propertyFileUtil import PropertyFileUtil
from sendMailUtil import EmailUtils

htmlUtil = HtmlUtil()
create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

summary_report_xml_content = XmlParserUtils.get_summary_report_content()
tz = os.environ['LOCAL_TZ']


def main():
    html = "".join([frame_html()])
    EmailUtils().frameEmailAndSend("Summary Report", html,
                                   AlarmKeys.SUMMARY_REPORT.value)
    logger.info("Summary Report sent")


def frame_html():
    html_body = ''
    for summary_report_item in summary_report_xml_content:
        logger.info("Fetching info for %s ", summary_report_item)
        query = PropertyFileUtil('{0}'.format(summary_report_item['Query']),
                                 'SummarySqlSection').getValueForKey()
        output, columns = execute_query(query, summary_report_item['Dbtype'],
                                        summary_report_item['Name'])
        index = get_column_position(columns,
                                    summary_report_item['thresholdColumns'])
        color_coded_content = set_color_coded_list(output, index,
                                                   summary_report_item)
        if '' not in color_coded_content:
            content = [columns] + color_coded_content
        else:
            content = ["No {0}".format(summary_report_item['Name'])]
        html_body = html_body + str(
            htmlUtil.generateHtmlFromList(
                '{0}'.format(summary_report_item['Name']), content))
    return html_body


def get_column_position(columns, column_name):
    for index, data in enumerate(columns.split(",")):
        if column_name == data:
            return index


def execute_query(query, value, key):
    final_query = query.format(query, **get_date_arg_dict(key))
    if value == "postgres":
        output = DbConnection().getConnectionAndExecuteSql(
            final_query, "postgres")
        columns = DbConnection().getConnectionAndExecuteSql(final_query,
                                                            "postgres",
                                                            columns="yes")
    elif value == "monitoring":
        output = DbConnection().getMonitoringConnectionObject(final_query)
        columns = DbConnection().getMonitoringConnectionObject(final_query,
                                                               columns="yes")
    return output, columns


def get_date_arg_dict(type):
    arg_dict = {}
    arg_dict['tz'] = tz
    if type in ('Hdfs FileSystem', 'Failed Jobs'):
        arg_dict['todays_date'] = DateTimeUtil.get_date_with_zero_time()
    elif type in ('Kafka Connector Status', 'ETL Status', 'ETL Lag',
                  'Backlog'):
        arg_dict['last15_date'] = DateTimeUtil.get_last_15min_date()
    return arg_dict


def set_color_coded_list(boundary_list, threshold_col_position,
                         summary_report_item):
    global color_coded_list, interim_data
    color_coded_list = boundary_list.split("\n")
    for index, data in enumerate(color_coded_list):
        interim_data = data.split(",")
        set_colr_code(interim_data, index, color_coded_list,
                      threshold_col_position, summary_report_item)
        color_coded_list[index] = ",".join(interim_data)
    return color_coded_list


def set_colr_code(interim_data, index, color_coded_list,
                  threshold_col_position, summary_report_item):
    for indx, item in enumerate(interim_data):
        try:
            if summary_report_item['thresholdColumnsType'] == 'Int':
                set_color_code_for_integers(
                    threshold_col_position, indx, int(item), index,
                    summary_report_item['RedThresholdValue'],
                    summary_report_item['OrangeThresholdValue'],
                    summary_report_item['GreenThresholdValue'])
            elif summary_report_item['thresholdColumnsType'] == 'Str':
                set_color_code_with_strings(
                    threshold_col_position,
                    summary_report_item['RedThresholdValue'],
                    summary_report_item['OrangeThresholdValue'],
                    summary_report_item['GreenThresholdValue'], indx, item,
                    index)
        except ValueError:
            set_color_code_with_strings(
                threshold_col_position,
                summary_report_item['RedThresholdValue'],
                summary_report_item['OrangeThresholdValue'],
                summary_report_item['GreenThresholdValue'], indx, item, index)


def set_color_code_with_strings(column_pos, red_threshold, orange_threshold,
                                green_threshold, indx, item, index):
    if indx == column_pos and item == red_threshold:
        color_item = item + 'Red'
        interim_data[indx] = color_item
    elif indx == column_pos and item == orange_threshold:
        color_item = item + 'Orange'
        interim_data[indx] = color_item
    elif indx == column_pos and item == green_threshold:
        color_item = item + 'Green'
        interim_data[indx] = color_item
    else:
        color_coded_list[index] = ",".join(interim_data)


def set_color_code_for_integers(column_pos, indx, item, index, red_threshold,
                                orange_threshold, green_threshold):
    logger.debug("Type of red threshold is %s ", type(red_threshold))
    if indx == column_pos and int(item) > int(red_threshold):
        color_item = str(item) + 'Red'
        interim_data[indx] = color_item
    elif indx == column_pos and int(item) >= int(orange_threshold) and int(
            item) <= int(red_threshold):
        color_item = str(item) + 'Orange'
        interim_data[indx] = color_item
    elif indx == column_pos and int(item) <= int(green_threshold):
        color_item = str(item) + 'Green'
        interim_data[indx] = color_item
    else:
        color_coded_list[index] = ",".join(interim_data)


if __name__ == '__main__':
    main()

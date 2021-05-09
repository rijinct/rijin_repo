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

import commands, datetime, time, sys, os
from datetime import date, timedelta
from xml.dom import minidom
from concurrent import futures

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")",
                                                                                                                  "\")"))
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from propertyFileUtil import PropertyFileUtil
from date_time_util import DateTimeUtil

MIN_DT, MAX_DT = DateTimeUtil().required_time_info('day')[1:]


def get_usage_tables():
    mobile_usage_tables = []
    fixed_line_usage_tables = []
    xml_parser = minidom.parse('/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parent_usage_tag = xml_parser.getElementsByTagName('USAGETABLE')
    property_tag = parent_usage_tag[0].getElementsByTagName('property')
    for property_elements in property_tag:
        value = property_elements.attributes['value'].value.lower()
        if value == "yes":
            if property_elements.attributes['type'].value.lower() == 'mobile':
                mobile_usage_tables.append(property_elements.attributes['Name'].value)
            else:
                fixed_line_usage_tables.append(property_elements.attributes['Name'].value)
    return mobile_usage_tables, fixed_line_usage_tables


def write_header_if_required(header_csv_writer):
    if os.path.getsize(header_csv) == 0:
        yesterday = (datetime.datetime.today() - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
        hour_string = "Date,Hour \n"
        header_csv_writer.write(hour_string)
        for hour in xrange(0, 25):
            header_csv_writer.write(str(yesterday) + ',' + str(hour) + "\n")
        header_csv_writer.close()


def format_output():
    all_usage_csv = '/var/local/monitoring/work/total_count_usage_%s.csv' % ("allUsage")
    final_output_csv = '/var/local/monitoring/output/tableCount_Usage/total_count_usage_%s.csv' % (
        datetime.datetime.now().strftime("%Y%m%d"))
    paste_all_usage_cmd = 'paste -d, /var/local/monitoring/work/total_count_usage_us*.csv > {0}'.format(all_usage_csv)
    paste_all_usage = commands.getoutput(paste_all_usage_cmd)
    paste_header = commands.getoutput(
        'paste -d, {0} {1} {2} {3} > {4}'.format(header_csv, all_usage_csv, sum_of_row_mobile_usage_tables_csv,
                                                 sum_of_row_fixed_usage_tables_csv, final_output_csv))


def write_content(table, content):
    if len(content) > 0:
        output_csv_temp = write_and_get_temp_csv(content, table)
        sort_output = commands.getoutput('sort -g {0} | gawk -F"," \'{{print $2}}\''.format(output_csv_temp))
        output_csv = '/var/local/monitoring/work/total_count_usage_%s.csv' % (table)
        with open(output_csv, 'w') as output_csv_writer:
            output_csv_writer.write(sort_output)


def write_and_get_temp_csv(content, table):
    output_csv_temp = '/var/local/monitoring/work/total_count_usage_temp_%s.csv' % (table)
    with open(output_csv_temp, 'w') as output_csv_writer_temp:
        output_csv_writer_temp.write("Hour" + "," + table + "\n")
        write_content_to_temp_csv(content, output_csv_writer_temp)
    return output_csv_temp

def write_content_to_temp_csv(content, output_csv_writer_temp):
    temp_list = []
    hour_string = set()
    content_hour = set()
    for hour in xrange(0, 24):
        hour_string.add(str(hour))
    for item in content.split("\n"):
        if 'Java HotSpot' not in item:
            content_hour.add(item.split(",")[0])
            temp_list.append([item.split(",")[0], int(item.split(",")[1])])
            output_csv_writer_temp.write(item.split(",")[0] + "," + item.split(",")[1] + "\n")
    write_to_temp_csv(content_hour, hour_string, output_csv_writer_temp, temp_list)


def write_to_temp_csv(content_hour, hour_string, output_csv_writer_temp, temp_list):
    hour_diff = hour_string - content_hour
    for item in hour_diff:
        temp_list.append([item, 0])
        output_csv_writer_temp.write(item + ",0\n")
    sum_of_column_values = sum([int(item[1]) for item in temp_list])
    temp_list.append(['24', sum_of_column_values])
    temp_list.sort(key=lambda item: int(item[0]))
    list_of_outputs.append([int(list_item[1]) for list_item in temp_list])
    output_csv_writer_temp.write('24,{}\n'.format(str(sum_of_column_values)))


def write_sum_of_row_values_of_tables(usage_table_type=None):
    global sum_of_row_mobile_usage_tables_csv
    global sum_of_row_fixed_usage_tables_csv
    list_sum_of_row_values = list(map(sum, zip(*list_of_outputs)))
    if usage_table_type is not None:
        list_sum_of_row_values.insert(0, 'sum_of_mobile_usage_row_values')
        sum_of_row_mobile_usage_tables_csv='/var/local/monitoring/work/total_count_usage_mobile_row_values_sum.csv'
        write_to_csv(list_sum_of_row_values,sum_of_row_mobile_usage_tables_csv)
    else:
        list_sum_of_row_values.insert(0, 'sum_of_fixed_line_usage_row_values')
        sum_of_row_fixed_usage_tables_csv = '/var/local/monitoring/work/total_count_usage_fixed_row_values_sum.csv'
        write_to_csv(list_sum_of_row_values,sum_of_row_fixed_usage_tables_csv)

def write_zero_values():
    length_rows = commands.getoutput("cat /var/local/monitoring/work/total_count_usage_fixed_row_values_sum.csv |wc -l")
    if int(length_rows.strip()) == 1:
        with open("/var/local/monitoring/work/total_count_usage_fixed_row_values_sum.csv",'a') as zero_writer:
            for i in xrange(0,25):
                zero_writer.write("0"+"\n")

def write_to_csv(list_sum_of_row_values, output_csv):
    with open(output_csv, 'a') as file_object:
        for element in list_sum_of_row_values:
            file_object.write(str(element) + '\n')


def get_usage_table_content(usage_table):
    count_cmd = 'su - {0} -c "beeline -u \'{1}\' --silent=true --showHeader=false --outputformat=csv2 -e \\"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set mapred.reduce.tasks=2; select hour(from_unixtime(floor(CAST(dt AS BIGINT)/1000), \'yyyy-MM-dd HH:mm:ss.SSS\')) as created_timestamp,count(*)  from {2} where dt>={3} and dt<={4} group by hour(from_unixtime(floor(CAST(dt AS BIGINT)/1000), \'yyyy-MM-dd HH:mm:ss.SSS\'))  order by created_timestamp; \\" 2>Logs.txt"'.format(
        cemod_hdfs_user, cemod_hive_url, usage_table, MIN_DT, MAX_DT)
    content = commands.getoutput(count_cmd)
    return usage_table, content

def push_graph_data_to_db(usage_table_counts, usage_types):
    global HOURS_PER_DAY 
    HOURS_PER_DAY = 24
    usage, count = usage_table_counts
    count_per_hour = [0 for i in range(HOURS_PER_DAY+1)]
    for item in count.split("\n"):
        index, value = map(int, item.split(','))
        count_per_hour[index] = value
    count_per_hour[HOURS_PER_DAY] = sum(count_per_hour)
    content = get_content_for_graph(usage, count_per_hour, usage_types)
    DBUtil().jsonPushToPostgresDB(str(content).replace("'", '"'), "usageTableCountGraph")
    

def get_content_for_graph(usage, counts, usage_type):
    if usage_type == None:
        usage_type = "fixed_line"
    Date  = DateTimeUtil().get_yesterday_date_time()
    keys = ['Date', 'DateInLocalTZ', 'Hour', 'TableName', 'Type', 'Count']
    data = [[Date, str(DateTimeUtil().get_yesterday_in_epoch(hour)), str(hour), usage, usage_type,
            str(counts[hour])] for hour in range(HOURS_PER_DAY + 1)]
    return str([dict(zip(keys, values)) for values in data])


def fetch_and_write_usage_tables_content(usage_tables, usage_type=None):
    global list_of_outputs
    global header_csv

    header_csv = '/var/local/monitoring/work/usageHeader.csv'
    with open(header_csv, 'w') as header_csv_writer:
        write_header_if_required(header_csv_writer)
    
    list_of_outputs = []
    with futures.ThreadPoolExecutor() as executor:
        list_of_usage_table_counts = list(executor.map(get_usage_table_content, usage_tables))
    
    for table, content in list_of_usage_table_counts:
        write_content(table, content)
        push_graph_data_to_db((str(table), content), usage_type)
    if usage_type:
        write_sum_of_row_values_of_tables(usage_type)
    else:
        write_sum_of_row_values_of_tables()


def push_data_to_postgres_db():
    output_file_path = PropertyFileUtil('usageTableCount', 'DirectorySection').getValueForKey()
    file_name = output_file_path + "total_count_usage_{0}.csv".format(datetime.datetime.today().strftime("%Y%m%d"))
    json_file_name = JsonUtils().convertCsvToJson(file_name)
    DBUtil().pushDataToPostgresDB(json_file_name, "usageTableCount")


def process_check():
    process_cmd = 'ps -eaf | grep "%s" | grep -v grep | gawk -F" " \'{print $2}\'' % (os.path.basename(__file__))
    process = commands.getoutput(process_cmd).split('\n')
    if len(process) > 3:
        exit(0)


def main():
    clean_up_temp_files = commands.getoutput('rm -rf {0}'.format("/var/local/monitoring/work/total_count_usage*"))
    clean_up_temp_files = commands.getoutput('rm -rf {0}'.format("/var/local/monitoring/work/usageHeader.csv*"))
    process_check()
    fetch_and_write_usage_tables_content(get_usage_tables()[0], 'mobile')
    fetch_and_write_usage_tables_content(get_usage_tables()[1])
    write_zero_values()
    format_output()
    push_data_to_postgres_db()


if __name__ == "__main__":
    main()

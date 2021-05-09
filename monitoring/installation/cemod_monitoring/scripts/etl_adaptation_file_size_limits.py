import sys
from commands import getoutput
from xml.dom import minidom
import datetime
import os

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils
from loggerUtil import loggerUtil
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from jsonUtils import JsonUtils
from dbUtils import DBUtil

current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name, currentDate=current_date)
LOGGER = loggerUtil.__call__().get_logger(log_file)

to_email = ['Adaptation,total_files,small_files,large_files,remarks']


def get_etl_adaptations():
    global file_count_threshold_limits
    adaptations = {}
    xmlparser = minidom.parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')
    parent_tag = xmlparser.getElementsByTagName('BACKLOG')[0]
    file_count_threshold_limits = get_file_count_threshold_limits(parent_tag)
    input_tag_list = parent_tag.getElementsByTagName('Input')
    for input_tag in input_tag_list:
        try:
            adaptations[input_tag.attributes['Name'].value] = {'import_dir': input_tag.attributes['Import'].value,
                                                               'Thresholdlimit': input_tag.attributes[
                                                                   'Thresholdlimit'].value,
                                                               'minFileSize': input_tag.attributes['minFileSize'].value,
                                                               'maxFileSize': input_tag.attributes['maxFileSize'].value}
        except Exception as error:
            LOGGER.info(error)
    return adaptations


def get_file_count_threshold_limits(parent_tag):
    file_count_threshold_limits = {}
    file_count_properties = parent_tag.getElementsByTagName('FileSizeProperty')[0]
    file_count_limit_inputs = file_count_properties.getElementsByTagName('property')
    for property in file_count_limit_inputs:
        file_count_threshold_limits[property.attributes['Name'].value] = property.attributes['value'].value
    return file_count_threshold_limits


def fetch_and_write_etl_adaptation_file_sizes(adaptation, adaptation_properties):
    MNT_IMPORT_DIRECTORY = '/mnt/staging/import'
    output_data = getoutput(
        'hdfs dfs -ls -R %s/%s' % (MNT_IMPORT_DIRECTORY, adaptation_properties['import_dir'])).strip().splitlines()
    if output_data:
        list_of_file_sizes = get_file_size_list(output_data)
        if list_of_file_sizes:
            write_etl_adaptation_small_files_and_large_files(adaptation, list_of_file_sizes, adaptation_properties)


def get_file_size_list(output_data):
    list_of_file_sizes = []
    if 'No such file or directory' in output_data[0]:
        LOGGER.info(output_data)
    else:
        try:
            for line in output_data:
                list_of_file_sizes.append(line.split()[4])
            return list_of_file_sizes
        except Exception as e:
            LOGGER.info(output_data, '\n%s' % e)


def write_etl_adaptation_small_files_and_large_files(adaptation, list_of_file_sizes, adaptation_properties):
    total_files = len(list_of_file_sizes)
    small_files, large_files = get_small_and_large_files_count(list_of_file_sizes, adaptation_properties)
    check_file_limitations(total_files, small_files, large_files, adaptation, adaptation_properties)
    write_to_csv(adaptation, adaptation_properties, total_files, small_files, large_files)


def get_small_and_large_files_count(list_of_file_sizes, adaptation_properties):
    file_sizes = []
    for size in list_of_file_sizes:
        file_sizes.append(round(int(size) / (1024 * 1024), 2))
    small_files = get_small_files(file_sizes, adaptation_properties)
    large_files = get_large_files(file_sizes, adaptation_properties)
    return len(small_files), len(large_files)


def get_large_files(file_sizes, adaptation_properties):
    return list(filter(
        lambda file_size: True if file_size > int(adaptation_properties['maxFileSize'].split('MB')[0]) else False,
        file_sizes))


def get_small_files(file_sizes, adaptation_properties):
    return list(filter(
        lambda file_size: True if file_size < int(adaptation_properties['minFileSize'].split('MB')[0]) else False,
        file_sizes))


def check_file_limitations(total_files, small_files, large_files, adaptation, adaptation_properties):
    if total_files > int(adaptation_properties['Thresholdlimit']):
        percentage_of_smallfiles = (float(small_files) / float(total_files)) * 100
        percentage_of_largefiles = (float(large_files) / float(total_files)) * 100
        if percentage_of_smallfiles > int(
                file_count_threshold_limits['PercentOfSmallFiles']) and percentage_of_largefiles > int(
            file_count_threshold_limits['PercentOfLargeFiles']):
            to_email.append('%s,%s,%s,%s,percentage of both small files and large files exceed limit' % (
                adaptation, total_files, small_files, large_files))
        elif percentage_of_smallfiles > int(file_count_threshold_limits['PercentOfSmallFiles']):
            to_email.append('%s,%s,%s,%s,percentage of small files exceed limit' % (
                adaptation, total_files, small_files, large_files))
        elif percentage_of_largefiles > int(
                file_count_threshold_limits['PercentOfLargeFiles']):
            to_email.append('%s,%s,%s,%s,percentage of large files exceed limit' % (
                adaptation, total_files, small_files, large_files))


def write_to_csv(adaptation, adaptation_properties, total_files, small_files, large_files):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    data_to_write = {'Time': current_time, 'adaptation_name': adaptation,
                     'adaptation_dir': adaptation_properties['import_dir'],
                     'total_files': total_files, 'small_files_count': small_files, 'large_files_count': large_files}
    CsvUtil().writeDictToCsv('etlAdaptationProp', data_to_write)


def push_to_postgres():
    UTIL_TYPE = 'etlAdaptationProp'
    outPutDirectory = PropertyFileUtil(UTIL_TYPE, 'DirectorySection').getValueForKey()
    file_name = '%s_%s.csv' % (UTIL_TYPE, datetime.date.today())
    jsonFileName = JsonUtils().convertCsvToJson(os.path.join(outPutDirectory, file_name))
    LOGGER.info('Pushing etl adaptation(s) properties to Postgres DB ')
    DBUtil().pushDataToPostgresDB(jsonFileName, UTIL_TYPE)


def main():
    adaptations = get_etl_adaptations()
    for adaptation in adaptations:
        adaptatin_properties = adaptations[adaptation]
        fetch_and_write_etl_adaptation_file_sizes(adaptation, adaptatin_properties)
    if len(to_email) > 1:
        html = HtmlUtil().generateHtmlFromList('Below adaptation(s) exceeded small/large file limit', to_email)
        EmailUtils().frameEmailAndSend('Adaptation(s) exceeding the small/large file limit', html)
    push_to_postgres()


if __name__ == '__main__':
    main()

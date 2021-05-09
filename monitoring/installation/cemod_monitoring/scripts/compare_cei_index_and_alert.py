import os,sys
from xml.dom.minidom import parseString, parse
from datetime import datetime,date,timedelta
from collections import OrderedDict
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from csvUtil import CsvUtil
from loggerUtil import loggerUtil
from propertyFileUtil import PropertyFileUtil
from postgres_connection import PostgresConnection
from date_time_util import DateTimeUtil
from jsonUtils import JsonUtils
from htmlUtil import HtmlUtil
from sendMailUtil import EmailUtils


current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name, currentDate=current_date)
LOGGER = loggerUtil.__call__().get_logger(log_file)


def main():
    global postgres_conn, date, today, yesterday_date,last_week,day_before_yesterday
    day_before_yesterday = (date.today() - timedelta(2)).strftime('%Y-%m-%d %H:%M:%S')
    last_week =(date.today() - timedelta(8)).strftime('%Y-%m-%d %H:%M:%S')
    yesterday = date.today() - timedelta(1)
    today = date.today().strftime('%Y-%m-%d')
    yesterday_date = yesterday.strftime('%Y-%m-%d %H:%M:%S')
    postgres_conn = PostgresConnection().get_instance()
    if get_qs_job():
        execute_workflow('today')


def get_presentation_names_from_xml():
    monintoring_xml = parse(r'/opt/nsn/ngdb/monitoring/conf/ceimonitoring.xml')
    cei_attributes = monintoring_xml.getElementsByTagName('CEI')[0]
    presentations = cei_attributes.getElementsByTagName('presentation')
    return sorted([presentation.attributes['Name'].value for presentation in presentations ])


def write_null_data_to_db(frequency):
    if frequency == 'today':
        date_passed = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
        date_passed_with_time = (date.today() - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
    elif frequency == 'yesterday':
        date_passed = (date.today() - timedelta(2)).strftime('%Y-%m-%d')
        date_passed_with_time = (date.today() - timedelta(2)).strftime('%Y-%m-%d %H:%M:%S')
    elif frequency == 'lastweek':
        date_passed = (date.today() - timedelta(8)).strftime('%Y-%m-%d')
        date_passed_with_time = (date.today() - timedelta(8)).strftime('%Y-%m-%d %H:%M:%S')
    output_csv = 'ceiIndex_%s.csv' % date_passed
    #LOGGER.info("Date passed is {0}".format(date_passed))
    presentation_names = get_presentation_names_from_xml()
    for presentation_name in presentation_names:
        content = '{0},{1},{2}'.format(date_passed_with_time,presentation_name,'NULL')
        CsvUtil().writeToCsv(output_csv, 'ceiIndex', content)
    jsonFileName=JsonUtils().convertCsvToJson(output_directory+output_csv)
    DBUtil().pushDataToPostgresDB(jsonFileName,"ceiIndex")


def send_email_alert(diff_list):
    LOGGER.info("sending email alert")
    html = HtmlUtil().generateHtmlFromDictList('', diff_list)
    EmailUtils().frameEmailAndSend('Index change by more than {0} points Alert on {1}'.format(get_cei_comparison_value_from_xml(),datetime.now().strftime("%d th %b %Y")),html,"yes")


def get_qs_job():
    global output_csv,output_directory
    output_csv = 'ceiIndex_%s.csv' % (date.today() - timedelta(1)).strftime("%Y-%m-%d")
    output_directory = PropertyFileUtil('ceiIndex','DirectorySection').getValueForKey()
    boundary = "select DATE(maxvalue) from sairepo.boundary where jobid like 'Perf_CEI2_O_INDEX_CITY_1_DAY_QSJob'";
    output = postgres_conn.fetch_records(boundary)[0][0].strftime("%Y-%m-%d")
    todaydate_obj = datetime.strptime(today,'%Y-%m-%d')
    outputdate_obj = datetime.strptime(output,'%Y-%m-%d')
    if todaydate_obj == outputdate_obj:
        if os.path.exists(output_directory+output_csv):
           LOGGER.info("File already exists")
           return False
        else: return True
    else:
        LOGGER.info("Qs Job not executed")
        return False



def get_portal_response(frequency):
    if frequency == 'today':
        yesterday = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
        epoch_time = str(DateTimeUtil().get_date_in_epoch(yesterday)) + '000'
    elif frequency == 'yesterday':
        day_before_yesterday_date = (date.today() - timedelta(2)).strftime('%Y-%m-%d')
        epoch_time = str(DateTimeUtil().get_date_in_epoch(day_before_yesterday_date)) + '000'
    elif frequency == 'lastweek':
        last_week_date = (date.today() - timedelta(8)).strftime('%Y-%m-%d')
        epoch_time = str(DateTimeUtil().get_date_in_epoch(last_week_date)) + '000'
    cache_tab_query = PropertyFileUtil('ceiTrendsResponse', 'PostgresSqlSection').getValueForKey().replace('Date', str(epoch_time))
    LOGGER.info("Fetching response from CACHE_TAB")
    output = postgres_conn.fetch_records(cache_tab_query)
    try:
        output_val = parseString(output[0][0])
    except IndexError:
        output_val = ''
    return output_val


def parse_xml_output(xml_output, attribute_indexes={}):
    attribute_parent_tag = xml_output.getElementsByTagName('row_header')[0]
    attribute_indexes_tag = xml_output.getElementsByTagName('row_elements')[0].getElementsByTagName('row')[0]
    attributes = attribute_parent_tag.getElementsByTagName('attribute_name')
    indexes = attribute_indexes_tag.getElementsByTagName('value')
    for (attribute, index) in zip(attributes, indexes):
        try:
            attribute_indexes[attribute.firstChild.nodeValue.split('/')[1]] = index.firstChild.nodeValue
        except AttributeError:
            attribute_indexes[attribute.firstChild.nodeValue.split('/')[1]] = 'NULL'
    del attribute_indexes['Dimensions']
    return attribute_indexes



def map_attribute_to_presentation_names(attribute_indexes):
    monintoring_xml = parse(r'/opt/nsn/ngdb/monitoring/conf/ceimonitoring.xml')
    cei_attributes = monintoring_xml.getElementsByTagName('CEI')[0]
    presentations = cei_attributes.getElementsByTagName('presentation')
    for presentation in presentations:
        attr = presentation.attributes['attributeName'].value
        reprs = presentation.attributes['Name'].value
        if attr in attribute_indexes:
            attribute_indexes[reprs] = attribute_indexes.pop(attr)


def write_data_to_csv(attribute_indexes,frequency):
    LOGGER.info("Writing Data to Csv")
    if frequency == 'today':
        date_passed = (date.today() - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
    elif frequency == 'yesterday':
        date_passed = (date.today() - timedelta(2)).strftime('%Y-%m-%d %H:%M:%S')
    elif frequency == 'lastweek':
        date_passed = (date.today() - timedelta(8)).strftime('%Y-%m-%d %H:%M:%S')
    output_csv = 'ceiIndex_%s.csv' % date_passed.split(" ")[0]
    for attributes in sorted(attribute_indexes.keys()):
        output = ','.join([date_passed, attributes, attribute_indexes[attributes]])
        CsvUtil().writeToCsv(output_csv, 'ceiIndex', output)
    LOGGER.info("Pushing data to DB")
    pushDataToPostgresDB(output_csv)

def pushDataToPostgresDB(output_csv):
    jsonFileName=JsonUtils().convertCsvToJson(output_directory+output_csv)
    DBUtil().pushDataToPostgresDB(jsonFileName,"ceiIndex")


def get_cei_comparison_value_from_xml():
    xmlparser = parse('/opt/nsn/ngdb/monitoring/conf/ceimonitoring.xml')
    parent_cei_tag = xmlparser.getElementsByTagName('CEI')
    child_cei_tag=parent_cei_tag[0].getElementsByTagName('indexDeltaPointsThreshold')
    return child_cei_tag[0].attributes['value'].value



def compare_cei_today_with_last_day(yesterday_data,day_before_data,last_week_data):
    LOGGER.info("Comparing CEI Index")
    day_before_yesterday = (date.today() - timedelta(2)).strftime('%d/%m/%Y')
    last_week =(date.today() - timedelta(8)).strftime('%d/%m/%Y')
    yesterday = date.today() - timedelta(1)
    yesterday_date = yesterday.strftime('%d/%m/%Y')
    diff_list = []
    for item_1 in range(0,len(yesterday_data)):
        index_1_name, index_1_value = yesterday_data[item_1][0]['Presentation Name'], yesterday_data[item_1][0]['Index Value']
        index_2_name, index_2_value = day_before_data[item_1][0]['Presentation Name'], day_before_data[item_1][0]['Index Value']
        index_3_name, index_3_value = last_week_data[item_1][0]['Presentation Name'], last_week_data[item_1][0]['Index Value']
        if (index_1_value != 'NULL' and index_2_value != 'NULL' and index_3_value != 'NULL'):
            diff_index_value = int(index_1_value) - int(index_2_value)
            diff_index_lastweek_value = int(index_1_value) - int(index_3_value)
            cei_comparison_value = get_cei_comparison_value_from_xml()
            if diff_index_value > int(cei_comparison_value) or diff_index_value < -int(cei_comparison_value) or diff_index_lastweek_value> int(cei_comparison_value) or diff_index_lastweek_value < -int(cei_comparison_value):
                diff_dict = OrderedDict()
                diff_dict['Index'] = index_1_name
                diff_dict['Last Day('+yesterday.strftime("%d/%m/%Y")+')'] = index_1_value
                diff_dict['Two Days Ago('+day_before_yesterday+')'] = index_2_value
                diff_dict['Last Week('+last_week+')'] = index_3_value
                diff_list.append(diff_dict)
        elif (index_1_value == 'NULL' and index_2_value == 'NULL' and index_3_value == 'NULL'):
            LOGGER.info("All values of the index {0} are NULL",index_1_name)
        else:
            null_dict = OrderedDict()
            null_dict['Index'] = index_1_name
            null_dict['Last Day('+yesterday.strftime("%d/%m/%Y")+')'] = index_1_value
            null_dict['Two Days Ago('+day_before_yesterday+')'] = index_2_value
            null_dict['Last Week('+last_week+')'] = index_3_value
            diff_list.append(null_dict)
    return diff_list



def get_data_from_db():
    todays_data_from_db_query = PropertyFileUtil('cei_index_query','PostgresSqlSection').getValueForKey().replace("DATE",yesterday_date)
    todays_data_from_db = postgres_conn.fetch_records(todays_data_from_db_query)
    day_bef_data_from_db_query = PropertyFileUtil('cei_index_query','PostgresSqlSection').getValueForKey().replace("DATE",day_before_yesterday)
    day_bef_data_from_db = postgres_conn.fetch_records(day_bef_data_from_db_query)
    last_week_data_from_db_query= PropertyFileUtil('cei_index_query','PostgresSqlSection').getValueForKey().replace("DATE",last_week)
    last_week_data_from_db = postgres_conn.fetch_records(last_week_data_from_db_query)
    if len(day_bef_data_from_db) == 0:
        execute_workflow('yesterday')
    if len(last_week_data_from_db) == 0:
        execute_workflow('lastweek')
        exit(0)
    return (todays_data_from_db,day_bef_data_from_db,last_week_data_from_db)

def execute_workflow(frequency):
        LOGGER.info("Executing the work flow")
        xml_output = get_portal_response(frequency)
        if xml_output:
            attribute_indexes = parse_xml_output(xml_output)
            map_attribute_to_presentation_names(attribute_indexes)
            write_data_to_csv(attribute_indexes,frequency)
            yesterday_data,day_before_data,last_week_data = get_data_from_db()
            diff_list = compare_cei_today_with_last_day(yesterday_data,day_before_data,last_week_data)
        else:
            LOGGER.info("Execute work flow else part has frequency {0}".format(frequency))
            write_null_data_to_db(frequency)
            yesterday_data,day_before_data,last_week_data = get_data_from_db()
            diff_list = compare_cei_today_with_last_day(yesterday_data,day_before_data,last_week_data)
        if diff_list:           
            send_email_alert(diff_list)
        else:
            LOGGER.info("No Index change hence no Alert")           


if __name__ == '__main__':
    main()

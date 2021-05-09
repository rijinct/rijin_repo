import sys
import os
import datetime
import multiprocessing
from commands import getoutput
from datetime import date, timedelta

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from loggerUtil import loggerUtil
from jsonUtils import JsonUtils
from dbUtils import DBUtil
from date_time_util import DateTimeUtil
from multiprocessing_util import multiprocess

exec(
    open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
        read()).replace("(", "(\"").replace(")", "\")")

current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name,
                                               currentDate=current_date)
LOGGER = loggerUtil.__call__().get_logger(log_file)

UTIL_TYPE = 'DataTraffic'
file_path = PropertyFileUtil(UTIL_TYPE, 'DirectorySection').getValueForKey()
min_dt, max_dt = DateTimeUtil().required_time_info('day')[1:]
columns = PropertyFileUtil(UTIL_TYPE,
                           'ColumnSection').getValueForKey().split(';')


@multiprocess
def data_to_monitor(column_name, results):
    LOGGER.info('getting %s of yesterday' % column_name)
    try:
        cmd_string = 'su - {0} -c "beeline -u \'{1}\' --silent=true --showHeader=false --outputformat=csv2 -e \\"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring; select {2} from ps_cdm2_segg_1_1_day where dt >= {3} and dt <= {4}; \\" 2>Logs.txt"'.format(
            cemod_hdfs_user, cemod_hive_url, column_name, min_dt, max_dt)
        data = getoutput(cmd_string)
        write_output_data(column_name, data, results)
    except Exception as e:
        LOGGER.info(e)


def write_output_data(column_name, data, results):
    if data == 'NULL':
        data = -1
    for header in PropertyFileUtil(
            UTIL_TYPE, 'HeaderSection').getValueForKey().split(',')[1:]:
        if header in column_name:
            results[header] = data
            break


def get_total_data_volume_in_gb(results):
    total_data_volume = results['total_data_volume']
    if total_data_volume is not -1:
        results['total_data_volume'] = round(
            float(total_data_volume) / (1024 ** 3), 3)


def write_results_to_csv(results):
    yesterday_date = str(date.today() - timedelta(1))
    results = dict(results)
    get_total_data_volume_in_gb(results)
    results['Date'] = yesterday_date
    CsvUtil().writeDictToCsv(UTIL_TYPE, results)
    csvfile_name = '%s_%s.csv' % (UTIL_TYPE, datetime.date.today())
    push_to_postgres(os.path.join(file_path, csvfile_name), UTIL_TYPE)


def push_to_postgres(filepath, utiltype):
    jsonFileName = JsonUtils().convertCsvToJson(filepath)
    DBUtil().pushDataToPostgresDB(jsonFileName, utiltype)


def main():
    manager = multiprocessing.Manager()
    results = manager.dict()
    data_to_monitor(columns, results)
    write_results_to_csv(results)


if __name__ == '__main__':
    main()

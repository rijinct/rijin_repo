import os
import re
import json
from propertyFileUtil import PropertyFileUtil
from dbConnectionManager import DbConnection
from csvUtil import CsvUtil
from enum_util import TypeOfUtils
from dbUtils import DBUtil
from subprocess import getoutput
from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import QueryExecutor
from healthmonitoring.collectors.utils.queries import Queries
from healthmonitoring.framework.specification.defs import DBType
logger = _LocalLogger.get_logger(__name__)

UTIL_TYPE = TypeOfUtils.DATA_TRAFFIC.value
RADIO_UTIL_TPYE = TypeOfUtils.RADIO_TABLE_UTIL.value


def main():
    is_running = process_check()
    if is_running:
        logger.info(
                "Data traffic script is under execution")
    else:
        _execute()


def process_check():
    cmd = "ps -eaf | grep {} | grep -v grep".format(
        os.path.basename(__file__).split(".")[0])
    processes = getoutput(cmd)
    processes = processes.split('\n')
    return len(processes) > 3


def _execute():
    raw_data = Collector().collect()
    present = Presenter(raw_data).present()
    return present


class Collector:
    def __init__(self):
        self._level = "DAY"
        self._identifier = '%sTableCount' % self._level
        date = DateTimeUtil.get_partition_info('DAY')[0]
        self._date = DateTimeUtil.parse_date(
            date, '%Y-%m-%d %H-%M-%S').strftime('%Y-%m-%d %H:%M:%S')
        self._tables = [
            'Perf_CDM2_SEGG_1_1_%s_AggregateJob' % self._level,
            'Perf_RADIO_SEGG_1_%s_AggregateJob' % self._level
        ]

    def collect(self):
        if self._validate_execution_parameters():
            logger.info("Getting data traffic info")
            column_table_list = self._get_column_data(
                UTIL_TYPE, "cemod.ps_cdm2_segg_1_1_day")
            column_table_list.extend(
                self._get_column_data(RADIO_UTIL_TPYE, "ps_radio_segg_1_day"))
            return self._get_response(column_table_list)
        else:
            logger.info("Cmd and radio segg boundary not completed")
            exit(1)

    def _validate_execution_parameters(self):
        if all(self._validate_job_boundary(table) for table in self._tables):
            return not self._validate_script_already_executed()
        else:
            return False

    def _get_column_data(self, name, table_name):
        data = []
        columns = PropertyFileUtil(name,
                                   'ColumnSection').getValueForKey().split(";")
        for col_name in columns:
            data.append((col_name, table_name))
        return data

    def _validate_job_boundary(self, _table):
        prop = {"table_name": _table}
        boundary = QueryExecutor.execute(DBType.POSTGRES_SDK,
                                         Queries.TABLE_MAX_VALUE,
                                         **prop)[0][0].strftime('%Y-%m-%d')
        expected_boundary = DateTimeUtil.parse_date(
            self._date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        return boundary == expected_boundary

    def _validate_script_already_executed(self):
        directory = PropertyFileUtil(self._identifier,
                                     "DirectorySection").getValueForKey()
        date = DateTimeUtil.get_today_date().strftime('%Y-%m-%d')
        file = '{0}{1}_{2}.csv'.format(directory, self._identifier, date)
        return os.path.exists(file)

    def _get_response(self, column_and_table_list):
        results = {}
        for column_and_table in column_and_table_list:
            column, table = column_and_table
            min_dt, max_dt = DateTimeUtil.get_partition_info('DAY')[1:]
            query = "select {} from {} where dt >= '{}' and dt <= '{}'".format(
                column, table, min_dt, max_dt)
            response = DbConnection().getHiveConnectionAndExecute(query)
            if response == 'NULL':
                response = -1
            column_header = re.findall(r'as ([A-Za-z_]+)', column)
            results[column_header[0]] = response
        return results


class Presenter:
    def __init__(self, raw_data):
        raw_data['Date'] = DateTimeUtil.now().strftime(
            DateTimeUtil.DATE_FORMAT)
        self._data = [raw_data]

    def present(self):
        if len(self._data[0].keys()) > 1:
            file_name = '{0}_{1}.csv'.format(
                UTIL_TYPE,
                DateTimeUtil.now().strftime("%Y-%m-%d_%H"))
            CsvUtil().writeDictToCsvDictWriter(UTIL_TYPE, self._data[0],
                                               file_name)
            DBUtil().jsonPushToMariaDB(json.dumps(self._data), UTIL_TYPE)
            logger.info(
                "Completed writing data traffic data to csv and maridb")
            return self._data


if __name__ == '__main__':
    main()

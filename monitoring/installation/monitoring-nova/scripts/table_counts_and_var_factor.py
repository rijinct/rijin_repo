import os
import re
import sys
from collections import OrderedDict
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from dbConnectionManager import DbConnection
from csvUtil import CsvUtil
from dateTimeUtil import DateTimeUtil
from dbUtils import DBUtil
from enum_util import TableCounts
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class TableCount():

    def __init__(self):
        self._level = self._get_level()
        self._identifier = '%sTableCount' % self._level
        self._initialise_values()

    def _get_level(self):
        try:
            if sys.argv[1] in ('day', 'week', 'month'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.exception('Usage: python %s day|week|month' % sys.argv[0])
            sys.exit(1)

    def _initialise_values(self):
        date, start_epoch = DateTimeUtil().requiredTimeInfo(self._level)[:2]
        self._date = datetime.strptime(
            date, '%Y-%m-%d %H-%M-%S').strftime('%Y-%m-%d %H:%M:%S')
        self._start_epoch = start_epoch
        self._create_prop_dict()
        self._table = 'Perf_CDM2_SEGG_1_1_%s_AggregateJob' % self._level.upper(
        )  # noqa: 501

    def _create_prop_dict(self):
        self._prop = OrderedDict()
        keys = ["imsi", "device", "cell", "imei", "region", "msisdn"]
        values = [
            "imsi_id", "device_type", "cell_sac_name", "imei", "region",
            "msisdn"
        ]
        for key, value in zip(keys, values):
            self._prop[key] = value

    def validate_execution_parameters(self):
        if self._validate_job_boundary():
            return not self._validate_script_already_executed()
        else:
            return False

    def get_and_write_table_counts(self):
        output = []
        tables = self._get_required_tables()
        for table in tables:
            logger.debug('Getting values for %s' % table)
            counts = self._get_count_and_var_factor(table)
            CsvUtil().writeDictToCsv(self._identifier, counts)
            output.append(counts)
        return output

    def write_to_db(self, output):
        logger.debug('Inserting to db')
        json = str(output).replace("'", '"')
        DBUtil().jsonPushToMariaDB(json, self._identifier)

    def _validate_job_boundary(self):
        sql = PropertyFileUtil("tableMaxvalue",
                               "PostgresSqlSection").getValueForKey()
        query = sql.replace('tablename', self._table)
        boundary = DbConnection().getConnectionAndExecuteSql(query, "postgres")
        expected_boundary = datetime.strptime(
            self._date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        return boundary == expected_boundary

    def _validate_script_already_executed(self):
        directory = PropertyFileUtil(self._identifier,
                                     "DirectorySection").getValueForKey()
        date = DateTimeUtil.get_required_start_date(self._level)
        file = '%s%s_%s.csv' % (directory, self._identifier, date)
        return os.path.exists(file)

    def _get_required_tables(self):
        logger.debug("Getting all the '%s' tables" ,self._level)
        tables = self._get_query_output('showtablesquery')
        return re.findall(r"\bps\w+{}\b".format(self._level), tables)

    def _get_query_output(self, queryname, table='', column=''):
        query = PropertyFileUtil(queryname, 'hiveSqlSection').getValueForKey()
        replacements = {
            r'\btablename\b': table,
            r'\bcolumn\b': column,
            r'\bstartpartition\b': str(self._start_epoch),
        }
        for org, new in replacements.items():
            query = re.sub(org, new, query)
        return DbConnection().getHiveConnectionAndExecute(query)

    def _get_count_and_var_factor(self, table):
        output = {'Date': self._date, 'Table': table}
        keys = [
            'TotalCount', 'Column', 'DistinctCountOfSelectedColumn',
            'VarFactor'
        ]
        colm = self._get_req_column(table, self._get_matched_columns(table),
                                    self._get_colm_from_table_name(table))
        for key, value in zip(keys, self._get_result(table, colm)):
            output[key] = value
        logger.debug("Table '%s' : '%s' ", table, output)
        return output

    def _get_req_column(self, table, matched_columns, matched_name):
        if "ana" in table:
            return 'msisdn'
        elif 'imsi_id' in matched_columns:
            return 'imsi_id'
        elif matched_name:
            return self._prop[list(matched_name)[0]]
        else:
            return self._get_column_from_matched_cols(matched_columns)

    def _get_matched_columns(self, table):
        columns = self._get_query_output('columnquery', table).split('\n')
        return set(columns) & set(self._prop.values())

    def _get_colm_from_table_name(self, table):
        return set(table.split('_')) & set(self._prop.keys())

    def _get_column_from_matched_cols(self, matched_columns):
        for column in self._prop.values():
            if column in matched_columns:
                return column
        else:
            return ''

    def _get_result(self, table, column):
        if column:
            return self._get_result_for_column(table, column)
        else:
            return self._get_result_for_no_column(table)

    def _get_result_for_column(self, table, column):
        counts = self._get_query_output("columnCountQuery", table, column)
        colm = list(self._prop.keys())[list(self._prop.values()).index(column)]
        result = self._process_counts(counts)
        result.insert(1, colm)
        return result

    def _get_result_for_no_column(self, table):
        total_count = self._get_query_output("totalCountQuery", table)
        return [
            total_count, 'NA',
            str(TableCounts.COULUMN_COUNT_FOR_NA.value),
            str(TableCounts.VAR_FACTOR_FOR_NA.value)
        ]

    def _process_counts(self, output):
        total, subscriber_count = output.split(',')
        try:
            var_factor = float(total) / float(subscriber_count)
        except ZeroDivisionError:
            var_factor = 0
        return list(map(str, [total, subscriber_count, round(var_factor, 2)]))


def main():
    table_count_obj = TableCount()
    if table_count_obj.validate_execution_parameters():
        output = table_count_obj.get_and_write_table_counts()
        table_count_obj.write_to_db(output)
    else:
        logger.info('Necessary conditions were not met to execute the script.')


if __name__ == '__main__':
    main()

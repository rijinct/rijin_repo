'''
Created on 27-Apr-2020

@author: deerakum
'''
import sys
import datetime

from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.enum_util import TableCounts
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.command import CommandExecutor, Command
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    hdfs_user = SpecificationUtil.get_field("hdfs_user", "cemod_hdfs_user")
    hive_url = SpecificationUtil.get_field("hive", "cemod_hive_url")
    raw_data = Collector(hdfs_user, hive_url).collect()
    return Presenter(raw_data).present()


class Collector:

    def __init__(self, hdfs_user, hive_url):
        self._table_props = TableProperties(hdfs_user, hive_url)

    def collect(self):
        return self._get_table_counts()

    def _get_table_counts(self):
        output = []
        tables = self._get_required_tables()
        for table in tables:
            logger.info('Getting values for %s' % table)
            output.append(self._get_count_and_var_factor(table))
        return output

    def _get_required_tables(self):
        logger.info('Getting all the %s tables' % self._table_props.level)
        level = self._table_props.level
        table_level_dict = {"table": f"ps*{level}*"}
        return QueryExecutor.get_query_output(
            Command.HIVE_GET_RECORD_COUNT_FROM_TABLE, table_level_dict,
            self._table_props).split("\n")

    def _get_count_and_var_factor(self, table):
        return VarFactorCalculator(table,
                                   self._table_props).get_result_for_column()


class VarFactorCalculator:

    def __init__(self, table, table_props):
        self._table = table
        self._table_props = table_props

    def get_result_for_column(self):
        matched_columns = self._get_matched_columns()
        colm_from_table = self._get_colm_from_table_name()
        column = self._get_req_column(self._table, matched_columns,
                                      colm_from_table)
        if column:
            result = self._execute_col_count_query(column)
        else:
            result = self._execute_total_count_query()
        return result

    def _get_matched_columns(self):
        table_name_dict = {"table": self._table}
        columns = QueryExecutor.get_query_output(Command.HIVE_SHOW_COLUMNS,
                                                 table_name_dict,
                                                 self._table_props).split('\n')
        return set(columns) & set(self._table_props.var_factor_cols.values())

    def _get_colm_from_table_name(self):
        return set(self._table.split('_')) & set(
            self._table_props.var_factor_cols.keys())

    def _get_req_column(self, table, matched_columns, matched_name):
        if "ana" in table:
            return 'msisdn'
        elif 'imsi_id' in matched_columns:
            return 'imsi_id'
        elif matched_name:
            return self._table_props.var_factor_cols[list(matched_name)[0]]
        else:
            for column in list(self._table_props.var_factor_cols.values()):
                if column in matched_columns:
                    return column
            else:
                return ''

    def _execute_col_count_query(self, column):
        column_count_dict = {
            "column": column,
            "table": self._table,
            "start_epoch": self._table_props.start_epoch
        }
        counts = QueryExecutor.get_query_output(Command.HIVE_COLUMN_COUNT,
                                                column_count_dict,
                                                self._table_props)
        var_factor_column = list(
            self._table_props.var_factor_cols.values()).index(column)
        colm = list(
            self._table_props.var_factor_cols.keys())[var_factor_column]
        result = Processor.process(counts.split("\n")[1])
        result.append(colm)
        return result

    def _execute_total_count_query(self):
        total_count_dict = {
            "table": self._table,
            "start_epoch": self._table_props.start_epoch
        }
        total_count = QueryExecutor.get_query_output(Command.HIVE_TOTAL_COUNT,
                                                     total_count_dict,
                                                     self._table_props)
        return [
            total_count, TableCounts.COULUMN_COUNT_FOR_NA.value,
            TableCounts.VAR_FACTOR_FOR_NA.value, 'NA'
        ]


class QueryExecutor:

    @staticmethod
    def get_query_output(query_key, query_arg_dict, table_props):
        query_arg_dict["hdfs_user"] = table_props.hdfs_user
        query_arg_dict["hive_url"] = table_props.hive_url
        return CommandExecutor.get_output(query_key, **query_arg_dict)


class Processor:

    def __init__(self, raw_data):
        self._raw_data = raw_data

    @staticmethod
    def process(output):
        total, subscriber_count = output.split(',')
        try:
            var_factor = float(total) / float(subscriber_count)
        except ZeroDivisionError:
            var_factor = 0
        return [total, subscriber_count, round(var_factor, 2)]


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        output = []
        cur_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for row in self._data:
            total_count, distinct_count, var_factor, column = row
            row_dict = {
                "Date": cur_date,
                "TotalCount": total_count,
                "DistinctCountOfSelectedColumn": distinct_count,
                "VarFactor": var_factor,
                "Column": column
            }
            output.append(row_dict)
        return output


class TableProperties:

    def __init__(self, hdfs_user, hive_url):
        self._hdfs_user = hdfs_user
        self._hive_url = hive_url
        self._level = self._get_level()
        self._start_epoch = DateTimeUtil.get_partition_info(self._level)[1]
        self._prop = dict([('imsi', 'imsi_id'), ('device', 'device_type'),
                           ('cell', 'cell_sac_name'), ('imei', 'imei'),
                           ('region', 'region'), ('msisdn', 'msisdn')])

    @property
    def level(self):
        return self._level

    @property
    def hdfs_user(self):
        return self._hdfs_user

    @property
    def hive_url(self):
        return self._hive_url

    @property
    def date(self):
        return self._date

    @property
    def start_epoch(self):
        return self._start_epoch

    @property
    def var_factor_cols(self):
        return self._prop

    def _get_level(self):
        try:
            if sys.argv[1] in ('day', 'week', 'month'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.info('Usage: python %s day|week|month' % sys.argv[0])
            sys.exit(1)


if __name__ == '__main__':
    main()

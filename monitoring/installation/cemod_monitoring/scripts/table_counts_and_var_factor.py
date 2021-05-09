import re
import os
import sys
from commands import getoutput
from collections import OrderedDict
from datetime import datetime

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from propertyFileUtil import PropertyFileUtil
from csvUtil import CsvUtil
from date_time_util import DateTimeUtil
from dbUtils import DBUtil
from loggerUtil import loggerUtil
from enum_util import TableCounts

exec(
    open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
    read().replace("(", "(\"").replace(")", "\")"))


class TableCount():
    def __init__(self):
        self._logger = self._set_logger()
        self._level = self._get_level()
        self._identifier = '%sTableCount' % self._level
        self._template = '''su - {0} -c "beeline -u \'{1}\' --silent=true --showHeader=false --outputformat=csv2 -e \\"SET mapreduce.job.queuename=root.nokia.ca4ci.monitoring;set mapred.reduce.tasks=2; query \\" 2>Logs.txt"'''.format(  # noqa: 501
            cemod_hdfs_user, cemod_hive_url)
        self._initialise_values()

    def _set_logger(self):
        file = os.path.basename(sys.argv[0]).replace('.py', '')
        time = datetime.now().strftime('%Y-%m-%d')
        return loggerUtil.__call__().get_logger('%s_%s' % (file, time))

    def _get_level(self):
        try:
            if sys.argv[1] in ('day', 'week', 'month'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            self._logger.info('Usage: python %s day|week|month' % sys.argv[0])
            sys.exit(1)

    def _initialise_values(self):
        date, start_epoch = DateTimeUtil().required_time_info(self._level)[:2]
        self._date = datetime.strptime(
            date, '%Y-%m-%d %H-%M-%S').strftime('%Y-%m-%d %H:%M:%S')
        self._start_epoch = start_epoch
        self._create__prop_dict()

    def _create__prop_dict(self):
        self._prop = OrderedDict()
        keys = ["imsi", "device", "cell", "imei", "region", "msisdn"]
        values = [
            "imsi_id", "device_type", "cell_sac_name", "imei", "region",
            "msisdn"
        ]
        for key, value in zip(keys, values):
            self._prop[key] = value

    def _get_required_tables(self):
        self._logger.info('Getting all the %s tables' % self._level)
        tables = self._get_query_output('showtablesquery')
        return re.findall(r"\bps\w+{}\b".format(self._level), tables)

    def _get_query_output(self, queryname, table='', column=''):
        query = PropertyFileUtil(queryname, 'hiveSqlSection').getValueForKey()
        replacements = {
            r'\btablename\b': table,
            r'\bcolumn\b': column,
            r'\bstartpartition\b': str(self._start_epoch),
        }
        for org, new in replacements.iteritems():
            query = re.sub(org, new, query)
        return getoutput(self._template.replace('query', query))

    def _get_count_and_var_factor(self, table):
        output = {'Date': self._date, 'Table': table}
        keys = [
            'TotalCount', 'DistinctCountOfSelectedColumn', 'VarFactor',
            'Column'
        ]
        colm = self._get_req_column(table, self._get_matched_columns(table),
                                    self._get_colm_from_table_name(table))
        for key, value in zip(keys, self._get_result_for_column(table, colm)):
            output[key] = value
        self._logger.info('{0}:{1}'.format(table, output))
        return output

    def _get_req_column(self, table, matched_columns, matched_name):
        if "ana" in table:
            return 'msisdn'
        elif 'imsi_id' in matched_columns:
            return 'imsi_id'
        elif matched_name:
            return self._prop[list(matched_name)[0]]
        else:
            for column in self._prop.values():
                if column in matched_columns:
                    return column
            else:
                return ''

    def _get_matched_columns(self, table):
        columns = self._get_query_output('columnquery', table).split('\n')
        return set(columns) & set(self._prop.values())

    def _get_colm_from_table_name(self, table):
        return set(table.split('_')) & set(self._prop.keys())

    def _get_result_for_column(self, table, column):
        if column:
            counts = self._get_query_output("columnCountQuery", table, column)
            colm = self._prop.keys()[self._prop.values().index(column)]
            result = self._process_counts(counts)
            result.append(colm)
        else:
            total_count = self._get_query_output("totalCountQuery", table)
            result = [
                total_count,
                str(TableCounts.COULUMN_COUNT_FOR_NA.value),
                str(TableCounts.VAR_FACTOR_FOR_NA.value), 'NA'
            ]
        return result

    def _process_counts(self, output):
        total, subscriber_count = output.split(',')
        try:
            var_factor = float(total) / float(subscriber_count)
        except ZeroDivisionError:
            var_factor = 0
        return map(str, [total, subscriber_count, round(var_factor, 2)])

    def get_table_counts(self):
        output = []
        tables = self._get_required_tables()
        for table in tables:
            self._logger.info('Getting values for %s.' % table)
            output.append(self._get_count_and_var_factor(table))
        return output

    def write_to_csv(self, output):
        self._logger.info('Writing to csv')
        for table_result in output:
            CsvUtil().writeDictToCsv(self._identifier, table_result)

    def write_to_db(self, output):
        self._logger.info('Inserting to db')
        json = str(output).replace("'", '"')
        DBUtil().jsonPushToPostgresDB(json, self._identifier)


def main():
    table_count_obj = TableCount()
    output = table_count_obj.get_table_counts()
    table_count_obj.write_to_csv(output)
    table_count_obj.write_to_db(output)


if __name__ == '__main__':
    main()

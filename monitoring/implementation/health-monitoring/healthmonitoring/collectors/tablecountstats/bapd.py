'''
Created on 15-Feb-2021

@author: deerakum
'''
import json

from dbConnectionManager import DbConnection
from dbUtils import DBUtil
from monitoring_utils import XmlParserUtils
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import Queries, QueryReplacer
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    _execute()


def _execute():
    raw_data = Collector().collect()
    logger.info("Data collection completed, data is %s", raw_data)
    processed_data = Processor(raw_data).process()
    Presenter(processed_data).present()


class Collector:
    def __init__(self):
        pass

    def collect(self):
        logger.info("Data collection Started")
        column_counts = self._get_column_counts_for_usage()
        usage_count = self._get_usage_count()
        return column_counts, usage_count

    def _get_column_counts_for_usage(self):
        topo_col_cnt = []
        topologies = {
            topology: info['Table']
            for topology, info in
            XmlParserUtils.get_topologies_from_xml().items()
        }
        for topology, table in topologies.items():
            topo_dict = {'topology_name': topology}
            final_query = QueryReplacer.get_replaced_sql(
                Queries.COLUMN_COUNT_QUERY, **topo_dict)
            result = DbConnection().getConnectionAndExecuteSql(
                final_query, "postgres")
            topo_col_cnt.append(
                {topology: {
                    'columncount': result,
                    'tablename': table
                }})
        return topo_col_cnt

    def _get_usage_count(self):
        date_dict = {'date_and_time': DateTimeUtil.get_curr_min_date()}
        return DbConnection().getMonitoringConnectionObject(
            QueryReplacer.get_replaced_sql(Queries.USAGE_COUNT_MON_QUERY,
                                           **date_dict))


class Processor:
    def __init__(self, processed_data):
        self._col_counts, self._usage_counts = processed_data[
            0], processed_data[1]

    def process(self):
        logger.info("Data Processing Started, usage count is %s",
                    self._usage_counts)
        processed_counts = self._get_processed_col_counts()
        filter_usage_counts = self._get_filtered_usage_counts()
        logger.debug("Column Counts is :%s , Usage Counts is : %s",
                     processed_counts, filter_usage_counts)
        return self._calculate_bapd(processed_counts, filter_usage_counts)

    def _get_processed_col_counts(self):
        table_col_count = {}
        for topologies in self._col_counts:
            for topo_name, topo_value in topologies.items():
                if topo_value['tablename'] in table_col_count:
                    col_count = table_col_count[topo_value['tablename']]
                    final_col_count = int(
                        topo_value['columncount']) + int(col_count)
                    table_col_count[topo_value['tablename']] = final_col_count
                else:
                    table_col_count[
                        topo_value['tablename']] = topo_value['columncount']
        return table_col_count

    def _get_filtered_usage_counts(self):
        processed_usage_counts = []
        removal_list = [
            'Date', 'DateInLocalTZ', 'Mobile_Usage_Per_Hour',
            'FL_Usage_Per_Hour', 'Hour'
        ]
        usage_count_dict = json.loads(str(self._usage_counts.split("\n")[0]))
        [usage_count_dict.pop(key, None) for key in removal_list]
        for key, value in usage_count_dict.items():
            if int(value) != 0:
                processed_usage_counts.append({key: value})
        return processed_usage_counts

    def _calculate_bapd(self, processed_counts, filter_usage_counts):
        total_bapd = 0
        for usage_count in filter_usage_counts:
            for table_name, rec_count in usage_count.items():
                for col_table_name, col_count in processed_counts.items():
                    if table_name == col_table_name:
                        total_bapd += int(col_count) * int(rec_count)
        logger.info("Total Bapd is %s", total_bapd)
        return total_bapd


class Presenter:
    def __init__(self, data):
        self._bapd = data

    def present(self):
        bapd_dict = [{
            'Date': DateTimeUtil.get_current_hour_date(),
            'BAPD': self._bapd
        }]
        bapd_json = str(bapd_dict).replace("'", '"')
        DBUtil().jsonPushToMariaDB(bapd_json, "Bapd")
        logger.info("Inserting data to db is completed")


if __name__ == '__main__':
    main()

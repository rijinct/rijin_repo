'''
Created on 16-May-2020

@author: deerakum
'''
import sys

from healthmonitoring.collectors.utils.queries import Queries, QueryReplacer
from healthmonitoring.collectors.utils.connection import ConnectionFactory
from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    processed_data = Processor(raw_data).process()
    return Presenter(processed_data).present()


class Collector:

    def collect(self):
        self._input_arg = self._get_input_arg()
        arg_dict = {"pattern": self._input_arg}
        if self._input_arg in ("DAY", "WEEK", "MONTH"):
            return self._get_aggregation_cache(arg_dict)
        else:
            return self._get_dimension_cache()

    def _get_aggregation_cache(self, arg_dict):
        expected_qs = self.get_query_result(Queries.EXPECTED_QS, arg_dict)
        arg_dict['unixtime'] = DateTimeUtil.get_partition_info(
            self._input_arg)[1]
        actual_qs = self.get_query_result(Queries.ACTUAL_QS, arg_dict)
        return expected_qs, actual_qs

    def _get_dimension_cache(self):
        qs_dict = {"pattern": "Entity"}
        expected_qs = self.get_query_result(Queries.EXPECTED_DIMENSION_QS,
                                            qs_dict)
        logger.debug("Expected Qs has %s", expected_qs)
        qs_dict = {"pattern": "es_"}
        actual_qs = self.get_query_result(Queries.ACTUAL_DIMENSION_QS, qs_dict)
        logger.debug("Actual Qs has %s", actual_qs)
        return expected_qs, actual_qs

    def get_query_result(self, query, arg_dict):
        replaced_sql = QueryReplacer.get_replaced_sql(query, **arg_dict)
        connection = ConnectionFactory.get_connection(DBType.POSTGRES_SDK)
        return connection.fetch_records(replaced_sql)

    def _get_input_arg(self):
        try:
            if sys.argv[1] in ('DAY', 'WEEK', 'MONTH', 'DIMENSION'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.info('Usage: python %s DAY|WEEK|MONTH|DIMENSION',
                        sys.argv[0])
            sys.exit(1)


class Processor:

    def __init__(self, raw_data):
        self._expected_qs, self._actual_qs = map(dict, raw_data)

    def process(self):
        combined_qs = []
        for key, value in self._expected_qs.items():
            combined_qs.append((key, value, self._actual_qs.get(key, "")))
        return combined_qs


class Presenter:

    def __init__(self, processed_data):
        self._processed_data = processed_data

    def present(self):
        output_list = []
        for value in self._processed_data:
            temp_dict = {
                "TableName": value[0],
                "ExpectedCount": value[1],
                "ActualCount": value[2]
            }
            output_list.append(temp_dict)
        logger.debug("Processed Data is %s ", output_list)
        return output_list


if __name__ == "__main__":
    main()

'''
Created on 05-May-2020

@author: khv
'''

import sys

from healthmonitoring.collectors.utils.connection import ConnectionFactory
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import Queries, QueryReplacer
from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)


def main():
    _execute()


def _execute():
    raw_data = Collector().collect()
    print(Presenter(raw_data).present())


class Collector:

    def __init__(self):
        self._input_arg = self._get_input_arg()

    def collect(self):
        arg_dict = self._get_arg_dict()
        if self._input_arg == "Usage":
            query = Queries.USAGE_FAILED_JOBS
        else:
            query = Queries.AGG_FAILED_JOBS
        replaced_query = QueryReplacer.get_replaced_sql(query, **arg_dict)
        connection = ConnectionFactory.get_connection(DBType.POSTGRES_SDK)
        return connection.fetch_records(replaced_query)

    def _get_arg_dict(self):
        if self._input_arg == "Usage":
            params = UsageParams.get_usage_params(self._input_arg)
        elif self._input_arg == "15MIN":
            params = UsageParams.get_usage_params(self._input_arg)
        elif self._input_arg == "HOUR":
            params = HourParams.get_hour_params(self._input_arg)
        elif self._input_arg in ("DAY", "WEEK", "MONTH"):
            params = DayWeekMonthParams.get_dwm_params(self._input_arg)
        return params

    def _get_input_arg(self):
        try:
            if sys.argv[1] in ('Usage', '15MIN', 'HOUR', 'DAY', 'WEEK',
                               'MONTH'):
                return sys.argv[1]
            else:
                raise IndexError
        except IndexError:
            logger.info('Usage: python %s Usage|15MIN|HOUR|DAY|WEEK|MONTH',
                        sys.argv[0])
            sys.exit(1)


class UsageParams:

    @staticmethod
    def get_usage_params(input_arg):
        return {
            "start_time": DateTimeUtil.get_last_15min_date(),
            "end_time": DateTimeUtil.get_current_hour_date(),
            "job_pattern": input_arg
        }


class HourParams:

    @staticmethod
    def get_hour_params(input_arg):
        return {
            "start_time": DateTimeUtil.get_last_hour_date(),
            "end_time": DateTimeUtil.get_current_hour_date(),
            "job_pattern": input_arg
        }


class DayWeekMonthParams:

    @staticmethod
    def get_dwm_params(input_arg):
        return {
            "start_time": DateTimeUtil.get_curr_min_date(),
            "end_time": DateTimeUtil.get_curr_max_date(),
            "job_pattern": input_arg
        }


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        output_list = []
        for item in self._data:
            temp_dict = {
                "Date":
                DateTimeUtil.get_current_hour_date(),
                "Job Name":
                item[0],
                "Start Time":
                DateTimeUtil.to_date_string(item[1],
                                            DateTimeUtil.POSTGRES_DATE_FORMAT),
                "End Time":
                DateTimeUtil.to_date_string(item[2],
                                            DateTimeUtil.POSTGRES_DATE_FORMAT),
                "Status":
                item[3],
                "Error Description":
                item[4]
            }
            output_list.append(temp_dict)
        return output_list


if __name__ == '__main__':
    main()

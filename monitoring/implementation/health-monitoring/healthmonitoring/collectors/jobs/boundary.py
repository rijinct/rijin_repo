'''
Created on 08-May-2020

@author: khv
'''

import sys

from healthmonitoring.collectors import _LocalLogger
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.framework.specification.defs import DBType

logger = _LocalLogger.get_logger(__name__)


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:

    def __init__(self):
        self._input_arg = self._get_input_arg()

    def collect(self):
        arg_dict = {"input_arg": self._input_arg}
        if self._input_arg == "Usage":
            query = Queries.USAGE_BOUNDARY
        else:
            query = Queries.AGG_BOUNDARY
        return QueryExecutor.execute(DBType.POSTGRES_SDK, query, **arg_dict)

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


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        output_list = []
        for job_name, max_value, cycles_behind, enabled in self._data:
            temp_dict = {
                "Date":
                DateTimeUtil.get_current_hour_date(),
                "Job Name": job_name,
                "Max Value":
                DateTimeUtil.to_date_string(max_value,
                                            DateTimeUtil.POSTGRES_DATE_FORMAT),
                "Cycles Behind": cycles_behind,
                "Job Enabled": enabled
            }
            output_list.append(temp_dict)
        return output_list


if __name__ == '__main__':
    main()

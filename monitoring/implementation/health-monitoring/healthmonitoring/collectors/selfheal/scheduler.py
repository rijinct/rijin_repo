'''
Created on 04-May-2020

@author: deerakum
'''
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
        self._connection = ConnectionFactory.get_connection(
            DBType.POSTGRES_SDK)

    def collect(self):
        sql_arg_dict = {
            "start_time": DateTimeUtil.get_last_hour_date(),
            "job_pattern": "Usage%",
            "error_description": '%Too many%'
        }
        jobs_count = self._get_jobs_count(Queries.LAST_HOUR_USAGE_JOBS,
                                          sql_arg_dict)
        error_jobs_count = self._get_jobs_count(
            Queries.LAST_HOUR_USAGE_ERROR_JOBS, sql_arg_dict)
        return jobs_count, error_jobs_count

    def _get_jobs_count(self, query, sql_arg_dict):
        replaced_sql = QueryReplacer.get_replaced_sql(query, **sql_arg_dict)
        return self._connection.fetch_records(replaced_sql)


class Presenter:

    def __init__(self, processed_data):
        self._jobs_count, self._error_jobs_count = processed_data

    def present(self):
        job_dict = {
            "jobs_count": self._jobs_count[0][0],
            "error_jobs_count": self._error_jobs_count[0][0]
        }
        return [job_dict]


if __name__ == "__main__":
    main()

'''
Created on 03-May-2020

@author: khv
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

    def collect(self):
        replaced_sql = QueryReplacer.get_replaced_sql(
            Queries.POSTGRES_USER_COUNT)
        connection = ConnectionFactory.get_connection(DBType.POSTGRES_SDK)
        return connection.fetch_records(replaced_sql)


class Presenter:

    def __init__(self, data):
        self._data = dict(data)

    def present(self):
        output = []
        for schema_name, count in self._data.items():
            count_dict = {
                "Date": DateTimeUtil.get_current_hour_date(),
                "SchemaName": schema_name,
                "UserCount": count
            }
            output.append(count_dict)
        return output


if __name__ == '__main__':
    main()

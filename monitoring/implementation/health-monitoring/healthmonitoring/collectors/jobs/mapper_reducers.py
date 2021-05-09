'''
Created on 17-Aug-2020

@author: deerakum
'''
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.collectors.utils.queries import Queries, QueryExecutor
from healthmonitoring.framework.specification.defs import DBType


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    return Presenter(raw_data).present()


class Collector:
    def collect(self):
        date_dict = {
            'start_time': DateTimeUtil.get_date_with_zero_time(),
            'end_time': DateTimeUtil.get_date_with_last_min_time()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.MAPPERS_REDUCER, **date_dict)


class Presenter:
    def __init__(self, raw_data):
        self._data = raw_data

    def present(self):
        output_list = []
        for load_time, job_id, report_time, time_zone, \
            total_mappers, total_reducers, cpu_time, total_time, \
                records_inserted in self._data:
            temp_dict = {
                "Job_Name": job_id,
                "Partition": report_time,
                "TimeZone": time_zone,
                "Mappers": total_mappers,
                "Reducers": total_reducers,
                "CPU Time": cpu_time,
                "Real Time": total_time,
                "Records": records_inserted
            }
            output_list.append(temp_dict)
        return output_list


if __name__ == '__main__':
    main()

'''
Created on 20-May-2020

@author: praveenD
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
    processed_data = Processor(raw_data).process()
    return Presenter(processed_data).present()


class Collector():

    AGGREGATIONS = ('15MIN', 'HOUR', 'DAY', 'WEEK', 'MONTH')
    SUMMARY_AGGREGATIONS = ('15MINSUMMARY', 'HOURSUMMARY', 'DAYSUMMARY',
                            'WEEKSUMMARY')

    def __init__(self):
        self._input_arg = self._get_input_arg()

    def collect(self):
        if self._input_arg in Collector.AGGREGATIONS:
            return self._input_arg, self.collect_aggregations(self._input_arg)
        elif self._input_arg in Collector.SUMMARY_AGGREGATIONS:
            return self._input_arg, \
                _SummaryAggregations._get_job_count_summary(self._input_arg)

    def collect_aggregations(self, input_arg):
        agg = _Aggregations()
        if input_arg == '15MIN':
            return agg._get_fifteen_min_aggregations()
        elif input_arg == 'HOUR':
            return agg._get_hour_aggregations()
        elif input_arg == 'DAY':
            return agg._get_day_aggregations()
        elif input_arg == 'WEEK':
            return agg._get_week_aggregations()
        else:
            return agg._get_month_aggregations()

    def _get_input_arg(self):
        try:
            script_name, arg = sys.argv
            if arg in Collector.AGGREGATIONS + \
                    Collector.SUMMARY_AGGREGATIONS:
                return arg
            else:
                raise IndexError
        except IndexError:
            script_name = sys.argv[0]
            logger.info('usage 1: python {} 15MIN|HOUR|DAY|WEEK|MONTH'.format(
                script_name))
            logger.info(
                'usage 2: python {} 15MINSUMMARY|HOURSUMMARY|DAYSUMMARY|WEEKSUMMARY'  # noqa: 501
                .format(script_name))
            sys.exit(1)


class Processor:
    def __init__(self, raw_data):
        self._data = raw_data

    def process(self):
        if self._data[1] and len(self._data[1][0]) == 6:
            return self._process_data()
        else:
            return self._data

    def _process_data(self):
        processed_data = []
        for item in self._data[1]:
            self._process_data_for_exec_duration(item, processed_data)
        return [self._data[0], processed_data]

    def _process_data_for_exec_duration(self, item, processed_data):
        time_frame, job_name, start_time, end_time, \
            execution_duration, status = item
        if execution_duration is None:
            execution_duration = self._get_execution_duration_from_start_time(
                start_time)
        processed_data.append((time_frame, job_name, start_time, end_time,
                               execution_duration, status))

    def _get_execution_duration_from_start_time(self, start_time):
        if isinstance(start_time, str):
            start_time = DateTimeUtil.parse_date(
                start_time, DateTimeUtil.POSTGRES_DATE_FORMAT)
        delta = DateTimeUtil.now() - start_time
        return round((delta.total_seconds() / 60), 2)


class Presenter:
    def __init__(self, data):
        self._data = data

    def present(self):
        output = []
        if 'SUMMARY' in self._data[0]:
            return self._get_agg_summary_data(output)
        else:
            for row in self._data[1]:
                time_frame, job_name, start_time, end_time, \
                    execution_duration, status = row
                row_dict = {
                    "Time_Frame": time_frame,
                    "Job_Name": job_name,
                    "Start_Time": str(start_time),
                    "End_Time": str(end_time),
                    "Execution_Duration": execution_duration,
                    "Status": status
                }
                output.append(row_dict)
            return output

    def _get_agg_summary_data(self, output):
        summary_type = self._data[0].split('SUMMARY')[0]
        for row in self._data[1]:
            time_frame, agg_end_time, total_jobs_executed, \
                zero_duration, success, error, running = row
            agg_type_end_time = "%s_agg_end_time" % summary_type
            if summary_type == '15MIN':
                summary_type = 'HOUR'
            row_dict = {
                summary_type: time_frame,
                agg_type_end_time: str(agg_end_time),
                "Total_Jobs_Executed": total_jobs_executed,
                "Zero_Duration": zero_duration,
                "Success": success,
                "Error": error,
                "Running": running
            }
            output.append(row_dict)
        return output


class _Aggregations:
    def _get_hour_aggregations(self):
        arg_dict = {
            'last_hour': DateTimeUtil.get_last_hour(),
            'current_hour': DateTimeUtil.get_current_hour(),
            'date_hour': DateTimeUtil.get_date_till_hour()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.HOUR_AGGREGATIONS, **arg_dict)

    def _get_day_aggregations(self):
        arg_dict = {
            'day': DateTimeUtil.get_previous_day(),
            'curr_min_date': DateTimeUtil.get_curr_min_date(),
            'curr_max_date': DateTimeUtil.get_curr_max_date()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.DAY_AGGREGATIONS, **arg_dict)

    def _get_week_aggregations(self):
        arg_dict = {
            'week_start_day': DateTimeUtil.get_week_start_day(),
            'week_end_day': DateTimeUtil.get_week_end_day(),
            'curr_min_date': DateTimeUtil.get_curr_min_date(),
            'curr_max_date': DateTimeUtil.get_curr_max_date()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.WEEK_AGGREGATIONS, **arg_dict)

    def _get_month_aggregations(self):
        arg_dict = {
            'curr_min_date': DateTimeUtil.get_curr_min_date(),
            'curr_max_date': DateTimeUtil.get_curr_max_date()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.MONTH_AGGREGATIONS, **arg_dict)

    def _get_fifteen_min_aggregations(self):
        arg_dict = {
            'last_hour': DateTimeUtil.get_last_hour(),
            'current_hour': DateTimeUtil.get_current_hour(),
            'date_hour': DateTimeUtil.get_date_till_hour()
        }
        return QueryExecutor.execute(DBType.POSTGRES_SDK,
                                     Queries.FIFTEEN_MIN_AGGREGATIONS,
                                     **arg_dict)


class _SummaryAggregations:
    @staticmethod
    def _get_job_count_summary(input_arg):
        args_dict = {
            'curr_min_date': DateTimeUtil.get_curr_min_date(),
            'curr_max_date': DateTimeUtil.get_curr_max_date()
        }
        query = {
            'HOURSUMMARY': Queries.HOUR_JOB_COUNT_SUMMARY,
            'DAYSUMMARY': Queries.DAY_JOB_COUNT_SUMMARY,
            'WEEKSUMMARY': Queries.WEEK_JOB_COUNT_SUMMARY,
            '15MINSUMMARY': Queries.FIFTEEN_MIN_JOB_COUNT_SUMMARY
        }[input_arg]
        return QueryExecutor.execute(DBType.POSTGRES_SDK, query, **args_dict)


if __name__ == '__main__':
    main()

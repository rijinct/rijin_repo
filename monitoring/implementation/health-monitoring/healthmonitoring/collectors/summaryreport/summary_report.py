'''
Created on 04-Jun-2020

@author: praveenD
'''
from healthmonitoring.collectors.utils.date_time_util import DateTimeUtil
from healthmonitoring.framework.util.specification import SpecificationUtil
from healthmonitoring.collectors.utils.file_util import FileUtils, CsvUtils

from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)

LOG_PATH = '/var/local/monitoring/output'


def main():
    print(_execute())


def _execute():
    raw_data = Collector().collect()
    logger.debug("Raw data from collector is %s", raw_data)
    return Presenter(raw_data).present()


class Collector():

    def __init__(self):
        self._summary = {}

    def collect(self):
        if DateTimeUtil.get_today_date().strftime(
                '%Y-%m-%d') == DateTimeUtil.get_week_start_date().strftime(
                '%Y-%m-%d'):
            self._get_week_jobs_summary()
        self._get_day_jobs_summary()
        self._get_job_counts_summary()
        self._get_boundary_summary()
        self._get_table_count_and_tnp_latency()
        self._get_monitor_data()
        return self._summary

    def _get_week_jobs_summary(self):
        self._summary['week_job_status'] = _JobStatus.get_week_job_status()
        self._summary['week_job_count'] = _JobCount.get_week_job_count()
        self._summary['week_boundary'] = _Boundary.get_week_boundary()
        self._summary['week_table_count'] = _TableCount.get_week_table_count()

    def _get_day_jobs_summary(self):
        self._summary['day_table_count'] = _TableCount.get_day_table_count()
        self._summary['day_job_status'] = _JobStatus.get_day_job_status()
        self._summary['day_boundary'] = _Boundary.get_day_boundary()
        self._summary['day_job_count'] = _JobCount.get_day_job_count()

    def _get_job_counts_summary(self):
        self._summary['hour_job_count'] = _JobCount.get_hour_job_count()
        self._summary[
            'fifteen_min_job_count'] = _JobCount.get_fifteen_min_job_count()

    def _get_boundary_summary(self):
        self._summary['hour_boundary'] = _Boundary.get_hour_boundary()
        self._summary['usage_boundary'] = _Boundary.get_usage_boundary()
        self._summary['re_agg_boundary'] = _Monitor.get_re_agg_boundary()

    def _get_table_count_and_tnp_latency(self):
        self._summary[
            'table_count_usage'] = _TableCountUsage.get_table_count_usage()
        self._summary['tnp_latency'] = _TnpLatency.get_tnp_latency()

    def _get_monitor_data(self):
        self._summary[
            'process_monitor_output'] = _Monitor.get_process_monitor_output()
        self._summary['url_status'] = _Monitor.get_url_status()
        self._summary[
            'service_stability'] = _ServiceStability.get_service_stability()


class Presenter:

    def __init__(self, data):
        self._data = data

    def present(self):
        return [self._data]


class _ServiceStability:

    @staticmethod
    def get_service_stability():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/serviceStability/ServiceStability_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_today_date().strftime('%Y-%m-%d')))
        if file:
            return _ServiceStability._get_latest_services_info(file)
        else:
            return ['File not present or is empty']

    @staticmethod
    def _get_latest_services_info(file):
        rows = FileUtils.readlines(file)
        search_str = rows[-1].split(',')[0]
        required_rows = []
        for row in rows[::-1]:
            if row.split(',')[0] == search_str:
                required_rows.append(row)
        required_rows = rows[:1] + required_rows[::-1]
        return required_rows


class _TableCountUsage:

    @staticmethod
    def get_table_count_usage():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/tableCount_Usage/total_count_usage_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_today_date().strftime('%Y%m%d')))
        if file:
            contents = FileUtils.readlines(file)
            contents[-1] = contents[-1].replace(',24,', ',Total,')
            return contents, round(
                (_TableCountUsage._get_total_bepd(file) / 1000000000), 2)
        else:
            return ['File not present or is empty']

    @staticmethod
    def _get_total_bepd(file):
        last_row = CsvUtils.read_csv_as_dict_output(file)[1][-1]
        bepd_mobile_and_fixed = int(
            last_row['sum_of_mobile_usage_row_values']) + int(
            last_row['sum_of_fixed_line_usage_row_values'])
        return bepd_mobile_and_fixed


class _JobCount:

    @staticmethod
    def get_fifteen_min_job_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_15Min/15MinJobsSummary_{1}_Allhours.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_yesterday_datetime().strftime("%Y-%m-%d")))
        return _JobCount._get_list_of_rows(file)

    @staticmethod
    def get_hour_job_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_Hour/hourSummary_{1}_Allhours.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_yesterday_datetime().strftime('%Y-%m-%d')))
        return _JobCount._get_list_of_rows(file)

    @staticmethod
    def get_day_job_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_Day/daySummary_{1}_Allday.csv'.format(
                LOG_PATH, DateTimeUtil.get_today_date()))
        return _JobCount._get_list_of_rows(file)

    @staticmethod
    def get_week_job_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_Week/weekSummary_{1}_AllWeek.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_week_start_date().strftime('%Y-%m-%d')))
        return _JobCount._get_list_of_rows(file)

    @staticmethod
    def _get_list_of_rows(file):
        if file:
            rows = FileUtils.readlines(file)
            for index, row in enumerate(rows):
                if index != 0:
                    _JobCount._add_color_code(rows, row, index)
            return rows
        else:
            return ['File not present or is empty']

    @staticmethod
    def _add_color_code(rows, row, index):
        row_data = row.split(',')
        if int(row_data[5]) > int(_Thresholds.get_threshold('JobCount')):
            row_data[5] += 'Red'
        rows[index] = ','.join(row_data)


class _TableCount:

    @staticmethod
    def get_day_table_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/tableCount_Day/dayTableCount_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime('%Y-%m-%d')))
        if file:
            return FileUtils.readlines(file)
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_week_table_count():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/tableCount_Week/weekTableCount_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_week_start_date().strftime('%Y-%m-%d')))
        if file:
            return FileUtils.readlines(file)
        else:
            return ['File not present or is empty']


class _TnpLatency:

    @staticmethod
    def get_tnp_latency():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/Tnp_Latency/TnpJobsLatency_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_yesterday_datetime().strftime("%Y-%m-%d")))
        if file:
            data = FileUtils.readlines(file)
            return _TnpLatency._get_final_data(data)
        else:
            return ['File not present or is empty']

    @staticmethod
    def _get_final_data(data):
        final_data = []
        for row in data:
            if float(row.split(',')[-1]) > 30:
                final_data.append(row)
        if final_data:
            return final_data
        else:
            return ['Overall latency is not greater than 30mins']


class _Boundary:

    @staticmethod
    def get_hour_boundary():
        file_path = _FileOperations.check_file_exists_and_not_empty(
            '{0}/boundaryStatus/HourBoundaryStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime("%Y%m%d")))
        if file_path:
            rows = FileUtils.read_file(file_path)
            required_data = rows[rows.rindex('time_frame,jobid,maxvalue'
                                             ):].splitlines()
            last_two_hours_date_time = DateTimeUtil. \
                get_last_two_hours_datetime()
            return _Boundary._get_final_boundary_data(
                required_data, last_two_hours_date_time)
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_day_boundary():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/boundaryStatus/DayBoundaryStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime("%Y%m%d")))
        if file:
            rows = FileUtils.read_file(file)
            required_data = rows[rows.rindex('time_frame,jobid,maxvalue'
                                             ):].splitlines()
            last_day_datetime = DateTimeUtil.get_yesterday_datetime().strftime(
                '%Y-%m-%d 00:00:00')

            return _Boundary._get_final_boundary_data(
                required_data,
                DateTimeUtil.parse_date(last_day_datetime,
                                        '%Y-%m-%d %H:%M:%S'))
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_week_boundary():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/boundaryStatus/WeekBoundaryStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_week_start_date().strftime("%Y%m%d")))
        if file:
            rows = FileUtils.read_file(file)
            required_data = rows[rows.rindex('time_frame,jobid,maxvalue'
                                             ):].splitlines()
            last_monday = DateTimeUtil.get_week_start_date().strftime(
                '%Y-%m-%d 00:00:00')
            return _Boundary._get_final_boundary_data(
                required_data,
                DateTimeUtil.parse_date(last_monday, '%Y-%m-%d %H:%M:%S'))
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_usage_boundary():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/boundaryStatus/UsageBoundaryStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime("%Y%m%d")))
        if file:
            rows = FileUtils.read_file(file)
            required_data = rows[rows.rindex(
                'time_frame,jobid,maxvalue,region_id'):].splitlines()
            return _Boundary._get_updated_usage_boundary_data(required_data)

        else:
            return ['File not present or is empty']

    @staticmethod
    def _get_final_boundary_data(data, time, re_agg_boundary=None):
        if re_agg_boundary:
            return data
        else:
            for index, row in enumerate(data):
                if index != 0:
                    row_data = row.split(',')
                    _Boundary._add_color_coding(data, index, row_data, time)
            return data

    @staticmethod
    def _add_color_coding(data, index, row_data, time):
        row_data = [ele.strip() for ele in row_data]
        day_boundary_adap_jobs = [
            'Usage_TT_1_LoadJob', 'Usage_BCSI_1_LoadJob',
            'Usage_BB_THRPT_1_LoadJob'
        ]
        if row_data[1] in day_boundary_adap_jobs and row_data[2] != '':
            _Boundary._update_row_data(row_data)
        elif row_data[2] == '' or DateTimeUtil.parse_date(
                row_data[2], '%Y-%m-%d %H:%M:%S') < time:

            row_data[2] += 'Red'
        data[index] = ','.join(row_data)

    @staticmethod
    def _update_row_data(row_data):
        today_date_time_str = DateTimeUtil.today().strftime(
            '%Y-%m-%d 00:00:00')
        today_date_time = DateTimeUtil.parse_date(today_date_time_str,
                                                  '%Y-%m-%d %H:%M:%S')
        output_date_time_obj = DateTimeUtil.parse_date(row_data[2].strip(),
                                                       '%Y-%m-%d %H:%M:%S')
        if output_date_time_obj < today_date_time:
            row_data[2] += 'Red'

    @staticmethod
    def _get_updated_usage_boundary_data(data):
        for row in data:
            if 'ArchivingJob' in row:
                data.remove(row)
        last_hour_datetime = DateTimeUtil.today().strftime('%Y-%m-%d %H:00:00')
        return _Boundary._get_final_boundary_data(
            data,
            DateTimeUtil.parse_date(last_hour_datetime, '%Y-%m-%d %H:%M:%S'))


class _JobStatus:

    @staticmethod
    def get_day_job_status():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_Day/dayJobsStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime("%Y-%m")))
        if file:
            rows = FileUtils.read_file(file)
            req_data = rows[rows.rindex(
                'day,job_name,start_time,end_time,executionduration,status'
            ):].splitlines()
            return _JobStatus._get_job_status_data_list(
                req_data, int(_Thresholds.get_threshold('DayJob')))
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_week_job_status():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/aggregations_Week/weekJobsStatus_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.today().strftime("%Y-%m")))
        if file:
            rows = FileUtils.read_file(file)
            req_data = rows[rows.rindex(
                'week,job_name,start_time,end_time,executionduration,status'
            ):].splitlines()
            return _JobStatus._get_job_status_data_list(
                req_data, int(_Thresholds.get_threshold('WeekJob')))
        else:
            return ['File not present or is empty']

    @staticmethod
    def _get_job_status_data_list(data, threshold):
        final_data = data[:1]
        for row in data[1:]:
            row_data = row.split(',')
            if row_data[4].strip() == '':
                row_data[4] += 'Red'
            elif row_data[4].strip() != '' and int(
                    row_data[4].strip()) > threshold:
                row_data[4] += 'Red'
            final_data.append(','.join(row_data))
        return final_data


class _Monitor:

    @staticmethod
    def get_process_monitor_output():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/processMonitor/processMonitor_{1}.csv'.format(
                LOG_PATH,
                DateTimeUtil.get_yesterday_datetime().strftime('%Y-%m-%d')))
        if file:
            return _Monitor._get_final_processed_data(file)
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_url_status():
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/checkURL/checkURL_{1}.csv'.format(
                LOG_PATH, DateTimeUtil.get_today_date()))
        if file:
            rows = FileUtils.readlines(file)[::-1]
            return _Monitor._get_updated_url_status_data(rows)
        else:
            return ['File not present or is empty']

    @staticmethod
    def get_re_agg_boundary():
        boundary_summary = {}
        time_period_list = ['15MIN', 'HOUR', 'DAY', 'WEEK']
        for time_period in time_period_list:
            boundary_summary[
                time_period] = _Monitor._get_final_re_agg_boundary_data(
                time_period)
        return boundary_summary

    @staticmethod
    def get_qs_table_level(qs_dict):
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}{1}_{2}.csv'.format(LOG_PATH, qs_dict[0],
                                    DateTimeUtil.today().strftime("%Y%m%d")))
        if file:
            return FileUtils.readlines(file)
        else:
            return ['File not found or is empty']

    @staticmethod
    def _get_final_processed_data(file):
        rows = CsvUtils.read_csv(file)
        final_data = []
        for row in rows:
            final_data.append(','.join(row[0:len(row):3]))
        return final_data

    @staticmethod
    def _get_updated_url_status_data(rows):
        last_hour_data = []
        for row in rows:
            last_hour_data.append(row)
            if row == 'Date,Component,Status':
                break
        last_hour_data = last_hour_data[::-1]
        return _Monitor._add_color_code(last_hour_data)

    @staticmethod
    def _add_color_code(last_hour_data):
        final_data = last_hour_data[:1]
        for row in last_hour_data[1:]:
            row_data = row.split(',')
            if row_data[2].strip() in ('DOWN', 'IP not configured'):
                row_data[2] += 'Red'
            final_data.append(','.join(row_data))
        return final_data

    @staticmethod
    def _get_final_re_agg_boundary_data(time_period):
        file = _FileOperations.check_file_exists_and_not_empty(
            '{0}/boundaryStatus/ReaggBoundaryStatus_{1}_{2}.csv'.format(
                LOG_PATH, time_period,
                DateTimeUtil.today().strftime("%Y%m%d")))
        if file:
            rows = FileUtils.read_file(file)
            required_data = rows[rows.rindex(
                'time_frame,jobname,status,maxvalue'):].splitlines()
            last_hour_datetime = DateTimeUtil.get_last_hour_date(
                '%Y-%m-%d %H:00:00')
            return _Boundary._get_final_boundary_data(
                required_data,
                DateTimeUtil.parse_date(last_hour_datetime,
                                        '%Y-%m-%d %H:%M:%S'), "Reagg")
        else:
            return ['File not present or is empty']


class _FileOperations:

    @staticmethod
    def get_csv_file_from_dir(directory):
        files = FileUtils.get_files_by_modtime(directory)
        csv_files = [file for file in files if str(file).endswith('.csv')]
        if csv_files:
            return csv_files[0]

    @staticmethod
    def check_file_exists_and_not_empty(file):
        if FileUtils.exists(file) and FileUtils.get_line_count(file) > 1:
            return True


class _Thresholds:

    @staticmethod
    def get_threshold(type):
        return SpecificationUtil.get_property('SUMMARYTHRESHOLDS', type,
                                              'value')

    @staticmethod
    def get_backlog_threshold(type):
        return SpecificationUtil.get_property('BACKLOG', type,
                                              'Thresholdlimit')


if __name__ == '__main__':
    main()

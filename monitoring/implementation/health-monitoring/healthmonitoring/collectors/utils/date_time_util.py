from datetime import timedelta, datetime, date, time as dtime
import time


class DateTimeUtil:
    DATE_FORMAT = '%d-%m-%Y %H:%M:%S'
    PARTITION_DATE_FORMAT = '%d.%m.%Y %H:%M:%S'
    DATE_TRUNC_BY_HOUR_FORMAT = '%Y-%m-%d %H:00:00'
    POSTGRES_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    DATE_TRUNC_TILL_HOUR_FORMAT = '%Y-%m-%d %H'

    @staticmethod
    def get_date_from_epoch(epoch_time, format=DATE_FORMAT):
        return datetime.strptime(
            datetime.fromtimestamp(epoch_time / 1000).strftime(format), format)

    @staticmethod
    def parse_date(string_date, format=DATE_FORMAT):
        return datetime.strptime(string_date, format)

    @staticmethod
    def to_date_string(date, format=DATE_FORMAT):
        return date.strftime(format) if date else ''

    @staticmethod
    def get_date_till_hour():
        return (DateTimeUtil.now()).strftime(
            DateTimeUtil.DATE_TRUNC_TILL_HOUR_FORMAT)

    @staticmethod
    def get_last_15min_date():
        return (DateTimeUtil.now() - timedelta(minutes=15)).strftime(
            DateTimeUtil.POSTGRES_DATE_FORMAT)

    @staticmethod
    def get_last_hour_date(format=None):
        if format:
            return (DateTimeUtil.now() - timedelta(hours=1)).strftime(format)
        return (DateTimeUtil.now() - timedelta(hours=1)).strftime(
            DateTimeUtil.POSTGRES_DATE_FORMAT)

    @staticmethod
    def get_curr_min_date():
        return (datetime.combine(DateTimeUtil.now(), dtime.min)).strftime(
            DateTimeUtil.POSTGRES_DATE_FORMAT)

    @staticmethod
    def get_curr_max_date():
        return (datetime.combine(DateTimeUtil.now(), dtime.max)).strftime(
            DateTimeUtil.POSTGRES_DATE_FORMAT)

    @staticmethod
    def get_current_hour_date():
        return (DateTimeUtil.now()).strftime(DateTimeUtil.POSTGRES_DATE_FORMAT)

    @staticmethod
    def get_difference_in_minutes(start_time, end_time):
        start_time_unix_format = time.mktime(start_time.timetuple())
        end_time_unix_format = time.mktime(end_time.timetuple())
        return DateTimeUtil._convert_seconds_to_minutes(
            start_time_unix_format, end_time_unix_format)

    @staticmethod
    def get_partition_info(partition):
        partition_type = partition.upper()
        if partition_type == 'DAY':
            date_info, start_partition, end_partition = \
                DateTimeUtil._get_day_start_end_partitions()
        elif partition_type == 'WEEK':
            date_info, start_partition, end_partition = \
                DateTimeUtil._get_week_start_end_partition()
        elif partition_type == 'MONTH':
            date_info, start_partition, end_partition = \
                DateTimeUtil._get_month_start_end_partition()
        return (date_info.strftime("%Y-%m-%d %H-%M-%S"), start_partition,
                end_partition)

    @staticmethod
    def get_date_in_epoch(date):
        return (int(time.mktime(time.strptime(str(date), '%Y-%m-%d')))) * 1000

    @staticmethod
    def _convert_seconds_to_minutes(start_time_unix_format,
                                    end_time_unix_format):
        return int(end_time_unix_format - start_time_unix_format) / 60

    @staticmethod
    def _get_day_start_end_partitions(pattern=PARTITION_DATE_FORMAT):
        date_info = DateTimeUtil.today() - timedelta(1)
        start = date_info.strftime('%d.%m.%Y 00:00:00')
        end = date_info.strftime('%d.%m.%Y 23:59:59')
        start_partition = int(time.mktime(time.strptime(start,
                                                        pattern))) * 1000
        end_partition = int(time.mktime(time.strptime(end, pattern))) * 1000
        return date_info, start_partition, end_partition

    @staticmethod
    def _get_week_start_end_partition(pattern=PARTITION_DATE_FORMAT):
        today = DateTimeUtil.today()
        weekday = today.weekday()
        start_delta = timedelta(days=weekday, weeks=1)
        date_info = today - start_delta
        start = date_info.strftime('%d.%m.%Y 00:00:00')
        start_partition = int(time.mktime(time.strptime(start,
                                                        pattern))) * 1000
        end_partition = start_partition
        return date_info, start_partition, end_partition

    @staticmethod
    def _get_month_start_end_partition(pattern=PARTITION_DATE_FORMAT):
        today = DateTimeUtil.today()
        first = today.replace(day=1)
        last_month_end = first - timedelta(days=1)
        date_info = last_month_end.replace(day=1)
        prev_last = last_month_end.strftime('%d.%m.%Y 23:59:59')
        prev_first = date_info.strftime('%d.%m.%Y 00:00:00')
        start_partition = int(time.mktime(time.strptime(prev_first,
                                                        pattern))) * 1000
        end_partition = int(time.mktime(time.strptime(prev_last,
                                                      pattern))) * 1000
        return date_info, start_partition, end_partition

    @staticmethod
    def get_last_hour():
        return (DateTimeUtil.now() - timedelta(hours=1)).hour

    @staticmethod
    def get_current_hour():
        return DateTimeUtil.now().hour

    @staticmethod
    def get_previous_day():
        return (date.today() - timedelta(days=1)).day

    @staticmethod
    def get_week_start_day():
        week_day = date.today().weekday()
        if week_day == 0:
            return (date.today() - timedelta(days=week_day + 7)).day
        else:
            return (date.today() - timedelta(days=week_day)).day

    @staticmethod
    def get_week_end_day():
        return (date.today() - timedelta(days=date.today().weekday() + 1)).day

    @staticmethod
    def get_week_start_date():
        return DateTimeUtil.now() - timedelta(
            days=DateTimeUtil.today().weekday())

    @staticmethod
    def get_last_two_hours_datetime():
        result = DateTimeUtil.now() - timedelta(hours=2)
        result = result.replace(minute=0, second=0, microsecond=0)
        return result

    @staticmethod
    def now():
        return datetime.now()

    @staticmethod
    def today():
        return datetime.today()

    @staticmethod
    def get_today_date():
        return date.today()

    @staticmethod
    def get_yesterday_datetime():
        return DateTimeUtil.today() - timedelta(days=1)

    @staticmethod
    def get_date_with_zero_time():
        return DateTimeUtil.now().strftime("%Y-%m-%d 00:00:00")

    @staticmethod
    def get_date_with_last_min_time():
        return DateTimeUtil.now().strftime("%Y-%m-%d 23:59:59")

import time
from datetime import timedelta, datetime


class DateTimeUtil:

    DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

    def required_time_info(self, call_type):
        if call_type == 'day':
            date_info = datetime.today() - timedelta(1)
            start = date_info.strftime('%d.%m.%Y 00:00:00')
            end = date_info.strftime('%d.%m.%Y 23:59:59')
            pattern = '%d.%m.%Y %H:%M:%S'
            start_partition = int(time.mktime(time.strptime(start, pattern))) * 1000
            end_partition = int(time.mktime(time.strptime(end, pattern))) * 1000
        elif call_type == 'week':
            today = datetime.today()
            weekday = today.weekday()
            start_delta = timedelta(days=weekday, weeks=1)
            date_info = today - start_delta
            start = date_info.strftime('%d.%m.%Y 00:00:00')
            pattern = '%d.%m.%Y %H:%M:%S'
            start_partition = int(time.mktime(time.strptime(start, pattern))) * 1000
            end_partition = start_partition
        elif call_type == 'month':
            today = datetime.today()
            first = today.replace(day=1)
            last_month_end = first - timedelta(days=1)
            date_info = last_month_end.replace(day=1)
            prev_last = last_month_end.strftime('%d.%m.%Y 23:59:59')
            prev_first = date_info.strftime('%d.%m.%Y 00:00:00')
            pattern = '%d.%m.%Y %H:%M:%S'
            start_partition = int(time.mktime(time.strptime(prev_first, pattern))) * 1000
            end_partition = int(time.mktime(time.strptime(prev_last, pattern))) * 1000
        return (date_info.strftime("%Y-%m-%d %H-%M-%S"), start_partition, end_partition)

    def get_date_in_epoch(self, date):
        return (int(time.mktime(time.strptime(str(date), '%Y-%m-%d'))))
    
    def get_yesterday_date_time(self):
        return (datetime.today() - timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
    
    def get_yesterday_in_epoch(self, hour=0):
        ysd = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        return int(time.mktime(time.strptime(ysd, '%Y-%m-%d')) + (hour * 3600))

    @staticmethod
    def get_date_from_epoch(epoch_time, format=DATE_FORMAT):
        return datetime.strptime(datetime.fromtimestamp(epoch_time / 1000).strftime(
                format), format)

    @staticmethod
    def parse_date(string_date, format=DATE_FORMAT):
        return datetime.strptime(string_date, format)

    @staticmethod
    def to_date_string(date, format=DATE_FORMAT):
        return date.strftime(format)

    @staticmethod
    def _convert_seconds_to_minutes(start_time_unix_format, end_time_unix_format):
        return int(end_time_unix_format - start_time_unix_format) / 60

    @staticmethod
    def get_difference_in_minutes(start_time, end_time):
        start_time_unix_format = time.mktime(start_time.timetuple())
        end_time_unix_format = time.mktime(end_time.timetuple())
        return DateTimeUtil._convert_seconds_to_minutes(
            start_time_unix_format, end_time_unix_format)

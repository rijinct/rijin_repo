from datetime import date, timedelta, datetime as dt, timezone
import datetime, time, os, calendar

from dateutil import tz


class DateTimeUtil:
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def requiredTimeInfo(self,calltype):

        if calltype=='day':
            dateinfo = datetime.datetime.today() - timedelta(1)
            start = dateinfo.strftime('%d.%m.%Y 00:00:00')
            end = dateinfo.strftime('%d.%m.%Y 23:59:59')
            pattern = '%d.%m.%Y %H:%M:%S'
            startpartition = int(time.mktime(time.strptime(start, pattern)))*1000
            endpartition = int(time.mktime(time.strptime(end, pattern)))*1000
        elif calltype=='week':
            today = datetime.datetime.today()
            weekday = today.weekday()
            start_delta = datetime.timedelta(days=weekday, weeks=1)
            dateinfo = today - start_delta
            start = dateinfo.strftime('%d.%m.%Y 00:00:00')
            pattern = '%d.%m.%Y %H:%M:%S'
            startpartition = int(time.mktime(time.strptime(start, pattern)))*1000
            endpartition=startpartition
        elif calltype=='month':
            today = datetime.datetime.today()
            first = today.replace(day=1)
            lastmonthend = first - datetime.timedelta(days=1)
            dateinfo = lastmonthend.replace(day=1)
            prev_last = lastmonthend.strftime('%d.%m.%Y 23:59:59')
            prev_first = dateinfo.strftime('%d.%m.%Y 00:00:00')
            pattern = '%d.%m.%Y %H:%M:%S'
            startpartition = int(time.mktime(time.strptime(prev_first, pattern)))*1000
            endpartition = int(time.mktime(time.strptime(prev_last, pattern)))*1000

        return (dateinfo.strftime("%Y-%m-%d %H-%M-%S"),startpartition,endpartition)

    def getDateInEpoch(self,date):
        return(int(time.mktime(time.strptime(str(date), '%Y-%m-%d'))))

    def get_yesterday_in_epoch(self, hour=0):
        ysd = (datetime.datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        return int(time.mktime(time.strptime(ysd, '%Y-%m-%d')) + (hour * 3600))

    def convertUtcToLocalTime(self, utc_time):
        utc_tz=tz.gettz('UTC')
        local_tz=tz.gettz(os.environ['LOCAL_TZ'])
        utc_time_stamp = utc_time.replace(tzinfo=utc_tz)
        local_time_stamp =  utc_time_stamp.astimezone(local_tz)
        return local_time_stamp.strftime('%Y-%m-%d %H:%M:%S')

    def get_epoch_time_of_past_days(self, no_of_days):
        return int(time.time() - datetime.timedelta(no_of_days).total_seconds())

    @staticmethod
    def get_required_start_date(type):
        today = datetime.date.today()
        if type == 'day':
            time = today
        elif type == 'week':
            time = today - timedelta(days=today.weekday())
        elif type == 'month':
            time = today.replace(day=1)
        return time.strftime('%Y-%m-%d')

    def get_start_and_end_of_last_week(self):
        date = (datetime.datetime.today() - timedelta(7)).strftime("%Y-%m-%d")
        date_obj = dt.strptime(date, '%Y-%m-%d')
        start_of_week = date_obj - timedelta(days=date_obj.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        return self.getDateInEpoch(start_of_week.strftime('%Y-%m-%d'))*1000,self.getDateInEpoch(end_of_week.strftime('%Y-%m-%d'))*1000

    def get_start_and_end_of_current_month(self):
        last_day_month = date.today().replace(day=calendar.monthrange(date.today().year, date.today().month)[1])
        today = datetime.datetime.today()
        first_day_month = today.replace(day=1)
        return self.getDateInEpoch(first_day_month.strftime('%Y-%m-%d'))*1000,self.getDateInEpoch(last_day_month.strftime('%Y-%m-%d'))*1000

    @staticmethod
    def now():
        return dt.now()

    @staticmethod
    def get_date_with_zero_time():
        return DateTimeUtil.now().strftime("%Y-%m-%d 00:00:00")

    @staticmethod
    def get_last_15min_date():
        return (DateTimeUtil.now() - timedelta(minutes=15)).strftime(DateTimeUtil.DATE_FORMAT)
		
    @staticmethod
    def get_last_hour_date(format=None):
        if format:
            return (DateTimeUtil.now() - timedelta(hours=1)).strftime(format)
        return (DateTimeUtil.now() - timedelta(hours=1)).strftime(DateTimeUtil.DATE_FORMAT) 
    
    @staticmethod
    def get_utc_time():
        current_time = dt.now()
        utc_time = current_time.replace(tzinfo=timezone.utc)
        return utc_time.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def get_date_from_epoch(epoch_time, format=DATE_FORMAT):
        return dt.strptime(
            dt.fromtimestamp(epoch_time / 1000).strftime(format), format)
    
    @staticmethod
    def get_current_utc_time_stamp(format=DATE_FORMAT):
        right_now = time.time()
        cur_date_time = datetime.datetime.utcfromtimestamp(right_now). \
            strftime(format)
        return cur_date_time

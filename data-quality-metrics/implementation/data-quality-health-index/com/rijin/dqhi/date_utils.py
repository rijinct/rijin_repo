import time
from datetime import date, timedelta, datetime


def get_lower_upper_bound(computation_date):
    yesterday = datetime.strptime(computation_date, '%Y-%m-%d')
    start = yesterday.strftime('%Y-%m-%d %H:%M:%S')
    end = yesterday.strftime('%Y-%m-%d 23:59:59')
    pattern = '%Y-%m-%d %H:%M:%S'
    start_partition = int(time.mktime(time.strptime(start, pattern))) * 1000
    end_partition = int(time.mktime(time.strptime(end, pattern))) * 1000
    return {"LB":start_partition, "UB":end_partition}


def get_previous_date():
    today = date.today()
    return today - timedelta(days=1)


def get_current_date():
    return date.today()


def get_current_date_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    computation_date = "2019-12-15"
    print(get_lower_upper_bound(computation_date))

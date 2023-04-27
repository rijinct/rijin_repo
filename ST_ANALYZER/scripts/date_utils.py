import time
from datetime import date, timedelta


def get_lower_upper_bound():
    yesterday = date.today() - timedelta(1)
    start = yesterday.strftime('%Y-%m-%d %H:%M:%S')
    end = yesterday.strftime('%Y-%m-%d 23:59:59')
    pattern = '%Y-%m-%d %H:%M:%S'
    start_partition = int(time.mktime(time.strptime(start, pattern))) * 1000
    end_partition = int(time.mktime(time.strptime(end, pattern))) * 1000
    return {"LB":start_partition, "UB":end_partition}


def get_previous_date():
    today = date.today()
    return today - timedelta(days=1)


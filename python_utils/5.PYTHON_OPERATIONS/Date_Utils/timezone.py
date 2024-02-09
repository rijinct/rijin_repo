from datetime import datetime
import pytz


def get_all_timezones():
    for timeZone in pytz.all_timezones:
        print('All available Timezones are:')
        print(timeZone)

def get_timezone_for_region(region):
    ###US/Central or CET or Australia/Sydney or America/Newyork
    return datetime.now(pytz.timezone(region)) 


if __name__ == "__main__":
    print('US Central time is ', str(get_timezone_for_region('US/Central').hour))
    print('Germany Time is ', get_timezone_for_region('CET').hour)
    print('Sydney time is ',get_timezone_for_region('Australia/Sydney').hour)
    print('Newyork time is ',get_timezone_for_region('America/New_York').hour)
    print('UTC time is ',get_timezone_for_region('UTC')+1)


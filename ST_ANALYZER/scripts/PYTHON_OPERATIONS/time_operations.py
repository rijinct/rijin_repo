'''
Created on 18-May-2020

@author: rithomas
'''
import datetime

def get_current_date_without_timeStamp():
    return datetime.date.today()
    
def get_current_date_with_timeStamp(): 
    return datetime.datetime.now()

def get_formatted_date(date,format_pattern):
    return date.strftime(format_pattern)

def get_earlier_date(delta):
    days = datetime.timedelta(delta)
    new_date = get_current_date_without_timeStamp() - days
    return new_date


def timeround10(dt):
    a, b = divmod(round(dt.minute, -1), 60)
    return '%i:%02i' % ((dt.hour + a) % 24, b)

if __name__ == '__main__':
    print(get_current_date_without_timeStamp())
    
    print(get_current_date_with_timeStamp())
    
    current_date = get_current_date_with_timeStamp()
    print(get_formatted_date(current_date, '%d-%m-%Y %H:%M:%S'))
    
    print(timeround10(datetime.datetime(2010, 1, 1, 12, 51, 0)))
    
    print(get_earlier_date(7))
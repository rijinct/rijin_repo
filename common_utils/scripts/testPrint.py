import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP



def get_formattedCurrentTimeMin():
    current_date = PY_OP.get_current_date_with_timeStamp()
    formatted_date = PY_OP.get_formatted_date(current_date, '%Y,%m,%d,%H,%M,%S')
    M = PY_OP.get_formatted_date(current_date, '%M')
    return M

currentTime_min=get_formattedCurrentTimeMin()
currentTime_min_list = list(map(int, str(currentTime_min))) 
currentTime_minLastElm = currentTime_min_list.pop()
print(currentTime_minLastElm)

if (currentTime_minLastElm==7):
    print('succes')
import pandas as pd

def get_dict():
    dicts = {}
    keys = range(4)
    values = ["Hi", "I", "am", "John"]
    list = []
    for i in keys:
        dicts[i] = values[i]
        list.append(values[i])
    return dicts,list

print( get_dict())
##can print
print( get_dict()[0])
i = get_dict()
values = i[1]
print(values)


#print(dicts.values[2])

#df = pd.DataFrame(dicts.items(), columns=['Date','DateValue'])
#print(dicts.items()[0])

#######
values = [['us_sms_1', 'report_time'], ['us_sms_1', 'event_time'], ['us_sms_1', 'load_time'], ['us_sms_1', 'imsi'], ['us_sms_1', 'msisdn'], ['us_sms_1', 'imei'], ['us_sms_1', 'mcc']]
check = ('us_sms_1', 'report_time')
if check  in values:
    print('succ')
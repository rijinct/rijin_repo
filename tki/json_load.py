import json


f = open('/Users/rthomas/Desktop/Rijin/rijin_git/tki/conf/qc-query-result.json')

instr_lst = []
for val in f:
    data_dict = json.loads(val)
    instr_lst.append(data_dict['instrument_name'])
    print(data_dict['instrument_name'])

print(set(instr_lst))
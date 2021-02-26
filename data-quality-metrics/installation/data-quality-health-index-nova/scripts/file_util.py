'''
Created on 12-Nov-2020

@author: rithomas
'''
import json
import sys
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
import constants


def write_list_to_file(hive_tables, file):
    filename = '{c}/{f}'.format(c=constants.DQHI_CONF, f=file)
    with open(filename, 'w') as f:
        for item in hive_tables:
            f.write("%s\n" % item)

def write_tuple_to_file(tuple_list, file):
    filename = '{c}/{f}'.format(c=constants.DQHI_CONF, f=file)
    with open(filename, 'w') as fp:
        fp.write('\n'.join('%s,%s' % x for x in tuple_list))

def write_dict_to_file(table_column_list, file):
    filename = '{c}/{f}'.format(c=constants.DQHI_CONF, f=file)
    with open(filename, 'w') as file:
        file.write(json.dumps(table_column_list))

        
def get_dict_properties(filename):
    with open(filename) as f:
        lines = f.read()
    f.close()
    lines_dict = json.loads(lines) 
    return lines_dict

def get_list_properties(filename):
    with open(filename) as f:
        lines = f.read().splitlines()
    return lines

def get_tuple_properties(filename):
    with open(filename) as f:
        lines = f.read()
    f.close()
    d = dict(x.split(",") for x in lines.split("\n"))
    result = []
    for key, value in d.items():
        result.append((key,value))
    return result
  

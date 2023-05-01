'''
Created on 17-Mar-2020

@author: rithomas

'''

import st_constants


def get_table_filter_condition():
    pattern_constructor = ''
    for table in st_constants.TABLE_PATTERN:
        table_condition = "job_prop.paramvalue like ''%{}%''".format(table)
        if (len(pattern_constructor) == 0):
            pattern_constructor = table_condition
        else:
            pattern_constructor = pattern_constructor + ' ' + 'or' + ' ' + table_condition
    return st_constants.CONDITION_CONSTRUCTOR.format(pattern_constructor)


print(get_table_filter_condition())


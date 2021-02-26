from datetime import datetime
import sys
import constants
import common_constants
import pandas as pd
import os
from dq_logging_util import loggerUtil
if os.getenv('IS_K8S') == 'true' and not os.getenv('HOSTNAME').startswith("cdlk"):
    from logger_util import *


def _get_application_host(name):
    statement = "out = {}.split()".format(name)
    exec(statement, globals(), locals())
    return locals()['out']


def get_as_dictionary(values):
    response_dictionary = {}
    for value in values:
        key, value = value.split(",")
        response_dictionary[key] = dictionaryElements = response_dictionary.get(key, [])
        dictionaryElements.append(value)
    return response_dictionary


def get_logger():
    currentDate = datetime.now().strftime("%Y-%m-%d")
    logFile = constants.LOG_FILE_NAME.format(currentDate)
    LOGGER = loggerUtil.__call__(common_constants.STREAMING_CONSOLE, common_constants.LOG_LEVEL).get_logger(logFile)
    return LOGGER

def get_logger_nova():
    create_logger()
    LOGGER = Logger.getLogger(__name__)
    LOGGER.setLevel(get_logging_instance(common_constants.LOG_LEVEL))
    return LOGGER

def get_dtype(df, colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def get_df_merged_result(df1, df2, join_condition, column_list):
    return pd.merge(df1, df2, how=join_condition , left_on=column_list, right_on=column_list)


def check_for_index(df, table_name, column_name):
    ret = (table_name, column_name)
    index_list = list(df.index)
    is_index_present = False;
    for index in index_list:
        if(index == ret):
            is_index_present = True
            break
    return is_index_present


def get_column_informtion(df, dimension, table_name, column_name):
    dimension = dimension.lower()
    rtv = None
    table_name = table_name.lower()
    column_name = column_name.lower()
    rule_name = dimension + "_rule_name"
    rule_parameter = dimension + "_rule_parameter"
    rule_threshold = dimension + "_threshold"
    rule_weight = dimension + "_weight"
    
    custom_expression = dimension + "_custom_expression"
    
    is_index_present = check_for_index(df, table_name, column_name)
    if is_index_present and df.loc[table_name, column_name][rule_name] != "NULL_IN_SOURCE":
        rtv = (df.loc[table_name, column_name][rule_name].strip(),
           df.loc[table_name, column_name][rule_parameter],
           df.loc[table_name, column_name][rule_threshold],
           df.loc[table_name, column_name][rule_weight],
           df.loc[table_name, column_name][custom_expression]
           )
    return rtv


def is_check_pass(threshold, total_records, bad_records):
    check_pass = "N"
    allowed_threshold_records = (threshold / 100) * total_records
    if total_records == 0 :
        check_pass = "NA"
    elif bad_records <= allowed_threshold_records:
        check_pass = "Y"
    return check_pass


def get_custom_default_filename(custom_kpi_cde_file, product_kpi_cde_file):
    if os.path.exists(custom_kpi_cde_file):
        file = custom_kpi_cde_file
    else:
        file = product_kpi_cde_file
    return file

    
def pre_validate_excel(df, table_col_list):
    LOGGER = get_logger()
    index_list = list(df.index)
    index_list_conv = list(map(list, index_list)) 
    is_index_present = False;
    for index in index_list_conv:
        if index not in table_col_list:
            LOGGER.error("Entered value {} in DQ_RULES_AND_FIELD_DEFINITIONS.xlsx is not present in the DB. Hence, exiting. Correct the excel and re-run.".format(index))
            sys.exit(1)


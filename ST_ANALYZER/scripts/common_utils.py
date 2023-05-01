from datetime import datetime
import sys
import st_constants
import pandas as pd
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from loggerUtil import loggerUtil


def get_as_dictionary(values):
    response_dictionary = {}
    for value in values:
        key, value = value.split(",")
        response_dictionary[key] = dictionaryElements = response_dictionary.get(key, [])
        dictionaryElements.append(value)
    return response_dictionary


def get_logger():
    currentDate = datetime.now().strftime("%Y-%m-%d")
    logFile = st_constants.LOG_FILE_NAME.format(currentDate)
    LOGGER = loggerUtil.__call__().get_logger(logFile)
    return LOGGER


def get_dtype(df, colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def get_df_merged_result(df1, df2, join_condition, column_list):
    return pd.merge(df1, df2, how=join_condition , on=column_list)

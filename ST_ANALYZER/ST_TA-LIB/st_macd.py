'''
Created on 10-Jun-2020

@author: rithomas
'''
import numpy as np
from talib.abstract import *
import talib
from nsepy import get_history
from nsepy import get_quote
import pandas as pd
from datetime import date
import pandas as pd
import ta
from sqlalchemy import create_engine
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df
import DF_OUTPUT_WRITE_OPERATIONS.various_write_operations
import PYTHON_OPERATIONS.file_operations
import DF_SQL_OPERATIONS.common_sql_operations
import DF_OPERATIONS.common_df_operations
import superTrend 
import atr
import ST_ANALYZER_NSEPY.st_common_constants as comm_constants
#from talib._ta_lib import MACDFIX

###Using DailyClose
MARIADB_URL = 'mysql+mysqlconnector://root:zaq12wsx@127.0.0.1/testrijdb'

day_query="""
SELECT * FROM st_analysis_5 WHERE Trading_symbol LIKE '%SBIN%' and STR_TO_DATE(LTT, '%d-%m-%Y') > '2020-07-12' ORDER BY STR_TO_DATE(LTT, '%d-%m-%Y %H:%i:%s') ASC
"""
print(day_query)
engine = create_engine(MARIADB_URL)
df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(day_query, engine)
df_from_sql['LTP'] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_from_sql, ['LTP'])
print(df_from_sql.columns)
macd, macdsignal, macdhist = MACD(df_from_sql['LTP'],fastperiod=12, slowperiod=26, signalperiod=9) 
#atr = ATR(df_from_sql['LTP'], df_from_sql['LTP'], df_from_sql['LTP'], timeperiod=14)
#print(atr)
#exit(0)
df_from_sql[comm_constants.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_from_sql,comm_constants.st_numeric_cols)
df = superTrend.ST(df_from_sql,3,10)
print(df[['LTT','LTP','Upper Band','Lower Band','SuperTrend']])
exit(0)
df_macd = pd.DataFrame(macd,columns=['macd'])
print(df_macd)
df_macd['macdhist'] = pd.DataFrame(macdhist,columns=['macdhist'])
df_macd['macdsignal'] = pd.DataFrame(macdsignal,columns=['macdsignal'])
df_macd['atr'] = pd.DataFrame(atr,columns=['atr'])
df_macd['ltt'] = df_from_sql['LTT'] 
print(df_macd)
exit(0)
print('--------Rij-----------')
print(macdsignal)
print('--------Rij-----------')
print(macdhist)
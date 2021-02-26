'''
Created on 28-May-2020

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
#from talib._ta_lib import MACDFIX


def calculate_ema(ltp, multiplier, ema_y):
    ema_today = (ltp * multiplier) + (ema_y * (1 - multiplier)) 
    return ema_today


trading_symbol_fut = get_history("AXISBANK", date(2020,4,1), date(2020,6,6),futures=False)
#trading_symbol_fut.set_index('Date', inplace=True)
trading_symbol_curr_quote = get_quote("SBIN", "EQ")
print(trading_symbol_fut.columns)
print(trading_symbol_curr_quote)
exit(0)
trading_symbol_fut.columns = ['symbol', 'series', 'prev close', 'open', 'high', 'low', 'last',
       'close', 'VWAP', 'volume', 'turnover', 'trades', 'deliverable volume',
       '%Deliverble']
print(trading_symbol_fut[['open', 'high', 'low','close', 'VWAP', 'volume']])
inputs=trading_symbol_fut
#print(inputs[])
macd, macdsignal, macdhist = MACD(inputs['close'],fastperiod=12, slowperiod=26, signalperiod=9)
#print(macd)
##print('--------------')
#print(macdsignal)
#exit(0)

#########GIVes Incorrect
#print(macd)
#print('----------------------USING MACD-----------------------------')
#print(macdsignal)
#print('----------------------------------------------------')
#print(macdhist)
print('-----------------------------------------------')
macd = ta.trend.MACD(inputs['close'], 26, 12, 9, False).macd()
print(macd)
exit(0)
#########GIVes Incorrect
macd, macdsignal, macdhist = MACDFIX(inputs['close'],9)
print(macd,'rij',macdsignal)
#########GIVes Incorrect
print('-----------------------------------------------')
ema_12 = ta.trend.ema_indicator(inputs['close'], 12, True)
ema_26 = ta.trend.ema(inputs['close'], 26, True)
print(ema_12)
#exit(0)
print('----------------')
print(ema_26)

#########GIVes Incorrect
print('-----------------------------------------------')
ema_12 = EMA(inputs['close'],12)
print(ema_12)

###using evm

inputs['ewm'] = inputs['close'].ewm(span=12,min_periods=0,adjust=True,ignore_na=False).mean()
inputs = inputs.sort_index()
#print(inputs['ewm'])

###Using DailyClose
MARIADB_URL = 'mysql+mysqlconnector://root:zaq12wsx@127.0.0.1/testrijdb'

day_query="""
SELECT LTP FROM st_analysis WHERE Trading_symbol LIKE '%AXI%' and LTT LIKE '01-06%' ORDER BY LTT ASC
"""
engine = create_engine(MARIADB_URL)
df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(day_query, engine)
df_from_sql = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_from_sql, ['LTP'])
print(df_from_sql)
macd, macdsignal, macdhist = MACD(df_from_sql['LTP'],fastperiod=12, slowperiod=26, signalperiod=9) 
print(macd)
print('--------Rij-----------')
print(macdsignal)

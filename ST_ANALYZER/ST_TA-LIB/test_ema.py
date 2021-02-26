'''
Created on 02-Jun-2020

@author: rithomas
'''
import pandas as pd
from talib.abstract import *
from sqlalchemy import create_engine
from df_db_util import dfDbUtil
import st_constants
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df
engine = create_engine(st_constants.MARIADB_URL)

day_query = '''
SELECT LTT,ATP FROM st_analysis WHERE Trading_symbol LIKE '%SBIN%' and LTT LIKE '08-06%' ORDER BY LTT ASC
'''
df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(day_query, engine)
print(df_from_sql)
close=df_from_sql['ATP']
print(EMA(close,12))
#print(EMA(close,26))


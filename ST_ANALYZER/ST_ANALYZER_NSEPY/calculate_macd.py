'''
Created on 18-Jul-2020

@author: rithomas
'''
from sqlalchemy import create_engine
import DF_SQL_OPERATIONS.common_sql_operations as COMM_OP
import DF_INPUT_READER_OPERATIONS as DF_OP
import DF_OPERATIONS
import st_common_constants as COMM_CONSTANTS
import PYTHON_OPERATIONS.time_operations as TIME_OP
from talib.abstract import *
engine = create_engine(COMM_CONSTANTS.MARIADB_URL)



def get_final_df_from_sql(sql_query):
    df_from_sql = DF_OP.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql

def get_macd_for_day(df):
    macd, macdsignal, macdhist = MACD(df['LTP'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['macd_day'] = macd
    df['macdsignal_day'] = macdsignal
    df['macdhist_day'] = macdhist
    return df

def get_macd_for_df(df):
    df['macd'] = ''
    df['macdsignal'] = '' 
    df['macdhist'] = ''
    for idx,row in df.iterrows():
        ltp =df['LTP'][idx]
        ltt =df['LTT'][idx]  
        trading_symbol = df['Trading_symbol'][idx]
        st_macd_superT_query = COMM_CONSTANTS.st_macd_superT_query.format(s=trading_symbol,d=TIME_OP.get_earlier_date(7))
        df_macd_query = get_final_df_from_sql(st_macd_superT_query)
        df_macd_query = DF_OPERATIONS.common_df_operations.get_combined_df(df_macd_query, row)
        macd, macdsignal, macdhist = MACD(df_macd_query['LTP'], fastperiod=12, slowperiod=26, signalperiod=9)        
        df['macd'][idx] = macd[-1]
        df['macdsignal'][idx] = macdsignal[-1]
        df['macdhist'][idx] = macdhist[-1]
    return df[COMM_CONSTANTS.pi_data_with_ta]
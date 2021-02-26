'''
Created on 25-Apr-2020

@author: rithomas
'''
import st_excel_reader
import st_constants
import st_xslb_reader
import sys
from datetime import date
from datetime import datetime
from ST_ANALYZER_NSEPY import st_nsePy_constants
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df
import DF_OUTPUT_WRITE_OPERATIONS.various_write_operations
import PYTHON_OPERATIONS.file_operations
import DF_SQL_OPERATIONS.common_sql_operations
import DF_OPERATIONS.common_df_operations
import constants
import email_alert
import pandas as pd
from sqlalchemy import create_engine
from df_db_util import dfDbUtil
import sched, time
from talib.abstract import *
#import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP
import ST_ANALYZER_NSEPY.st_nsePy as nse
import ST_ANALYZER_NSEPY.st_nsePy_constants as constants
import DF_OPERATIONS.common_df_operations as DF_OP
import numpy as np

s = sched.scheduler(time.time, time.sleep)
# sql_query = 'SELECT * FROM stock_info WHERE LTT LIKE \'28-04-2020%\''
engine = create_engine(st_constants.MARIADB_URL)


def get_formattedCurrentTimeMin():
    current_date = PY_OP.get_current_date_with_timeStamp()
    formatted_date = PY_OP.get_formatted_date(current_date, '%Y,%m,%d,%H,%M,%S')
    M = PY_OP.get_formatted_date(current_date, '%M')
    return M

def difference_btw_values(val1, val2):
    h_val = ''
    l_val = ''
    if val1 > val2:
        h_val = val1
        l_val = val2
    else:
        h_val = val2
        l_val = val1
    return h_val - l_val    


def raise_alert(msg):
    print(msg)
    #email_alert.email_alert_sender(constants.PORT, constants.SMTP_SERVER, constants.SENDER_EMAIL, constants.RECEIVER_EMAIL, constants.PASSWORD, msg)
        
    
def get_macd_for_df(df_merged_result_with_macd):
    df_merged_result_with_macd['macd'] = ''
    df_merged_result_with_macd['macdsignal'] = '' 
    df_merged_result_with_macd['macdhist'] = ''   
    numeric_col_list = ['closePrice', 'vwap']
    DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_merged_result_with_macd, numeric_col_list)
    for idx in df_merged_result_with_macd.index:
        ltp =df_merged_result_with_macd['closePrice'][idx]
        ltt =df_merged_result_with_macd['lastUpdateTime'][idx]  
        trading_symbol = df_merged_result_with_macd['trading_symbol'][idx]
        print(trading_symbol)
        st_analysis_10_query = "SELECT closePrice,lastUpdateTime,STR_TO_DATE(lastUpdateTime, '%d-%M-%Y %H:%i:%s') FROM st_nse_data_5 WHERE trading_symbol LIKE '{s}' and STR_TO_DATE(lastUpdateTime, '%d-%M-%Y') > '2020-06-01' ORDER BY STR_TO_DATE(lastUpdateTime, '%d-%M-%Y %H:%i:%s') ASC".format(s=trading_symbol)
        print(st_analysis_10_query)
        df_st_analysis_10 = get_final_df_from_sql(st_analysis_10_query)
        macd, macdsignal, macdhist = MACD(df_st_analysis_10['closePrice'], fastperiod=12, slowperiod=26, signalperiod=9)
        df_merged_result_with_macd['macd'][idx] = macd
        df_merged_result_with_macd['macdsignal'][idx] = macdsignal[-1]
        df_merged_result_with_macd['macdhist'][idx] = macdhist[-1]
        print(df_merged_result_with_macd.columns)
        return df_merged_result_with_macd[constants.nse_data_with_ta]

def get_final_df_from_sql(sql_query):
    df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql


def get_final_df_from_excel(excel_file):
    df_from_excel = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_excel(excel_file, 0, 0)
    return df_from_excel

        

def get_vwap_for_df(todays_date, nse_df):
    vwap_sql_query = constants.vwap_sql_query_5.format(todays_date=todays_date)
    print(vwap_sql_query)
    df_from_sql = get_final_df_from_sql(vwap_sql_query)
    if df_from_sql.empty:
        df_merged_result = nse_df
        df_merged_result['vwap'] = ''
    else:
        df_merged_result = DF_SQL_OPERATIONS.common_sql_operations.get_df_merged_result(nse_df, df_from_sql, 'inner', 'trading_symbol')
        df_merged_result['vwap_closePrice_diff'] = df_merged_result['closePrice'] - df_merged_result['vwap']
    return df_merged_result
    

def main(sc):

    todays_date = date.today()
    currentTime_min=get_formattedCurrentTimeMin()
    currentTime_min_list = list(map(int, str(currentTime_min))) 
    currentTime_minLastElm = currentTime_min_list.pop()



    if (currentTime_minLastElm==7 or currentTime_minLastElm==4):
            nifty50_df = nse.get_nifty50_df(constants.nifty50)
            nifty50_df = get_vwap_for_df(todays_date, nifty50_df)
            nifty50_df = get_macd_for_df(nifty50_df)
            print(nifty50_df)
            print('Inserting to DB')
            DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(nifty50_df, engine, 'st_nse_data_5', 'testrijdb', 'append', False) 
            time.sleep(120)
        
    #check_vwapCrossOver_raiseAlert(todays_date, xlsb_df)
    
    # Schedule code to run every 3 mins
    s.enter(5, 1, main, (sc,))

    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()

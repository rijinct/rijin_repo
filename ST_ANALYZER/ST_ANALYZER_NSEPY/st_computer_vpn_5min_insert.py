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
import ST_ANALYZER_NSEPY.st_common_constants as comm_constants
s = sched.scheduler(time.time, time.sleep)
# sql_query = 'SELECT * FROM stock_info WHERE LTT LIKE \'28-04-2020%\''
engine = create_engine(st_constants.MARIADB_URL)


def get_formattedCurrentTimeMin():
    current_date = PY_OP.get_current_date_with_timeStamp()
    formatted_date = PY_OP.get_formatted_date(current_date, '%Y,%m,%d,%H,%M,%S')
    M = PY_OP.get_formatted_date(current_date, '%M')
    return M

def timeround10(dt):
    a, b = divmod(round(dt.minute, -1), 60)
    return '%i:%02i' % ((dt.hour + a) % 24, b)

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
    for idx,row in df_merged_result_with_macd.iterrows():
        ltp =df_merged_result_with_macd['LTP'][idx]
        ltt =df_merged_result_with_macd['LTT'][idx]  
        trading_symbol = df_merged_result_with_macd['Trading_symbol'][idx]
        st_analysis_10_query = "SELECT * FROM st_analysis_5 WHERE Trading_symbol LIKE '{s}' and STR_TO_DATE(LTT, '%d-%m-%Y') > '2020-07-06' ORDER BY STR_TO_DATE(LTT, '%d-%m-%Y %H:%i:%s') ASC".format(s=trading_symbol)
        df_st_analysis_10 = get_final_df_from_sql(st_analysis_10_query)
        df_st_analysis_10 = DF_OPERATIONS.common_df_operations.get_combined_df(df_st_analysis_10, row)
        macd, macdsignal, macdhist = MACD(df_st_analysis_10['LTP'], fastperiod=12, slowperiod=26, signalperiod=9)
        df_merged_result_with_macd['macd'][idx] = macd[-1]
        df_merged_result_with_macd['macdsignal'][idx] = macdsignal[-1]
        df_merged_result_with_macd['macdhist'][idx] = macdhist[-1]
    return df_merged_result_with_macd[comm_constants.pi_data_with_ta]

def get_final_df_from_xslb():
    latest_default_folder = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(constants.EXCEL_PATH, 'Default')
    latest_xlsb_file = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(latest_default_folder, 'Default')
    #print(latest_xlsb_file)
    xlsb_df = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_xlsb(latest_xlsb_file)
    xlsb_df = DF_OPERATIONS.common_df_operations.add_df_with_new_column_names(xlsb_df, st_constants.xlsb_column_list)
    xlsb_df[['Volume_traded_today', 'ATP', 'LTP']] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(xlsb_df, ['Volume_traded_today', 'ATP', 'LTP'])
    xlsb_df['cumVol_X_Ltp'] = xlsb_df['ATP'] * xlsb_df['Volume_traded_today']
    #print(xlsb_df)
    return xlsb_df

def check_breakout_raise_alarm(df):
    df = df.fillna(0)
    df[['LTP', 'vwap', 'macd','macdsignal','macdhist']] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df, ['LTP', 'vwap', 'macd','macdsignal','macdhist'])
    for idx in df.index:
        macd = df['macd'][idx].item()
        macdsignal = df['macdsignal'][idx].item()
        macdhist = df['macdhist'][idx].item()
        ltp = df['LTP'][idx]
        vwap = df['vwap'][idx]
        ltt = df['LTT'][idx]
        symbol = df['Trading_symbol'][idx]
        if (difference_btw_values(ltp, vwap) < 1.5) and (difference_btw_values(macdhist, 0) < 1.5):
            if (ltp > vwap) and (macd > macdsignal):
                raise_alert('MACD over SIGNAL and ltp over vwap at {l} for {s} and MacdDiff is {d}'.format(l=ltt,s=symbol,d=macdhist))
            if (ltp < vwap) and (macd < macdsignal):
                raise_alert('SIGNAL over MACD and ltp below vwap at {l} for {s} and MacdDiff is {d}'.format(l=ltt,s=symbol,d=macdhist))

def get_final_df_from_sql(sql_query):
    df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql


def get_final_df_from_excel(excel_file):
    df_from_excel = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_excel(excel_file, 0, 0)
    return df_from_excel

def get_vwap_for_df(todays_date, xlsb_df):
    vwap_sql_query = comm_constants.vwap_sql_query_Pi.format(todays_date=todays_date)
    #print(vwap_sql_query)
    df_from_sql = get_final_df_from_sql(vwap_sql_query)
    if df_from_sql.empty:
        df_merged_result = xlsb_df
        df_merged_result['vwap'] = ''
        df_merged_result['vwap_ltp_diff'] = ''
    else:
        df_merged_result = DF_SQL_OPERATIONS.common_sql_operations.get_df_merged_result(xlsb_df, df_from_sql, 'inner', 'Trading_symbol')
        df_merged_result['vwap_ltp_diff'] = df_merged_result['LTP'] - df_merged_result['vwap']
    return df_merged_result

def main(sc):

    todays_date = date.today()
    current_time = datetime.now()
    xlsb_df = get_final_df_from_xslb()
    currentTime_min=get_formattedCurrentTimeMin()
    currentTime_min_list = list(map(int, str(currentTime_min))) 
    currentTime_minLastElm = currentTime_min_list.pop()
    try:
        if (currentTime_minLastElm==7 or currentTime_minLastElm==2):
            todays_date
            print('Inserting to DB')
            nifty50_df = get_vwap_for_df(todays_date, xlsb_df)
            nifty50_df = get_macd_for_df(nifty50_df)
            nifty50_df[comm_constants.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(nifty50_df,comm_constants.st_numeric_cols)
            DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(nifty50_df, engine, 'st_analysis_5', 'testrijdb', 'append', False)
            check_breakout_raise_alarm(nifty50_df) 
            time.sleep(180)
        else:
            pass
    except: 
        print('Duplicate Key, so skipping insert!')



    
    # Schedule code to run every 3 mins
    s.enter(5, 1, main, (sc,))

    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()

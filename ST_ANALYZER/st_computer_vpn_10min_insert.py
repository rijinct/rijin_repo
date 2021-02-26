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
    numeric_col_list = ['LTP', 'vwap']
    DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_merged_result_with_macd, numeric_col_list)
    for idx in df_merged_result_with_macd.index:      
        if difference_btw_values(df_merged_result_with_macd['LTP'][idx], df_merged_result_with_macd['vwap'][idx]) < 1.5:
            trading_symbol = df_merged_result_with_macd['Trading_symbol'][idx]
            raise_alert('Threshold reached for {}'.format(trading_symbol))

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


def get_final_df_from_sql(sql_query):
    df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql


def get_final_df_from_excel(excel_file):
    df_from_excel = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_excel(excel_file, 0, 0)
    return df_from_excel



def check_macdCrossover_raiseAlert(xlsb_df):
    for idx in xlsb_df.index:
        symbol = xlsb_df['Trading_symbol'][idx]
        st_analysis_10_query = "SELECT LTP,LTT FROM st_analysis_10 WHERE Trading_symbol LIKE '{s}' and LTT > '27-06-2020' ORDER BY LTT ASC".format(s=symbol)
        df_st_analysis_10 = get_final_df_from_sql(st_analysis_10_query)
        macd, macdsignal, macdhist = MACD(df_st_analysis_10['LTP'], fastperiod=12, slowperiod=26, signalperiod=9)
        macdHistLasVal = macdhist[-1]
        macdHistoValDiff = difference_btw_values(0, macdHistLasVal)
        if macdHistoValDiff < 0.4:
            raise_alert('MACD crossover for {s} and difference is {d}'.format(s=symbol,d=macdHistoValDiff))


def get_vwap_for_df(todays_date, xlsb_df):
    vwap_sql_query = st_constants.vwap_sql_query.format(todays_date=todays_date)
    df_from_sql = get_final_df_from_sql(vwap_sql_query)
# Joining both xlsb and DB xlsb_df to to get the past values within the day!
    df_merged_result = DF_SQL_OPERATIONS.common_sql_operations.get_df_merged_result(xlsb_df, df_from_sql, 'inner', 'Trading_symbol')
    df_merged_result_selected_fields = df_merged_result[['LTT', 'Trading_symbol', 'LTP', 'cumVol_X_Ltp', 'total_cumVolumeLtp', 'total_traded_vol', 'vwap']]
    db_util_obj = dfDbUtil(st_constants.MARIADB_URL)
    db_util_obj.insert_db_df(df_merged_result_selected_fields, 'st_analysis_vwap', 'testrijdb', 'append', False)
    get_macd_for_df(df_merged_result)

def main(sc):

    todays_date = date.today()
    current_time = datetime.now()
    #print(current_time)
    #print(todays_date)
    xlsb_df = get_final_df_from_xslb()
    currentTime_min=get_formattedCurrentTimeMin()
    currentTime_min_list = list(map(int, str(currentTime_min))) 
    currentTime_minLastElm = currentTime_min_list.pop()
    #print(currentTime_minLastElm)

    try:
        if (currentTime_minLastElm==7):
            print('Inserting to DB')
            DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(xlsb_df, engine, 'st_analysis_10', 'testrijdb', 'append', False)
            check_macdCrossover_raiseAlert(xlsb_df) 
            time.sleep(180)
        else:
            pass
            #print('Skipping insert to DB st_analysis_10')
            #DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(xlsb_df, engine, 'st_analysis', 'testrijdb', 'append', False)
    except: 
        print('Duplicate Key, so skipping insert!')
          
    
    #check_vwapCrossOver_raiseAlert(todays_date, xlsb_df)
    
    # Schedule code to run every 3 mins
    s.enter(5, 1, main, (sc,))

    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()

'''
Created on 25-Apr-2020

@author: rithomas
'''
import sys
from datetime import date
from datetime import datetime
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as DF_INP_OP
import DF_OUTPUT_WRITE_OPERATIONS.various_write_operations
import PYTHON_OPERATIONS.file_operations
import PYTHON_OPERATIONS.time_operations as TIME_OP
import DF_SQL_OPERATIONS.common_sql_operations as DF_SQL_OP
import DF_OPERATIONS.common_df_operations
import MARIADB.mariadb_execute_query as mariadb_executeQuery
import constants
import email_alert
import pandas as pd
from sqlalchemy import create_engine
from df_db_util import dfDbUtil
import sched, time
#import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP
import st_common_constants as COMM_CONSTANTS
import calculate_vwap as vwap
import calculate_macd as macd
import calculate_superTrend as superTrend
s = sched.scheduler(time.time, time.sleep)
# sql_query = 'SELECT * FROM stock_info WHERE LTT LIKE \'28-04-2020%\''
engine = create_engine(COMM_CONSTANTS.MARIADB_URL)


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
    email_alert.email_alert_sender_win32(msg)

def get_final_df_from_xslb(file_dir_pattern):
    latest_default_folder = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(constants.EXCEL_PATH, file_dir_pattern)
    latest_xlsb_file = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(latest_default_folder, file_dir_pattern)
    #print(latest_xlsb_file)
    xlsb_df = DF_INP_OP.get_df_from_xlsb(latest_xlsb_file)
    xlsb_df = DF_OPERATIONS.common_df_operations.add_df_with_new_column_names(xlsb_df, COMM_CONSTANTS.xlsb_column_list)
    xlsb_df[['Volume_traded_today', 'ATP', 'LTP']] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(xlsb_df, ['Volume_traded_today', 'ATP', 'LTP'])
    xlsb_df['cumVol_X_Ltp'] = xlsb_df['ATP'] * xlsb_df['Volume_traded_today']
    #print(xlsb_df)
    return xlsb_df

def check_breakout_raise_alarm(df):
    df = df.fillna(0)
    df[COMM_CONSTANTS.st_aler_num_col] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df, COMM_CONSTANTS.st_aler_num_col)
    for idx in df.index:
        macd = df['macd'][idx].item()
        macdsignal = df['macdsignal'][idx].item()
        macdhist = df['macdhist'][idx].item()
        macd_day = df['macd_day'][idx].item()
        macdsignal_day = df['macdsignal_day'][idx].item()
        macdhist_day = df['macdhist_day'][idx].item()        
        ltp = df['LTP'][idx]
        vwap = df['vwap'][idx]
        ltt = df['LTT'][idx]
        st_7_2 =  df['st_7_2'][idx]
        st_7_3 =  df['st_7_3'][idx]
        st_10_4 =  df['st_10_4'][idx]
        st_7_2_day =  df['st_7_2_day'][idx]
        st_7_3_day =  df['st_7_3_day'][idx]
        symbol = df['Trading_symbol'][idx]
        if (difference_btw_values(ltp, vwap) < 2.5) and (difference_btw_values(macdhist, 0) < 1.5):
        #if (difference_btw_values(ltp, vwap) < 10) and (difference_btw_values(macdhist, 0) < 10):
            if (ltp > vwap) and (macd > macdsignal) and (ltp > st_7_2) and (ltp > st_7_3) and (ltp > st_10_4) and (macd_day > macdsignal_day) and (ltp > st_7_2_day) and (ltp > st_7_3_day):
                raise_alert('ST up, MACD over SIGNAL and ltp over vwap at {l} for {s} and MacdDiff is {d}'.format(l=ltt,s=symbol,d=macdhist))
            if (ltp < vwap) and (macd < macdsignal) and (ltp < st_7_2) and (ltp < st_7_3) and (ltp < st_10_4) and (macd_day < macdsignal_day) and (ltp < st_7_2_day) and (ltp < st_7_3_day):
                raise_alert('ST down, SIGNAL over MACD and ltp below vwap at {l} for {s} and MacdDiff is {d}'.format(l=ltt,s=symbol,d=macdhist))


def get_final_df_from_excel(excel_file):
    df_from_excel = DF_INP_OP.get_df_from_excel(excel_file, 0, 0)
    return df_from_excel


def delete_older_data_db(older_date):
    delete_query_st_analysis = COMM_CONSTANTS.delete_query_st_analysis.format(older_date)
    delete_query_st_analysis_5 = COMM_CONSTANTS.delete_query_st_analysis_5.format(older_date)
    mariadb_executeQuery.connectMariadb_executeQuery(delete_query_st_analysis)
    mariadb_executeQuery.connectMariadb_executeQuery(delete_query_st_analysis_5)


def get_finalXlsb_df():
    xlsb_df1 = get_final_df_from_xslb('Default')
    xlsb_df2 = get_final_df_from_xslb('MyMW')
    xlsb_df = DF_OPERATIONS.common_df_operations.get_combined_df(xlsb_df1, xlsb_df2)
    xlsb_df = DF_SQL_OP.drop_duplicate_based_on_columns(xlsb_df, 'Trading_symbol', 'first')
    return xlsb_df

def main(sc):

    todays_date = date.today()
    current_time = datetime.now()
    older_date = TIME_OP.get_earlier_date(7)
    delete_older_data_db(older_date)
    currentTime_min=get_formattedCurrentTimeMin()
    currentTime_min_list = list(map(int, str(currentTime_min))) 
    currentTime_minLastElm = currentTime_min_list.pop()
    
    try:
        xlsb_df = get_finalXlsb_df()
        xlsb_df[COMM_CONSTANTS.st_xlsb_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(xlsb_df,COMM_CONSTANTS.st_xlsb_numeric_cols)
        xlsb_df.to_csv(r'C:\Users\rithomas\Desktop\xlsb_df.csv')
        DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(xlsb_df, engine, 'st_analysis', 'testrijdb', 'append', False)
        print('Inserted minData to DB')
        
    except (FileNotFoundError,ValueError):
        print('xlsb file Not found')
        
    except:
        print('Dup key in st_analysis')
        
    try:
        
        if (currentTime_minLastElm==7 or currentTime_minLastElm==2):   
            print('Inserting to DB')
            #writes 2 mins or 3mins data to st_analysis
            st_analysis_5minQueryDate = COMM_CONSTANTS.st_analysis_5minQuery.format(d=TIME_OP.get_earlier_date(4))
            nifty_50_df = DF_INP_OP.get_df_from_sql(st_analysis_5minQueryDate, engine)
            nifty50_df = vwap.get_vwap_for_df(todays_date, nifty_50_df, COMM_CONSTANTS.vwap_sql_query_Pi)
            nifty50_df = macd.get_macd_for_df(nifty50_df)
            #nifty50_df.to_csv(r'C:\Users\rithomas\Desktop\nifty_macd.csv')
            nifty50_df[COMM_CONSTANTS.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(nifty50_df,COMM_CONSTANTS.st_numeric_cols)
            nifty50_ta_df = superTrend.get_superTrend_for_df(nifty50_df)
            #nifty50_ta_df.to_csv(r'C:\Users\rithomas\Desktop\nifty_superT.csv')
            #nifty50_ta_df[COMM_CONSTANTS.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(nifty50_ta_df,COMM_CONSTANTS.st_numeric_cols)
            nifty_50_dayDf = DF_INP_OP.get_df_from_sql(COMM_CONSTANTS.st_analysis_nifty50_dayQuery, engine)
            nifty_finalDf = DF_SQL_OP.get_df_merged_result(nifty50_ta_df, nifty_50_dayDf, 'inner', 'Trading_symbol')
            nifty_finalDf['LTT_formatted'] = pd.to_datetime(nifty_finalDf['LTT'], format='%d-%m-%Y %H:%M:%S')
            #nifty_finalDf.to_csv(r'C:\Users\rithomas\Desktop\nifty_finalDf_before_drop.csv')           
            ##To filter out records lesser than today
            nifty_finalDf.drop(nifty_finalDf[nifty_finalDf['LTT_formatted'] < TIME_OP.get_formatted_date(todays_date, '%d-%m-%Y')].index, inplace = True)
            nifty_finalDf.drop(['LTT_formatted'], axis = 1,inplace = True)
            nifty_finalDf = nifty_finalDf.replace('NaN',0)
            nifty_finalDf.to_csv(r'C:\Users\rithomas\Desktop\nifty_finalDf.csv')
            nifty_finalDf[COMM_CONSTANTS.st_day_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(nifty_finalDf,COMM_CONSTANTS.st_day_numeric_cols)
            DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(nifty_finalDf, engine, 'st_analysis_5', 'testrijdb', 'append', False)
            check_breakout_raise_alarm(nifty_finalDf) 
            time.sleep(180)
        else:
            pass
    except: 
            print('Duplicate Key, so skipping insert!')


    
    # Schedule code to run every 3 mins
    s.enter(30, 1, main, (sc,))

    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()

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
s = sched.scheduler(time.time, time.sleep)
# sql_query = 'SELECT * FROM stock_info WHERE LTT LIKE \'28-04-2020%\''
engine = create_engine(st_constants.MARIADB_URL)


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
        #print(difference_btw_values(df_merged_result_with_macd['LTP'][idx], df_merged_result_with_macd['vwap'][idx]))
        if difference_btw_values(df_merged_result_with_macd['LTP'][idx], df_merged_result_with_macd['vwap'][idx]) < 1.5:
            trading_symbol = df_merged_result_with_macd['Trading_symbol'][idx]
            #if (df_merged_result_with_macd['LTP'][idx] > df_merged_result_with_macd['vwap'][idx]) and (df_merged_result_with_macd['macd'][idx] > df_merged_result_with_macd['signal'][idx]):
            raise_alert('Threshold reached for {}'.format(trading_symbol))    
           # elif (df_merged_result_with_macd['LTP'][idx] < df_merged_result_with_macd['vwap'][idx]) and (df_merged_result_with_macd['macd'][idx] < df_merged_result_with_macd['signal'][idx]):
             #   raise_alert(df_merged_result_with_macd['Trading_symbol'][idx])      

def get_final_df_from_xslb():
    latest_default_folder = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(constants.EXCEL_PATH, 'Default')
    latest_xlsb_file = PYTHON_OPERATIONS.file_operations.get_latest_file_or_folder_based_pattern(latest_default_folder, 'Default')
    print(latest_xlsb_file)
    xlsb_df = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_xlsb(latest_xlsb_file)
    xlsb_df = DF_OPERATIONS.common_df_operations.add_df_with_new_column_names(xlsb_df, st_constants.xlsb_column_list)
    xlsb_df[['Volume_traded_today', 'ATP', 'LTP']] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(xlsb_df, ['Volume_traded_today', 'ATP', 'LTP'])
    xlsb_df['cumVol_X_Ltp'] = xlsb_df['ATP'] * xlsb_df['Volume_traded_today']
    #print(xlsb_df)
    return xlsb_df


def get_final_df_from_sql(todays_date):
    vwap_sql_query = st_constants.vwap_sql_query.format(todays_date=todays_date)
    #print(vwap_sql_query)
    df_from_sql = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_sql(vwap_sql_query, engine)
    return df_from_sql


def get_final_df_from_excel(excel_file):
    df_from_excel = DF_INPUT_READER_OPERATIONS.different_input_format_to_df.get_df_from_excel(excel_file, 0, 0)
    return df_from_excel


def calculate_ema(ltp, multiplier, ema_y):
    ema_today = (ltp * multiplier) + (ema_y * (1 - multiplier)) 
    return ema_today


def regenerate_macd_with_latest_ltp(df):
    df['Ema26_curr'] = calculate_ema(df['LTP'], 0.074, df['Ema26'])
    df['Ema12_curr'] = calculate_ema(df['LTP'], 0.154, df['Ema12'])
    df['Macd_curr'] = df['Ema12_curr'] - df['Ema26_curr']
    df['Signal_curr'] = calculate_ema(df['Macd'], 0.2, df['Macd'])
    df['Hologram_curr'] = df['Macd_curr'] - df['Signal_curr']
    df_regenerated_macd = df[['Trading_symbol', 'Ema26_curr', 'Ema12_curr', 'Macd_curr', 'Signal_curr', 'Hologram_curr']]
    df_regenerated_macd = DF_OPERATIONS.common_df_operations.add_df_with_new_column_names(df_regenerated_macd, st_constants.MACD_FILE_COL_LIST)
    #print(df_regenerated_macd)
    if not df.empty:     
        df_regenerated_macd.to_excel(st_constants.ST_INPUT_TECHNICAL_EXCEL, index=False)
    else:
        raise_alert('Empty Dataframe')  
    return df


def main(sc):

    todays_date = date.today()
    current_time = datetime.now()
    print(current_time)
    # todays_early_morning = datetime.combine(todays_date, datetime.min.time())
    # todays_early_morning_formatted = todays_early_morning.strftime("%d-%m-%Y %H:%M:%S")
    print(todays_date)
    xlsb_df = get_final_df_from_xslb()
    
    try:
        DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(xlsb_df, engine, 'st_analysis', 'testrijdb', 'append', False)
    except: 
        print('Duplicate Key, so skipping insert!')
    # exit(0)
    # check_dfs_and_generate_alert(input_technical_df,xslb_df)
    '''
    different_input_format_to_df_obj.get_df_from_sql(sql_query)
    df_from_sql = different_input_format_to_df_obj.get_df('sql')
    df_with_selected_fields_from_sql = df_from_sql[['Trading_symbol', 'LTP','Volume_traded_today','LTT']]
    df_with_selected_fields_from_sql['lagged_volume'] = df_with_selected_fields_from_sql.groupby(['Trading_symbol'])['Volume_traded_today'].shift(1)
    df_with_selected_fields_from_sql.sort_values(by=['Trading_symbol','LTT'], inplace=True)
    df_with_selected_fields_from_sql[['Volume_traded_today','lagged_volume','LTP']] = df_with_selected_fields_from_sql[['Volume_traded_today','lagged_volume','LTP']].apply(pd.to_numeric)
    df_with_selected_fields_from_sql['delta_volume'] = df_with_selected_fields_from_sql['Volume_traded_today'] - df_with_selected_fields_from_sql['lagged_volume']
    df_with_selected_fields_from_sql['volXprice'] = df_with_selected_fields_from_sql['lagged_volume'] * df_with_selected_fields_from_sql['LTP']
    print(df_with_selected_fields_from_sql)
    '''
    
    df_from_sql = get_final_df_from_sql(todays_date)
    df_from_excel = get_final_df_from_excel(st_constants.ST_INPUT_TECHNICAL_EXCEL)
    # Joining both xlsb and DB xlsb_df to to get the past values within the day!
    df_merged_result = DF_SQL_OPERATIONS.common_sql_operations.get_df_merged_result(xlsb_df, df_from_sql, 'inner', 'Trading_symbol')
    df_merged_result_with_macd = DF_SQL_OPERATIONS.common_sql_operations.get_df_merged_result(df_merged_result, df_from_excel, 'inner', 'Trading_symbol')
    '''
    if not df_merged_result_with_macd.empty:     
        df_merged_result_with_macd = regenerate_macd_with_latest_ltp(df_merged_result_with_macd)
    else:
        raise_alert('Empty Dataframe when joining with macd DF')
        exit(0)  
    '''
    df_merged_result_selected_fields = df_merged_result[['LTT', 'Trading_symbol', 'LTP', 'cumVol_X_Ltp', 'total_cumVolumeLtp', 'total_traded_vol', 'vwap']]
    #print(df_merged_result_selected_fields)
    db_util_obj = dfDbUtil(st_constants.MARIADB_URL)
    db_util_obj.insert_db_df(df_merged_result_selected_fields, 'st_analysis_vwap', 'testrijdb', 'append', False)
    
    get_macd_for_df(df_merged_result)
    
    # Schedule code to run every 3 mins
    s.enter(5, 1, main, (sc,))

    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()

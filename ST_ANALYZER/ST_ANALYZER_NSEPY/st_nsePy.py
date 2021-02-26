'''
Created on 24-May-2020

@author: rithomas
'''
import sys
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
from nsepy import get_history
from nsepy import get_quote
import pandas as pd
from talib.abstract import *
import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as INPUT_FR
import st_common_constants as constants
import DF_OPERATIONS.common_df_operations as DF_OP

#trading_symbol_fut = get_history("SBIN", date(2020,5,26), date(2020,7,3),futures= False)
#trading_symbol_curr_quote = get_quote("SBIN", "EQ")
#print(trading_symbol_fut.columns)
#print(trading_symbol_fut)
#atr = ATR(trading_symbol_fut['High'],trading_symbol_fut['Low'], trading_symbol_fut['Close'], timeperiod=14)
#print(atr)

def get_dayData_df(symbol,from_dt,to_dt):
    if isinstance(from_dt, str):
        from_dt = datetime.datetime.strptime(from_dt, "%d,%m,%Y").date()
        to_dt = datetime.datetime.strptime(to_dt, "%d,%m,%Y").date()
    return get_history(symbol, from_dt, to_dt,futures= False)

def get_minData_df(symbol):
    q = get_quote(symbol, "EQ")
    df = INPUT_FR.get_df_from_nested_json(q)
    return df

def get_nifty50_df(symbol_list):
    nse_df =  pd.DataFrame()
    for sym in symbol_list:
        nse_df_symb = get_minData_df(sym)
        nse_df = nse_df.append(nse_df_symb, sort=False)
    nse_df_sub = nse_df[constants.nse_data_act_col]
    nse_df_sub = DF_OP.add_df_with_new_column_names(nse_df_sub, constants.nse_data_col)
    nse_df = nse_df_sub
    nse_df = DF_OP.get_df_converted_toString(nse_df)
    nse_df = nse_df.apply(lambda x: x.str.replace(',',''))
    nse_df = nse_df.replace('nan',0)
    nse_df[['totalTradedVolume','closePrice']] = nse_df[['totalTradedVolume','lastPrice']].replace(',','').apply(pd.to_numeric)
    nse_df.rename(columns = {'symbol':'trading_symbol'}, inplace = True)
    nse_df['cumVol_X_Ltp'] = nse_df['closePrice'] * nse_df['totalTradedVolume']
    #nse_df.('trading_symbol', inplace=True)
    return nse_df 

if __name__ == '__main__':
    
    from_dt = PY_OP.get_earlier_date(30)
    to_dt = PY_OP.get_current_date_without_timeStamp()
    day_df = get_dayData_df('NIITTECH', from_dt, to_dt)
    print(day_df.columns)
    print(day_df)
    exit(0)
    '''
    #To run manually, make sure the arguments are str
    from_dt = '2020-06-05'
    to_dt = '2020-06-30'
    day_df = get_dayData_df('SBIN', from_dt, to_dt)
    '''
    
    ### To get minute data in df 
    df_min_data = get_minData_df('SBIN')
    print(df_min_data.columns)
    
    nifty_df = get_nifty50_df(constants.nifty50)
    print(nifty_df['trading_symbol'])
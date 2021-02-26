'''
Created on 24-May-2020

@author: rithomas
'''

from nsepy import get_history
from nsepy import get_quote
import pandas as pd
from talib.abstract import *
import datetime
import PYTHON_OPERATIONS.time_operations as PY_OP
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as INPUT_FR
import ST_ANALYZER_NSEPY.st_nsePy as nse

def get_macd(df):
    macd, macdsignal, macdhist = MACD(df, fastperiod=12, slowperiod=26, signalperiod=9)
    return macd[-1],macdsignal[-1],macdhist[-1]

def get_atr(high,low,close):
    atr =  ATR(high,low, close, timeperiod=14)


if __name__ == '__main__':
    
    from_dt = PY_OP.get_earlier_date(60)
    to_dt = PY_OP.get_current_date_without_timeStamp()
    day_df = nse.get_dayData_df('RELIANCE', from_dt, to_dt)
    print(day_df)
    macd,signal,macdHist = get_macd(day_df['Close'])
    print(macd,signal,macdHist)
    exit(0)
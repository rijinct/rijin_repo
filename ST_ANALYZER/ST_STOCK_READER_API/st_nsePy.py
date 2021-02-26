'''
Created on 24-May-2020

@author: rithomas
'''

from nsepy import get_history
from nsepy import get_quote
import pandas as pd
from datetime import date
import pandas as pd
import talib
import ta
from talib.abstract import *

trading_symbol_fut = get_history("SBIN", date(2020,5,26), date(2020,7,3),futures= False)
trading_symbol_curr_quote = get_quote("SBIN", "EQ")
print(trading_symbol_fut.columns)
print(trading_symbol_fut)
atr = ATR(trading_symbol_fut['High'],trading_symbol_fut['Low'], trading_symbol_fut['Close'], timeperiod=14)
print(atr)

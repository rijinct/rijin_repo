'''
Created on 28-May-2020

@author: rithomas
'''
import numpy as np
from talib.abstract import *
import talib

# note that all ndarrays must be the same length!
inputs = {
    'open': np.random.random(10),
    'high': np.random.random(10),
    'low': np.random.random(10),
    'close': np.random.random(10),
    'volume': np.random.random(10)
}

print(inputs)
exit(0)

output = SMA(inputs, timeperiod=25)

# uses open prices
output = SMA(inputs, timeperiod=25, price='open')

macd, macdsignal, macdhist = MACD(inputs)


print(macd)
print('---------------------------------------------------')
print(macdsignal)
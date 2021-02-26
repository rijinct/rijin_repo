'''
Created on 28-May-2020

@author: rithomas
'''

"""This is a example adding bollinger band features.
"""
import pandas as pd
import ta

# Load data
df = pd.read_csv(r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\ST_ANALYZER\conf\data.csv', sep=',')

# Clean nan values
df = ta.utils.dropna(df)

print(df.columns)

# Add bollinger band high indicator filling nans values
df['bb_high_indicator'] = ta.volatility.bollinger_hband_indicator(df["Close"], n=20, ndev=2, fillna=True)

# Add bollinger band low indicator filling nans values
df['bb_low_indicator'] = ta.volatility.bollinger_lband_indicator(df["Close"], n=20, ndev=2, fillna=True)

print(df['bb_high_indicator'] )
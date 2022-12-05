import pandas as pd
import pandas_ta as ta
from ta.volume import VolumeWeightedAveragePrice as vwap


def calculate_macd(df):
    df.ta.macd(close='Close', fast=12, slow=26, signal=9, append=True)
    #print(df.columns)
    df.columns = ['Open', 'High', 'Low', 'Close',  'Volume', 'macd', 'histogram',
                  'signal']  # uncomment
    #df.columns = ['Date','Open', 'High', 'Low', 'Close', 'macd', 'histogram','signal']  #test
    pd.set_option("display.max_columns", None)  # show all columns
    return df[['macd', 'signal', 'histogram', 'Close']]


def calculate_ST(df):
    sti = ta.supertrend(df['High'], df['Low'], df['Close'], length=10, multiplier=3)
    #print(sti)
    return sti


def calculate_pivot(df):
    last_day = df
    last_day['Pivot'] = (last_day['High'] + last_day['Low'] + last_day[
        'Close']) / 3
    last_day['R1'] = 2 * last_day['Pivot'] - last_day['Low']
    last_day['S1'] = 2 * last_day['Pivot'] - last_day['High']
    last_day['R2'] = last_day['Pivot'] + (last_day['High'] - last_day['Low'])
    last_day['S2'] = last_day['Pivot'] - (last_day['High'] - last_day['Low'])
    last_day['R3'] = last_day['Pivot'] + 2 * (
            last_day['High'] - last_day['Low'])
    last_day['S3'] = last_day['Pivot'] - 2 * (
            last_day['High'] - last_day['Low'])
    return last_day['Pivot']


def calculate_vwap(df):
    df_vwap = vwap(df['High'],df['Low'],df['Close'],df['Volume'])
    #print(df_vwap.volume_weighted_average_price())
    return df_vwap.volume_weighted_average_price()

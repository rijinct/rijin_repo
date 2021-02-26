'''
Created on 24-May-2020

@author: rithomas
'''

import yfinance as yf

#define the ticker symbol
tickerSymbol = 'NSEI'

#get data on this ticker
tickerData = yf.Ticker(tickerSymbol)

#get the historical prices for this ticker
tickerDf = tickerData.history(period='1d', start='2010-1-1', end='2020-1-25')

#see your data
print(tickerDf)
# -*- coding: utf-8 -*-
"""

@author: techietrader

"""

import numpy as np
import pandas as pd  


#SuperTrend
def ST(df,f,n): #df is the dataframe, n is the period, f is the factor; f=3, n=7 are commonly used.
    
    try:
        #Calculation of ATR
        df['H-L']=abs(df['High']-df['Low'])
        df['H-PC']=abs(df['High']-df['LTP'].shift(1))
        df['L-PC']=abs(df['Low']-df['LTP'].shift(1))
        df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR']=np.nan
        df.ix[n-1,'ATR']=df['TR'][:n-1].mean() #.ix is deprecated from pandas verion- 0.19
        for i in range(n,len(df)):
            df['ATR'][i]=(df['ATR'][i-1]*(n-1)+ df['TR'][i])/n
    
        #Calculation of SuperTrend
        df['Upper Basic']=(df['High']+df['Low'])/2+(f*df['ATR'])
        df['Lower Basic']=(df['High']+df['Low'])/2-(f*df['ATR'])
        df['Upper Band']=df['Upper Basic']
        df['Lower Band']=df['Lower Basic']
        for i in range(n,len(df)):
            if df['LTP'][i-1]<=df['Upper Band'][i-1]:
                df['Upper Band'][i]=min(df['Upper Basic'][i],df['Upper Band'][i-1])
            else:
                df['Upper Band'][i]=df['Upper Basic'][i]    
        for i in range(n,len(df)):
            if df['LTP'][i-1]>=df['Lower Band'][i-1]:
                df['Lower Band'][i]=max(df['Lower Basic'][i],df['Lower Band'][i-1])
            else:
                df['Lower Band'][i]=df['Lower Basic'][i]   
        df['SuperTrend']=np.nan
        for i in df['SuperTrend']:
            if df['LTP'][n-1]<=df['Upper Band'][n-1]:
                df['SuperTrend'][n-1]=df['Upper Band'][n-1]
            elif df['LTP'][n-1]>df['Upper Band'][i]:
                df['SuperTrend'][n-1]=df['Lower Band'][n-1]
        for i in range(n,len(df)):
            if df['SuperTrend'][i-1]==df['Upper Band'][i-1] and df['LTP'][i]<=df['Upper Band'][i]:
                df['SuperTrend'][i]=df['Upper Band'][i]
            elif  df['SuperTrend'][i-1]==df['Upper Band'][i-1] and df['LTP'][i]>=df['Upper Band'][i]:
                df['SuperTrend'][i]=df['Lower Band'][i]
            elif df['SuperTrend'][i-1]==df['Lower Band'][i-1] and df['LTP'][i]>=df['Lower Band'][i]:
                df['SuperTrend'][i]=df['Lower Band'][i]
            elif df['SuperTrend'][i-1]==df['Lower Band'][i-1] and df['LTP'][i]<=df['Lower Band'][i]:
                df['SuperTrend'][i]=df['Upper Band'][i]
        return df
    
    except:
        return df

    

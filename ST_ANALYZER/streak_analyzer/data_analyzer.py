'''
Created on 31-Dec-2020

@author: rithomas
'''
import pandas as pd

month="Jan"
month_val="2"
year="2021"

streak_dir=r"C:\Users\admin\Desktop\Rijin\data\15mins\{y}\{m}\Strategy1".format(y=year,m=month)
file_name = '{m}.{f}'.format(m=month,f='csv')

file = '{p}\{f}'.format(p=streak_dir, f=file_name)
day_sym_file = '{p}\{m}-{f}'.format(p=streak_dir, f='day_symbol.csv', m=month)
day_pl_file = '{p}\{m}-{f}'.format(p=streak_dir, f='day_pl.csv', m=month)

df = pd.read_csv(file)
df.columns = ['date', 'symbol', 'buy_sell', 'quantity', 'buy_price', 'pl', 'trigger_type']

# df2 = df.groupby(['date','symbol']).last()
df['sell_price'] = df.loc[:,['buy_price']].groupby(df['symbol']).shift(-1)
df['sell_date'] = df['date'].shift(-1)
df['sell_symbol'] = df['buy_sell'].shift(-1) ## This logic, coz when a buy is not completed it gets confused and takes beginning of group as sell.
df['profit'] = (df['sell_price'] - df['buy_price']) * df['quantity']
#df['diff_pl'] = df['price'] - df['buy_value']
#df['day_prof'] = df['diff_pl'] * -df['quantity']
df = df.loc[(df['buy_sell'] == 'BUY') & (df['sell_symbol'] == 'SELL'), :].drop_duplicates()
df.to_csv(day_sym_file)
df[['date', 'time']] = df.date.str.split(expand=True)
df2 = df.loc[:,['profit']].groupby(df['date']).sum()
df2.to_csv(day_pl_file)
print(df2)

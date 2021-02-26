'''
Created on 25-Apr-2020

@author: rithomas
'''

MARIADB_URL = 'mysql+mysqlconnector://root:zaq12wsx@127.0.0.1/testrijdb'

vwap_sql_query = '''
select Trading_symbol, total_cumVolumeLtp, total_traded_vol, total_cumVolumeLtp/total_traded_vol as vwap from (select Trading_symbol,\
SUM(volume_traded_today) as total_traded_vol, SUM(cumVol_X_Ltp) as total_cumVolumeLtp from st_analysis \
    where STR_TO_DATE(LTT, '%d-%m-%Y') = '{todays_date}' group by Trading_symbol)q1
'''

vwap_sql_query_5 = '''
select Trading_symbol, total_cumVolumeLtp, total_traded_vol, total_cumVolumeLtp/total_traded_vol as vwap from (select Trading_symbol,\
SUM(volume_traded_today) as total_traded_vol, SUM(cumVol_X_Ltp) as total_cumVolumeLtp from st_analysis_5 \
    where STR_TO_DATE(LTT, '%d-%m-%Y') = '{todays_date}' group by Trading_symbol)q1
'''
    
xlsb_column_list = ['Trading_symbol', 'LTP', 'Bid_qty', 'Bid_rate', 'Ask_rate', 'Ask_qty', 'LTQ', 'Open', 'High', 'Low', 'Prev_close', 'Volume_traded_today', 'Open_interest', 'ATP', 'Total_bid_qty', 'Total_ask_qty', 'Exchange', 'LTT', 'LUT']
MACD_FILE_COL_LIST = ['Trading_symbol','Ema26', 'Ema12', 'Macd', 'Signal', 'Hologram']    
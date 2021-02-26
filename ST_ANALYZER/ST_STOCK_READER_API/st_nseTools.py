'''
Created on 24-May-2020

@author: rithomas
'''
from nsetools import Nse
from pprint import pprint
from nsepy import get_quote
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as INPUT_FR
from sqlalchemy import create_engine
from df_db_util import dfDbUtil
import sched, time
s = sched.scheduler(time.time, time.sleep)

def main(sc):
    nse = Nse()
    q = get_quote('sbin', 'EQ')
    print(q)
    
    all_stock_codes = nse.get_stock_codes()
    #print(all_stock_codes)
    
    top_gainers = nse.get_top_gainers()
    print(top_gainers)
    
    df = INPUT_FR.get_df_from_nested_json(q)
    print(df.columns)
    print(df[['data_0_symbol','data_0_closePrice','lastUpdateTime']])
    exit(0)
    
    db_util_obj = dfDbUtil('mysql+mysqlconnector://root:zaq12wsx@127.0.0.1/testrijdb')
    db_util_obj.insert_db_df(df, 'st_nse_realtime', 'testrijdb', 'append', False)
    
    s.enter(5, 1, main, (sc,))
    
if __name__ == '__main__':

    s.enter(1, 1, main, (s,))
    s.run()
'''
Created on 25-Apr-2020

@author: rithomas
'''
import sys
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
import st_nsePy as nse
import PYTHON_OPERATIONS.time_operations as PY_OP
import st_common_constants as COMM_CONSTANTS
import DF_OPERATIONS
import calculate_superTrend as superTrend
import calculate_macd as macd
import pandas as pd
import DF_OUTPUT_WRITE_OPERATIONS.various_write_operations
from sqlalchemy import create_engine
engine = create_engine(COMM_CONSTANTS.MARIADB_URL)

def get_nifty50_dayDf():
    nifty50_dayDf=pd.DataFrame()
    nifty_total = COMM_CONSTANTS.nifty50 + COMM_CONSTANTS.non_nifty_wrk
    for syb in nifty_total:
        try:
            from_dt = PY_OP.get_earlier_date(50)
            to_dt = PY_OP.get_current_date_without_timeStamp()
            day_df = nse.get_dayData_df(syb, from_dt, to_dt)
            day_df = DF_OPERATIONS.common_df_operations.add_df_with_new_column_names(day_df, COMM_CONSTANTS.st_dayDf_col_list)
            macd_df = macd.get_macd_for_day(day_df)
            st_df = superTrend.get_superTrend_for_day(macd_df)
            nifty50_dayDf = nifty50_dayDf.append(st_df.iloc[[-1]],sort=False)
        except: 
            print('Failed for {}'.format(syb))
    nifty50_dayDf['Trading_symbol'] = nifty50_dayDf['Trading_symbol'] + '-EQ'
    return nifty50_dayDf[COMM_CONSTANTS.st_dayDf_final_col_list]    

def main():
    nifty50_dayDf = get_nifty50_dayDf()    
    DF_OUTPUT_WRITE_OPERATIONS.various_write_operations.insert_db_df(nifty50_dayDf, engine, 'st_analysis_nifty50_day', 'testrijdb', 'replace', False)
    
    
if __name__ == '__main__':
    main()

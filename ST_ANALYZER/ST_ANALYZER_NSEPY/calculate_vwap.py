'''
Created on 17-Jul-2020

@author: rithomas
'''
import  DF_SQL_OPERATIONS.common_sql_operations as COMM_OP
import DF_INPUT_READER_OPERATIONS as DF_OP
from sqlalchemy import create_engine
import st_common_constants as COMM_CONSTANTS

engine = create_engine(COMM_CONSTANTS.MARIADB_URL)

def get_final_df_from_sql(sql_query):
    df_from_sql = DF_OP.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql

def get_vwap_for_df(todays_date, xlsb_df, vwap_query):
    vwap_sql_query = vwap_query.format(todays_date=todays_date)
    df_from_sql = get_final_df_from_sql(vwap_sql_query)
    if df_from_sql.empty:
        df_merged_result = xlsb_df
        df_merged_result['vwap'] = ''
        df_merged_result['vwap_ltp_diff'] = ''
    else:
        df_merged_result = COMM_OP.get_df_merged_result(xlsb_df, df_from_sql, 'left', 'Trading_symbol')
        df_merged_result['vwap_ltp_diff'] = df_merged_result['LTP'] - df_merged_result['vwap']
    return df_merged_result

if __name__ == '__main__':
    df = get_vwap_for_df('2020-07-12','xlsb_df',COMM_CONSTANTS.vwap_sql_query_5);



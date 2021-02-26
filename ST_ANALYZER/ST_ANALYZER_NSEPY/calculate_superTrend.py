'''
Created on 18-Jul-2020

@author: rithomas
'''
from sqlalchemy import create_engine
import DF_INPUT_READER_OPERATIONS as DF_OP
import st_common_constants as COMM_CONSTANTS
import PYTHON_OPERATIONS.time_operations as TIME_OP
import superTrendCalc as superTrend
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as DF_INP_OP
import DF_OPERATIONS.common_df_operations

engine = create_engine(COMM_CONSTANTS.MARIADB_URL)



def get_final_df_from_sql(sql_query):
    df_from_sql = DF_OP.different_input_format_to_df.get_df_from_sql(sql_query, engine)
    return df_from_sql


def get_st():
    st_macd_superT_query = COMM_CONSTANTS.st_macd_superT_query.format(s='ADANIPORTS-EQ',d=TIME_OP.get_earlier_date(4))
    df_from_sql = get_final_df_from_sql(st_macd_superT_query)
    #df_from_sql['LTP'] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_from_sql, ['LTP'])
    print(df_from_sql)
    df_from_sql[COMM_CONSTANTS.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_from_sql,COMM_CONSTANTS.st_numeric_cols)
    df = superTrend.ST(df_from_sql,3,7)
    print(df)

def get_superTrend_for_df(df):
    df['st_7_2'] = ''
    df['st_7_3'] = '' 
    df['st_10_4'] = ''
    for idx,row in df.iterrows():
        ltp =df['LTP'][idx]
        ltt =df['LTT'][idx]  
        trading_symbol = df['Trading_symbol'][idx]
        try:
            st_macd_superT_query = COMM_CONSTANTS.st_macd_superT_query.format(s=trading_symbol,d=TIME_OP.get_earlier_date(4))
            df_macd_superT_query = get_final_df_from_sql(st_macd_superT_query)
            df_macd_superT_query = DF_OPERATIONS.common_df_operations.get_combined_df(df_macd_superT_query, row)
            df_macd_superT_query[COMM_CONSTANTS.st_numeric_cols] = DF_OPERATIONS.common_df_operations.get_col_converted_toNumeric(df_macd_superT_query,COMM_CONSTANTS.st_numeric_cols)
            df_length = len(df_macd_superT_query)
            df_macd_superT_query.index = range(df_length)
            df['st_7_2'][idx] = superTrend.ST(df_macd_superT_query,2,7).tail(1)['SuperTrend'].to_string(index=False)
            df['st_7_3'][idx] = superTrend.ST(df_macd_superT_query,3,7).tail(1)['SuperTrend'].to_string(index=False)
            df['st_10_4'][idx] = superTrend.ST(df_macd_superT_query,4,10).tail(1)['SuperTrend'].to_string(index=False)
        except: 
            print('SuperT calc failed for {}'.format(trading_symbol))
    return df[COMM_CONSTANTS.pi_data_with_ta_ST]

def get_superTrend_for_day(df):
    df = superTrend.ST(df,2,7)
    df['st_7_2_day'] = df['SuperTrend']
    df = superTrend.ST(df,3,7)
    df['st_7_3_day'] = df['SuperTrend']
    return df

if __name__ == '__main__':
    nifty_50_df = DF_INP_OP.get_df_from_sql(COMM_CONSTANTS.st_analysis_5minQuery, engine)
    df = get_superTrend_for_df(nifty_50_df)
    #get_st()
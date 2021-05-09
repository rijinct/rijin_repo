import pandas as pd
from sqlalchemy import create_engine

class dfDbUtil:
    def __init__(self,db_url):
        self.engine = create_engine(db_url)
        self.pd = pd
        
    def get_sql_result_df(self,query): 
        return self.pd.read_sql(query, self.engine)
 
    def insert_db_df(self, df, table_name, schema, action, index):
         df.to_sql(table_name, self.engine, schema=schema, if_exists=action, index=index)
         
    def get_df_merged_result(self,df1, df2, join_condition, join_col_list):
        return pd.merge(df1, df2, how=join_condition , left_on=join_col_list, right_on=join_col_list)
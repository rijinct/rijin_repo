import pandas as pd

    def __init__(self, df1, df2, join_condition, column):       
        self.df_merge = pd.merge(df1, df2, how='join_condition' , on=column)
        
    def get_df_merged(self):
        return self.df_merge
        
    def get_df_column_list(self, col_list):
        return self.df_excel[col_list]       
    
    def add_df_new_column(self, col_name, col_value):
        self.df_merge[col_name] = col_value
    
    
    
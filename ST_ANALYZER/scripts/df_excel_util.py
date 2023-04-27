import pandas as pd
import common_constants

class dfExcelUtil:

    def __init__(self, file, header, sheet_name):
        if sheet_name is None:
            sheet_name = 0       
        self.df_excel = pd.read_excel(file, header=header, sheet_name=sheet_name)
        
    def get_df_excel(self):
        return self.df_excel
        
    def get_df_column_list(self, col_list):
        return self.df_excel[col_list]    
    
    def get_df_all_columns(self):
        return self.df_excel.columns     
    
    def add_df_with_new_column_names(self, column_names_as_list):
        self.df_excel.columns = column_names_as_list
        return self.df_excel
    
    def add_df_new_column(self, col_name):
        self.df_excel[col_name]
    
    def get_unique_df_col(self, col_name):
        return self.df_excel[col_name].unique()
    
    def add_df_rank_col(self, rank_col_name, col_name):
        self.df_excel[rank_col_name] = self.df_excel[col_name].rank(method='dense')

    def add_new_columns(self,column_list):
        self.df_excel.columns = column_list
        
    def add_df_with_new_column_and_indexes(self, column_name_list,index_values_as_list):
        self.df_excel.columns = column_name_list
        self.df_excel.set_index(index_values_as_list, inplace = True)
        self.df_excel.fillna(value=common_constants.DEFAULT_VALUES_FOR_FIELDS_DEFINITION_DF, inplace = True)

    def add_default_values(self, default_values):
        self.df_excel.fillna(value=default_values, inplace=True)
        
    def lowercase_df_column(self,col_list):   
        self.df_excel[col_list] = self.df_excel[col_list].applymap(lambda s:s.lower() if type(s) == str else s)

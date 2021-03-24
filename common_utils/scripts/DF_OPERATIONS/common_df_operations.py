'''
Created on 01-May-2020

@author: rithomas
'''
import pandas as pd
import numpy as np

def get_col_converted_toNumeric(df, column_list):
    return df[column_list].apply(pd.to_numeric)

def add_df_with_new_column_names(df, column_names_as_list):
    df.columns = column_names_as_list
    return df

def get_df_converted_toString(df):
    return df.applymap(str)

def get_combined_df(df1,df2):
    return df1.append(df2, sort=False)

def convert_NaN_toEmptyStrings(df):
    df = df.replace(np.nan, '', regex=True)
    return df

def replace_character_df(df):
    return df.apply(lambda x: x.str.replace(',','.'))

if __name__ == '__main__':
     data = {'Name':['Jai', 'Princi', 'Gaurav,Thomas', 'Anuj'], 
            'Age':[27, 24, 22, None], 
            'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'], 
            'Qualification':['Msc,BSC,PHD', 'MA', 'MCA', 'Phd']} 
     df = pd.DataFrame(data)
     print(df)
     
     df = convert_NaN_toEmptyStrings(df)
     print(df)
     
     df = replace_character_df(df) # Replaced to Gaurav.Thomas
     print(df)
     
     
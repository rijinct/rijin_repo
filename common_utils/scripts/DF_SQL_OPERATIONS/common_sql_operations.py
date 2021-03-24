'''
Created on 01-May-2020

@author: rithomas
'''
import pandas as pd

def get_df_merged_result(df1, df2, join_condition, column_list):
    return pd.merge(df1, df2, how=join_condition , on=column_list)

def get_explode_df(df, col_list):
        for val in col_list:
           df= df.assign(**{val: df[val].str.split(',')}).explode(val)
        df.drop_duplicates(inplace=True)
        return df

def get_lagged_column_df(df, column_to_be_lagged, group_by_col_list):
    return df.groupby(group_by_col_list)[column_to_be_lagged].shift(1)        
        
def get_sorted_df_based_column(df, sort_col_list):
    return df.sort_values(by=sort_col_list)

def drop_duplicate_based_on_columns(df, column_list, retain_val):
    df.drop_duplicates(subset=column_list ,keep=retain_val, inplace=True)        
    return df   

def get_first_in_GroupByList(df,group_by_col,column_to_apply_agg):
    return df.groupby(group_by_col)[column_to_apply_agg].first()

if __name__ == '__main__':
    
    data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj', 'Jai1'], 
            'Age':[27, 24, 22, 32, 27], 
            'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj', 'Delhi'], 
            'Qualification':['Msc,BSC,PHD', 'MA', 'MCA', 'Phd', 'Multi']} 
  
    data1 = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj', 'Jai1','Rijin'], 
            'Age':[27, 24, 22, 32, 27,32], 
            'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj', 'Delhi','Blore'], 
            'Qualification':['Msc,BSC,PHD', 'MA', 'MCA', 'Phd', 'Multi','BE']} 
    
    df = pd.DataFrame(data)
    df1 =  pd.DataFrame(data1)

#get merge of 2 df
    print(df)
    print(df1)
    print(get_df_merged_result(df, df1, 'right', 'Name'))
    exit(0)
# Convert the dictionary into DataFrame  
    df = pd.DataFrame(data)
    df1 =  pd.DataFrame(data1)
    print('------------------------------------ACTUAL DF-------------------------------------------------')
    print(df)       
    explode_col_list = ['Qualification']
    df_explode  = get_explode_df(df, explode_col_list)
    print('------------------------------------EXPLODED DF-------------------------------------------------')
    print(df_explode)
    
    print('------------------------------------LAGGED DF-------------------------------------------------')
    group_by_list = ['Name']
    df['lagged_col'] = get_lagged_column_df(df, 'Address', group_by_list)
    print(df)
    print('------------------------------------SORTED DF-------------------------------------------------')
    sort_col_list = ['Name', 'Age']
    print(get_sorted_df_based_column(df, sort_col_list))
    
    print('------------------------------------Removing Dup-------------------------------------------------')
    
    print(drop_duplicate_based_on_columns(df, ['Name','Age'], 'first'))
    
    print(get_first_in_GroupByList(df, 'Name', 'Qualification'))
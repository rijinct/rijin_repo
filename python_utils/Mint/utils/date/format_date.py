from df_util import DfUtil
from filesystem_util import FileSystemUtil as f
import os


df_util_csv_obj = DfUtil('merge.csv','merge_1.csv')
df_util_csv_obj.read_csv()
#df_util_csv_obj.extract_patterns_from_df('Date','Date1', '^(\d+/\d+)')

print(df_util_csv_obj.get_df())
df_util_csv_obj.extract_patterns_from_df('Date','Day', '(/\d+)')
df_util_csv_obj.extract_patterns_from_df('Date','Month', '^(\d+/)')

        #replace / values
df_util_csv_obj.replace_column_with_pattern('Day','/','')
df_util_csv_obj.replace_column_with_pattern('Month','/','')

df_util_csv_obj.concat_columns('Day','Month', 'Date_Merge')

df_util_csv_obj.create_csv()
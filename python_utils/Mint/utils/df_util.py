
import pandas as pd
import tabula
import os
from filesystem_util import FileSystemUtil as f
import subprocess

class DfUtil:

    def __init__(self, read_file=False, write_file=False, file_list=False):
        self.read_file = read_file
        self.write_file = write_file
        self.df = ''
        self.file_list = file_list
    
    def read_pdf(self):
        self.df = tabula.read_pdf(self.read_file, pages="all", stream=True, multiple_tables=True, guess=False)
        #print(self.df)


    def read_pdf_as_strings(self):
        col2str = {'dtype': str}
        # kwargs = {'output_format': 'dataframe',
        #   'pandas_options': col2str,
        #   'stream': True}
        #self.df = tabula.read_pdf(self.read_file, pages='all', output_format='dataframe', multiple_tables=True, pandas_options={'dtype':str, 'on_bad_lines':'skip'})[0]
        self.df = tabula.read_pdf(self.read_file, pages='all', output_format='dataframe', multiple_tables=True)
        self.add_new_col_names_df(Dynamic_col_names_flag=True)
        self.extract_patterns_from_df('col0','Date', '^(\d+/\d+)')
        self.df['col0'] = '"' + self.df['col0'].astype(str) + '"'
        self.df.to_csv('temp.csv')
        #print(self.df.iloc[:, 0])
        #exit(0)

    def write_pdf_to_csv(self):
        tabula.convert_into(self.read_file, self.write_file, output_format="csv", pages='all', stream=True,  guess=False)

    def write_df_to_csv(self):
        self.df.to_csv('df_csv.csv')

    def read_csv(self):
        #self.df = pd.read_csv(self.read_file, encoding='latin1', on_bad_lines='skip')
        self.df = pd.read_csv(self.read_file, encoding='latin1')

    def format_date(self, date_col):
        # self.df['series'] = 
        # self.df['day'] = pd.to_datetime(self.df[date_col], errors='ignore').day
        # self.df['month'] = pd.to_datetime(self.df[date_col],errors='ignore').month
        self.df['MM-DD'] = self.df[date_col].astype(str)

    def create_csv(self):
        self.df.to_csv(self.write_file, index=False)

    def get_df(self):
        return self.df
    
    def concat_df(self): 
        self.df = pd.concat([pd.read_csv(f) for f in self.file_list ], ignore_index=True)

    def get_rows_based_on_regex_input(self, col, regex_val):
        self.df = self.df[self.df[col].str.match(regex_val)]

    def extract_patterns_from_df(self, src_col, tgt_col, regex_pattern):
        self.df[tgt_col] = self.df[src_col].str.extract(regex_pattern, expand=True)

    def add_new_col_names_df(self, col_names_list=False, Dynamic_col_names_flag=False):
        #self.df1 = pd.DataFrame(self.df, columns=col_names_list)
        if (Dynamic_col_names_flag):
            #print('Dynamic Flag set')
            col_names_list = ['col{}'.format(val) for val in range(self.df.shape[1])]
            #print(col_names_list)

        self.df.columns = col_names_list
    
    def replace_column_with_pattern(self, src_col, value, replace_value):
        self.df[src_col] = self.df[src_col].str.replace(value,replace_value)

    def concat_columns(self, col1,col2, tgt_col):
        self.df[tgt_col] = self.df[col1] + '/' + self.df[col2]

    def convert_col_string(self, col):
        self.df[col] = '"' + self.df[col].astype(str) + '"'

    def add_header_to_file(self, header,file):
        cmd = "sed -i \"1i {h}\" {f}".format(h=header,f=file)
        subprocess.call(cmd)







###############################################################
# This class allows passing df as well as creating from a file
#
###############################################################

import pandas as pd


class DfUtil:

    def __init__(self, file=False):
        # if file is None:
        #     self.file = ''
        # else:
        #     self.file = file
        self.file = file
        self.df = ''

    def create_csv_df(self):
        self.df = pd.read_csv(self.file)

    def get_df(self):
        return self.df

    def drop_null_records(self, column_pattern):
        col_list = []
        for field in column_pattern:
            col_name = [col for col in self.df.columns if
                        field.lower() in col.lower()]
            if col_name:
                col_list.extend(col_name)
        self.df = self.df.dropna(subset=col_list, how='all')

    def filter_df(self, pattern):
        return self.df.filter(regex=pattern)


if __name__ == '__main__':
    data = {'Name': ['Tom', 'nick', 'krish', 'jack'],
            'Age': [20, 21, 19, 18]}

    # Create DataFrame
    df = pd.DataFrame(data)

    field_patter = ['pregnancy ID', 'unique ID']
    # df_util_obj = DfUtil('report.csv')
    # print(df)
    df_util_obj = DfUtil()
    df_util_obj.df = df
    count = len(df_util_obj.get_df())
    print(count)


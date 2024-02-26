
import sys
sys.path.append(r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\rij_git\rijin_repo\python_utils\Mint')
sys.path.append(r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\rij_git\rijin_repo\python_utils\Mint\utils')
sys.path.append(r'C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\rij_git\rijin_repo\python_utils\\5.PYTHON_OPERATIONS')
from utils.df_util import DfUtil
from utils.filesystem_util import FileSystemUtil as f
import os
import time
from utils.file_operations import move_files 

def create_CSV(file_loc, file):
    #print(file)
    input_file=file
    out_file=file.replace('pdf','csv')
    temp_file='temp.csv'
    os.chdir(file_loc)
    df_util_obj = DfUtil(input_file,temp_file)
    df_util_obj.read_pdf()
    df_util_obj.write_pdf_to_csv()
    time.sleep(2)
    col_names="col1,col2,col3,col4,col5,col6,col7,col8,col9,col10, col11,col12"
    df_util_obj.add_header_to_file(col_names, temp_file)
            
    df_util_csv_obj = DfUtil(temp_file,out_file)
    df_util_csv_obj.read_csv()
            #print(df_util_csv_obj.get_df())
    df_util_csv_obj.add_new_col_names_df(Dynamic_col_names_flag=True)
            #df_util_csv_obj.extract_patterns_from_df('col0','Date', '^(\d+/\d+)')
            #df_util_csv_obj.convert_col_string('Date')
    df_util_csv_obj.create_csv()

            #Transform the CSV
            #col_names = ['col1','col2','col3','col4','col5']
            #df_util_csv_obj.add_new_col_names_df(Dynamic_col_names_flag=True)

            #Match Date pattern and split it as Day and month to interchange
            #df_util_csv_obj.get_rows_based_on_regex_input('col1','^\d\d')
            #df_util_csv_obj.extract_patterns_from_df('col0','Date', '^(\d+/\d+)')
            #df_util_csv_obj.extract_patterns_from_df('col0','Day', '(/\d+)')
            #df_util_csv_obj.extract_patterns_from_df('col0','Month', '^(\d+/)')

            #replace / values
            # df_util_csv_obj.replace_column_with_pattern('Day','/','')
            # df_util_csv_obj.replace_column_with_pattern('Month','/','')

            # df_util_csv_obj.concat_columns('Day','Month', 'Date_Merge')

    df_util_csv_obj.create_csv()

if __name__ == '__main__':

    #file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\test"
    file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\html"
    skip_file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\SEC\SEC\SEC_SKIP"
    #file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\Rijin\BT\data\X4921 (2021_01-2022_08)"

    #Transforming PDF object
    for file in os.listdir(file_loc):
            try: 
                create_CSV(file_loc, file)
            except:
                 print("file:{} failed".format(file))
                 move_files(file, skip_file_loc)
                 


    ##Merge PDF object\    
    fileSystem_obj = f(dir_path=file_loc,file_format='csv')
    file_list = fileSystem_obj.get_file_list()
    df_util_merge_obj = DfUtil(file_list=file_list,write_file='{}\merge.csv'.format(file_loc))
    df_util_merge_obj.concat_df()
    df_util_merge_obj.create_csv()

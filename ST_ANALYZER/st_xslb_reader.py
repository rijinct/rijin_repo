import pandas as pd
from pyxlsb import open_workbook as open_xlsb
import os
import sched, time
import sys
import glob
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
from df_db_util import dfDbUtil
import constants
s = sched.scheduler(time.time, time.sleep)


def get_latest_file_or_folder(zerodha_file_path):
    os.chdir(zerodha_file_path)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    oldest = files[0]
    newest = files[-1]
    return newest

def get_latest_file_or_folder_based_pattern(zerodha_file_path, pattern):
    files = glob.glob(zerodha_file_path + '\\' + pattern +'*')
    latest_file =  max(files, key=os.path.getctime)
    return latest_file
    
def read_xlsb_and_return_as_list(xlsb_file):
    df = []
    with open_xlsb(xlsb_file) as wb: 
        with wb.get_sheet(1) as sheet:
            for row in sheet.rows():
                df.append([item.v for item in row])
    return df


def get_list_as_df(df_list):
    df = pd.DataFrame(df_list[1:], columns=df_list[0])
    return df


def add_df_with_new_column_names(df, column_names_as_list):
    df.columns = column_names_as_list
    return df


def read_from_xlsb_insert_db():
    db_util_obj = dfDbUtil(constants.MARIADB_URL)
    latest_default_folder = get_latest_file_or_folder_based_pattern(constants.EXCEL_PATH,'Default')
    print('rij',latest_default_folder)
    latest_xlsb_file = get_latest_file_or_folder_based_pattern(latest_default_folder, 'Default')
    print(latest_xlsb_file)
    df_list = read_xlsb_and_return_as_list(latest_xlsb_file)
    df = get_list_as_df(df_list)
    column_list = ['Trading_symbol', 'LTP', 'Bid_qty', 'Bid_rate', 'Ask_rate', 'Ask_qty' , 'LTQ'  , 'Open' , 'High' , 'Low', 'Prev_close' , 'Volume_traded_today', 'Open_interest', 'ATP', 'Total_bid_qty', 'Total_ask_qty', 'Exchange', 'LTT' , 'LUT']
    df = add_df_with_new_column_names(df, column_list)
    df[['Volume_traded_today','LTP']] = df[['Volume_traded_today','LTP']].apply(pd.to_numeric)
    df['cumVol_X_Ltp'] = df['LTP'] * df['Volume_traded_today']
    db_util_obj.insert_db_df(df, 'st_analysis', 'testrijdb', 'append', False)
    #s.enter(180, 1, read_from_xlsb_insert_db, (sc,))
    return df

def main():
    global db_util_obj
    db_util_obj = dfDbUtil(constants.MARIADB_URL)
    s.enter(1, 1, read_from_xlsb_insert_db, (s,))
    s.run()


if __name__ == "__main__":
    main()

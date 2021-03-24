'''
Created on 25-Apr-2020

@author: rithomas
'''
from pyxlsb import open_workbook as open_xlsb

def read_xlsb_and_return_as_list(xlsb_file):
        df_list = []
        with open_xlsb(xlsb_file) as wb: 
            with wb.get_sheet(1) as sheet:
                for row in sheet.rows():
                    df_list.append([item.v for item in row])
        return df_list
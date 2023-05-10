'''
Created on 24-Apr-2020

@author: rithomas
'''

import pandas as pd
import xml.etree.ElementTree as et
from pyxlsb import open_workbook as open_xlsb
from flatten_json import flatten 
from pandas.io.json._normalize import json_normalize
        

def get_df_from_xml_with_df_columns(xml_file, xml_df_columns):    
    xtree = et.parse(xml_file)                                                                 # For a xml structure like 
    root_el = xtree.getroot()                                                                       # <universes>                --> root_el
    rows = []                                                                                       #    <universe>              --> ch_root_el
    for ch_root_el in root_el:                                                                      #        <Attributes>        --> ch_ch_root_el
        for ch_ch_root_el in ch_root_el:                                                            #            <Attributr>     --> ch_ch_ch_root_el
            for ch_ch_ch_root_el in ch_ch_root_el:                                                  #            </Attributr>
                res = []                                                                            #        </Attributes>
                for el in xml_df_columns[0:]:                                                       #    </universe>
                    if ch_ch_ch_root_el is not None and ch_ch_ch_root_el.find(el) is not None:      # <universes>
                        res.append(ch_ch_ch_root_el.find(el).text)
                    else: 
                        res.append(None)
                rows.append({xml_df_columns[i]: res[i] 
                    for i, _ in enumerate(xml_df_columns)})
    df_from_xml = pd.DataFrame(rows, columns=xml_df_columns)        
    return df_from_xml

def get_df_from_csv(csv_file, header):
    df_from_csv = pd.read_csv(csv_file, header=header)
    return df_from_csv
    
def get_df_from_excel(excel_file,header,sheet_name):
    if sheet_name is None:
        sheet_name = 0       
    df_from_csv = pd.read_excel(excel_file, header=header, sheet_name=sheet_name)
    return df_from_csv
    
def get_df_from_sql(sql_query, db_engine):
    df_from_sql = pd.read_sql(sql_query, db_engine)
    return df_from_sql

def get_df_from_list(item_list):
    df_from_list = pd.DataFrame(item_list[1:], columns=item_list[0])
    return df_from_list

def get_df_from_xlsb(xlsb_file):
    df_list = []
    with open_xlsb(xlsb_file) as wb: 
        with wb.get_sheet(1) as sheet:
            for row in sheet.rows():
                df_list.append([item.v for item in row])
    df_from_xlsb = pd.DataFrame(df_list[1:], columns=df_list[0])   
    return df_from_xlsb
             
def get_df_from_json_file(json_file):
    df = pd.read_json(json_file) ## This works for non-nested Jsons
    return df


def get_df_from_nested_json(json_nested_data):
    flattened_json = flatten(json_nested_data)
    df = json_normalize(flattened_json)
    return df

if __name__ == '__main__':
    
    input_json_file = r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\jsonSample.json'
    data = {"tradedDate": "22MAY2020", "data": [{"pricebandupper": "167.10", "symbol": "SBIN"}],  "otherSeries": ["EQ", "N2", "N5", "N6"]}
    
    print(get_df_from_json_file(input_json_file))
    
    print(get_df_from_nested_json(data))
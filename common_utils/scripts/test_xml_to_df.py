'''
Created on 06-May-2020

@author: rithomas
'''
import sys
sys.path.insert(0, r"C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\scripts")
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df
import pandas as pd
import xml.etree.ElementTree as et

def get_df_from_xml_with_df_columns(xml_file, xml_df_columns):    
    xtree = et.parse(xml_file)                                                                 # For a xml structure like 
    root_el = xtree.getroot()                                                                       # <universes>                --> root_el
    rows = []                                                                                       #    <universe>              --> ch_root_el
    for ch_root_el in root_el:
        print(ch_root_el.tag)                                                                      #        <Attributes>        --> ch_ch_root_el                                               #            </Attributr>
        res = []                                                                            #        </Attributes>
        for el in xml_df_columns[0:]:
            print(el)                                                       #    </universe>
            if ch_root_el is not None and ch_root_el.find(el) is not None:      # <universes>
                res.append(ch_root_el.find(el).text)
            else: 
                res.append(None)
        rows.append({xml_df_columns[i]: res[i] 
                    for i, _ in enumerate(xml_df_columns)})
    df_from_xml = pd.DataFrame(rows, columns=xml_df_columns)        
    return df_from_xml
     

if __name__ == '__main__':
    
    input_xml_filename = r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\test.xml' 
    xml_df_columns = ['tablename','columnname','definition', 'ultimateSourcetable', 'ultimateSourceColumns']
    df = get_df_from_xml_with_df_columns(input_xml_filename,xml_df_columns)
    #df = get_df_from_xml(input_xml_filename)
    print(df)
    ultimate_source_col = 'ultimateSourceColumns'
    ultimate_source_tbl = 'ultimateSourcetable'
    #print(df[['tablename','ultimateSourcetable', 'ultimateSourceColumns']])
    df = df.assign(ultimateSourceColumns=df[ultimate_source_col].str.split(',')).explode(ultimate_source_col)
    print(df[['tablename','ultimateSourcetable', 'ultimateSourceColumns']])
    df = df.assign(ultimateSourcetable=df[ultimate_source_tbl].str.split(',')).explode(ultimate_source_tbl)
    print(df)
    df.drop_duplicates(inplace=True) 
    print(df[['tablename','ultimateSourcetable', 'ultimateSourceColumns']])
    
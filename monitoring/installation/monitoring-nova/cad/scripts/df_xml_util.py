'''
Created on 06-May-2020

@author: rithomas
'''
import pandas as pd
import xml.etree.ElementTree as et
import common_constants

class dfXmlUtil():
    def __init__(self,input_xml,output_xml, df_cols):
        self.input_xml = input_xml
        self.output_xml = output_xml
        self.df_from_xml = ''
        self.df_cols = df_cols

    def convert_xml_df(self):    
        xtree = et.parse(self.input_xml)                                                                
        root_el = xtree.getroot()                                                                       
        rows = []                                                                                      
        for ch_root_el in root_el: 
            res = []                                                                   
            for el in self.df_cols[0:]:                                                      
                if ch_root_el is not None and ch_root_el.find(el) is not None:      
                    res.append(ch_root_el.find(el).text)
                else: 
                    res.append(None)
            rows.append({self.df_cols[i]: res[i] 
                        for i, _ in enumerate(self.df_cols)})
        self.df_from_xml = pd.DataFrame(rows, columns=self.df_cols)
     
    def get_df_from_xml(self):
        return self.df_from_xml

    def explode_df(self,col_list):
        for val in col_list:
            self.df_from_xml= self.df_from_xml.assign(**{val: self.df_from_xml[val].str.split(',')}).explode(val)
        self.df_from_xml.drop_duplicates(inplace=True)
     
    def create_xml_from_df(self,df, tree_hierarchy_list):
        df_to_dict = df.to_dict(orient='records')
        tree = et.ElementTree()
        root = et.Element(tree_hierarchy_list[0])
        for dict_val in df_to_dict:
            row = et.Element(tree_hierarchy_list[1])
            for key,val in dict_val.items():
                et.SubElement(row, key).text = val
            root.append(row)
        tree._setroot(root)
        tree.write(self.output_xml);  
'''
Created on 24-Apr-2020

@author: rithomas
'''

import pandas as pd
import xml.etree.ElementTree as et

class variousInputFormatToDf:
    def __init__(self,xml_file,csv_file, xml_df_columns):
        self.xml_file = xml_file
        self.csv_file = csv_file
        self.excel_file = csv_file
        self.xml_df_columns = xml_df_columns
        self.df_from_xml = ''
        
    def get_df(self):
        return self.df_from_xml

    def get_df_from_xml_with_df_columns(self):    
        xtree = et.parse(self.xml_file)
        root_el = xtree.getroot()
        rows = []   
        for ch_root_el in root_el:
            for ch_ch_root_el in ch_root_el:
                #for ch_ch_ch_root_el in ch_ch_root_el:
                    res = []
                    for el in self.xml_df_columns[0:]: 
                        if ch_ch_root_el is not None and ch_ch_root_el.find(el) is not None:
                            res.append(ch_ch_root_el.find(el).text)
                        else: 
                            res.append(None)
                    rows.append({self.xml_df_columns[i]: res[i] 
                        for i, _ in enumerate(self.xml_df_columns)})
        self.df_from_xml = pd.DataFrame(rows, columns=self.xml_df_columns)        
        return self.df_from_xml
    
    
    def create_csv_from_df(self):
        self.df_from_xml.to_csv(self.csv_file, index=False)

    def create_excel_from_df(self):
        self.df_from_xml.to_excel(self.csv_file, index=False)
    
    
    def filter_df_rows(self,key_column, filter_list):
        self.df_from_xml = self.df_from_xml[self.df_from_xml[key_column].isin(filter_list)]
    
if __name__ == "__main__":

    #file_name = 'dataQualityResults_US'
    file_name = 'Pre-Prod\dataQualityStatistics_2021-03-26_14-15-45'
    xml_file = r"C:\Users\rithomas\Desktop\NokiaCemod\14. MyProjects\rijin_git\rijin_repo\common_utils\conf\{f}{e}" \
               r"".format(f=file_name,e='.xml')
    csv_file = r"C:\Users\rithomas\Desktop\NokiaCemod\14. MyProjects\rijin_git\rijin_repo\common_utils\conf\{f}{" \
               r"e}".format(f=file_name,e='.xlsx')
    #xml_df_columns = ['kpiName','definition','descriptiveExample','businessRule','calculationRule','LogicalTable',
    # 'LogicalColumn','Status']
    #xml_df_columns = ['schemaName', 'databaseType', 'tableID', 'tableName', 'fieldID', 'fieldName', 'ruleID',
         #             'ruleName', 'ruleDescription', 'typeID', 'objectType', 'supportDataType', 'threshold',
          #            'totalRecord', 'badRecord', 'score', 'isCheckPass']

    xml_df_columns = ['objectType', 'parentObjectID', 'schemaName', 'databaseType', 'objectID', 'objectName', 'objectDescription', 'score', 'completenessScore', 'conformityScore', 'consistencyscore', 'integrityScore', 'rangeScore', 'uniquenessScore']
    filter_list = ['Subscriber Group']    
    xml_to_csv_obj = variousInputFormatToDf(xml_file,csv_file,xml_df_columns)    
    xml_to_csv_obj.get_df_from_xml_with_df_columns()
    #xml_to_csv_obj.filter_df_rows('kpiName',filter_list)
    print(xml_to_csv_obj.get_df())
    xml_to_csv_obj.create_excel_from_df()

import pandas as pd
import xml.etree.ElementTree as et

class xmlTocsv:
    def __init__(self,xml_file,csv_file, csv_columns):
        self.xml_file = xml_file
        self.csv_file = csv_file
        self.csv_columns = csv_columns
        self.df = ''
        
    def get_df_from_xml(self):
        return self.df

    def create_df_from_xml(self):    
        xtree = et.parse(self.xml_file)
        root_el = xtree.getroot()
        rows = []   
        for ch_root_el in root_el:
            for ch_ch_root_el in ch_root_el:
                for ch_ch_ch_root_el in ch_ch_root_el: 
                    res = []
                    for el in self.csv_columns[0:]: 
                        if ch_ch_ch_root_el is not None and ch_ch_ch_root_el.find(el) is not None:
                            res.append(ch_ch_ch_root_el.find(el).text)
                        else: 
                            res.append(None)
                    rows.append({self.csv_columns[i]: res[i] 
                        for i, _ in enumerate(self.csv_columns)})
        self.df = pd.DataFrame(rows, columns=self.csv_columns)        
    
    def create_csv_from_df(self):
        self.df.to_csv(self.csv_file, index=False)
    
    
    def filter_df_rows(self,key_column, filter_list):
        self.df = self.df[self.df[key_column].isin(filter_list)]

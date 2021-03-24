'''
Created on 04-May-2020

@author: rithomas
'''
from sqlalchemy import create_engine
import xml.etree.ElementTree as et
import pandas as pd
import DF_OPERATIONS.common_df_operations as com_op

def insert_db_df(df, engine, table_name, schema, action, index):
    df.to_sql(table_name, engine, schema=schema, if_exists=action, index=index)
    
def create_xml_from_df(df, tree_hierarchy_list,output_xml):
    df_to_dict = df.to_dict(orient='records')                                   # concept is, wanted every record in a row tag, for that will convert every record to dictionary
    tree = et.ElementTree()                                                     #a and write that dictionary to a xml subelement. 
    root = et.Element(tree_hierarchy_list[0])
    for dict_val in df_to_dict:
        row = et.Element(tree_hierarchy_list[1])
        for key,val in dict_val.items():
            et.SubElement(row, key).text = val
        root.append(row)
    et.dump(root)
    tree._setroot(root)
    tree.write(output_xml);


if __name__ == '__main__':
     data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'], 
            'Age':[27, 24, 22, 32], 
            'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'], 
            'Qualification':['Msc,BSC,PHD', 'MA', 'MCA', 'Phd']} 
     df = pd.DataFrame(data)
     df = com_op.get_df_converted_toString(df)
     print(df)
     tree_hierarchy_list = ['root','row']
     output_xml = r'C:\Users\rithomas\Desktop\myCygwin\RIJIN_PROJECT\common\conf\outputXml.xml'
     create_xml_from_df(df, tree_hierarchy_list, output_xml)
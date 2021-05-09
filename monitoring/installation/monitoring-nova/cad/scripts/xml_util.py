import xml.etree.ElementTree as ET
import subprocess
import lxml.etree
import os
import sys
from  df_xml_util import dfXmlUtil
import common_constants
from dbConnectionUtil import DbConnectionUtil
import pandas as pd

def generate_ultimateSourceField_to_xml(input_xml_filename, output_xml_filename, xml_df_columns):
    df_xml_util_obj = dfXmlUtil(input_xml_filename, output_xml_filename, xml_df_columns)
    df_xml_util_obj.convert_xml_df()
    df_xml_util_obj.explode_df(common_constants.EXPLODE_COL_LIST)
    df_xml = df_xml_util_obj.get_df_from_xml().replace('NONE', '', regex=True)
    df_xml['formula'] = df_xml['formula'].str.strip().str.upper()
    df_xml[common_constants.EXPLODE_COL_LIST] = df_xml[common_constants.EXPLODE_COL_LIST].apply(lambda x: x.str.replace(",",""))
    df_db_writtenBylist = DbConnectionUtil().getDataframeFromSql(common_constants.WRITTEN_BY_LIST_METADATA_QUERY)
    df_db_readbylist = DbConnectionUtil().getDataframeFromSql(common_constants.READ_BY_LIST_METADATA_QUERY)
    df_db_esTablelist = DbConnectionUtil().getDataframeFromSql(common_constants.ES_TABLE_QUERY)
    df_xml_dbWritten_merge = get_df_merged_result(df_xml, df_db_writtenBylist, common_constants.JOIN_CONDITION, common_constants.JOIN_COL_LIST)
    df_xml_unmatch = df_xml.merge(df_xml_dbWritten_merge, how = 'outer',on=common_constants.UNIQ_COL_KEYLIST,indicator=True).loc[lambda x : x['_merge']=='left_only']
    df_xml_unmatch.drop(common_constants.TECH_META_DROP_COL_LIST,axis=1,inplace=True)
    df_xml_unmatch.columns = common_constants.TECH_META_COL_LIST
    df_xml_unmatch['formula'] = df_xml_unmatch['columnname']
    df_merge_final = df_xml_dbWritten_merge.append(df_xml_unmatch,sort=False).applymap(str)
    df_merge_final = get_df_merged_result(df_merge_final, df_db_readbylist, 'left', 'readbylist')
    df_merge_final.rename(columns={'writtenbylist_x':'writtenbylist'}, inplace=True)
    df_merge_final.drop(['writtenbytabledescription_x','writtenbytabledescription_y','writtenbylist_y'],axis=1,inplace=True)
    df_merge_final = get_df_merged_result(df_merge_final, df_db_readbylist, 'left', 'writtenbylist')
    df_merge_final.rename(columns={'readbylist_x':'readbylist'}, inplace=True)
    df_merge_final.rename(columns={'readbytabledescription_x':'readbytabledescription'}, inplace=True)
    df_merge_final.drop(['readbytabledescription_y','readbylist_y'],axis=1,inplace=True)
    df_merge_final.sequence = df_merge_final.sequence.astype('float')
    df_merge_final = df_merge_final.sort_values(['tablename','sequence'], ascending=[True, True])
    df_merged_result_ES = get_df_merged_result(df_xml, df_db_esTablelist, common_constants.JOIN_CONDITION, 'tablename')
    df_merge_final = df_merge_final.append(df_merged_result_ES,sort=False).applymap(str).replace('None',' ').replace('nan',' ')
    df_xml_util_obj.create_xml_from_df(df_merge_final, common_constants.TREE_HIERARCHY_LIST)

def modify_and_regenerate_xml(input_xml_filename, output_xml_filename, tag_xpath_loc, executor, subproc_script):
    tree = ET.parse(input_xml_filename)
    root = tree.getroot()
    for elem in root.findall(tag_xpath_loc):
        process = subprocess.Popen([executor, subproc_script, "{}".format(elem.text)], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].decode('utf-8').strip()
        elem.text = str(process).replace('[', '').replace(']', '')
    tree.write(output_xml_filename)
    
def transform_xml(process_xsl_filename, input_xml_filename,output_xml_filename, write_format):
    parser = lxml.etree.XMLParser(ns_clean=True,recover=True)
    input_xml_parser = lxml.etree.parse(input_xml_filename,parser)
    process_xsl = lxml.etree.parse(process_xsl_filename)
    xsl_transform = lxml.etree.XSLT(process_xsl)
    transform_xml = xsl_transform(input_xml_parser)
    if write_format == 'a':
        write_or_append_to_xml(output_xml_filename,transform_xml,write_format)
    else:
        write_or_append_to_xml(output_xml_filename,transform_xml,write_format)
        
def get_absolute_filename(output_filename):
    return os.popen("readlink -f {0}".format(output_filename)).read()

    
def write_or_append_to_xml(output_xml_filename, transform_xml, write_format):  
    with open(output_xml_filename,write_format) as f:
        f.write(str(transform_xml))
    f.close()
        
def append_root_to_xml(filename):
    f = open(filename,'r+')
    lines = f.readlines()
    f.seek(0)
    f.write("<Universes>") 
    for line in lines: 
        f.write(line)
    f.write("</Universes>")
    f.close()

def get_custom_default_filename(custom_file_name, product_file_name):
    if os.path.exists(custom_file_name):
        file = custom_file_name
    else:
        file = product_file_name
    return file

def get_df_merged_result(df1, df2, join_condition, join_col_list):
    return pd.merge(df1, df2, how=join_condition , left_on=join_col_list, right_on=join_col_list)
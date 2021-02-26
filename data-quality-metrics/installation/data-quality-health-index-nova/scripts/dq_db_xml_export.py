'''
Created on 13-Nov-2020

@author: rithomas
'''
import os
import sys
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor
from com.rijin.dqhi import constants


def write_to_xml(table, xml_path):
    f = open(xml_path, "w")
    f.write(prettify(table))
    f.close()

def generate_data_quality_rule_xml(sqlite_db_query_executor,filename,compute_date): 
    data_quality_rule =constants.DATA_QUALITY_RULE_QUERY_NOVA.format(compute_date)
    result_set = sqlite_db_query_executor.fetch_result(data_quality_rule)
    table = Element("dataQualityRule")
    root_list = Element("ruleList")
    for record in result_set:
        list_of_tags = [
                        "ruleID:" + str(record[0]),
                        "ruleClass:" + str(record[1]),
                        "ruleName:" + str(record[2]),
                        "ruleCondition:" + str(record[3]),
                        "ruleDescription:" + str(record[4]),
                        "typeID:" + str(record[5]),
                        "objectType:" + str(record[6]),
                        "schemaName:" + str(record[7]),
                        "databaseType:" + str(record[8]),
                        "tableID:" + str(record[9]),
                        "tableName:" + str(record[10]),
                        "fieldID:" + str(record[11]),
                        "fieldName:" + str(record[12]),
                        "supportDataType:" + str(record[13]),
                        "threshold:" + str(record[14]),
                        "action:" + str(record[15]),
                        "valueRange:" + str(record[16]),
                        "enumeration:" + str(record[17]),
                        "auditExpression:" + str(record[18]),
                        "fieldLength:" + str(record[19]),
                        "action:" + str(record[20]),
                        ]
        root_tag = Element("rule")
        root_list.append(root_tag)
        for tag in list_of_tags:
            tag_name = tag.split(":")[0]
            tag_value = tag.split(":")[1]
            tag = SubElement(root_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    write_to_xml(table, filename)

def generate_data_quality_result_xml(sqlite_db_query_executor, filename,compute_date):
    data_quality_result_query = constants.DATA_QUALITY_RESULT_QUERY_NOVA.format(compute_date)
    result_set = sqlite_db_query_executor.fetch_result(data_quality_result_query)
    table = Element("dataQualityResult")
    root_list = Element("resultList")
    for record in result_set:
        list_of_tags = [
                        "schemaName:" + str(record[0]),
                        "databaseType:" + str(record[1]),
                        "tableID:" + str(record[2]),
                        "tableName:" + str(record[3]),
                        "fieldID:" + str(record[4]),
                        "fieldName:" + str(record[5]),
                        "ruleID:" + str(record[6]),
                        "ruleName:" + str(record[7]),
                        "ruleDesc:" + str(record[8]),
                        "typeID:" + str(record[9]),
                        "objectType:" + str(record[10]),
                        "supportDataType:" + str(record[11]),
                        "threshold:" + str(record[12]),
                        "totalRecord:" + str(record[13]),
                        "badRecord:" + str(record[14]),
                        "score:" + str(record[15]),
                        "isCheckPass:" + str(record[16])

                        ]
        root_tag = Element("rule")
        root_list.append(root_tag)
        for tag in list_of_tags:
            tag_name = tag.split(":")[0]
            tag_value = tag.split(":")[1]
            tag = SubElement(root_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    write_to_xml(table, filename)


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def generate_data_quality_statistics_xml(sqlite_db_query_executor,filename, compute_date):
    data_quality_statistics = constants.DATA_QUALITY_STATISTICS_QUERY_NOVA.format(compute_date)
    result_set = sqlite_db_query_executor.fetch_result(data_quality_statistics)
    table = Element("dataQualityResult")
    root_list = Element("resultList")  
    for record in result_set:
        list_of_tags = [
                        "objectType:" + str(record[0]),
                        "parentObjectID:" + str(record[1]),
                        "schemaName:" + str(record[2]),
                        "databaseType:" + str(record[3]),
                        "objectID:" + str(record[4]),
                        "objectName:" + str(record[5]),
                        "objectDescription:" + str(record[6]),
                        "score:" + str(record[7]),
                        "completenessScore:" + str(record[8]),
                        "conformityScore:" + str(record[9]),
                        "consistencyScore:" + str(record[10]),
                        "integrityScore:" + str(record[11]),
                        "rangeScore:" + str(record[12]),
                        "uniquenessScore:" + str(record[13])
                        
                        ]
        statistics_tag = Element("statistics")
        root_list.append(statistics_tag)
        for tag in list_of_tags:
            tag_name = tag.split(":")[0]
            tag_value = tag.split(":")[1]
            tag = SubElement(statistics_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    write_to_xml(table, filename)

def execute(compute_date,filename,output_filename):
    sqlite_db_connection_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
    sqlite_db_query_executor = SqliteDBExecutor(sqlite_db_connection_obj)
    if filename == 'dataQualityRule':
        generate_data_quality_rule_xml(sqlite_db_query_executor,output_filename,compute_date)
    elif filename == 'dataQualityResults':
        generate_data_quality_result_xml(sqlite_db_query_executor,output_filename,compute_date)
    else:
        generate_data_quality_statistics_xml(sqlite_db_query_executor,output_filename,compute_date)
    
    



if __name__ == '__main__':
    execute()
    

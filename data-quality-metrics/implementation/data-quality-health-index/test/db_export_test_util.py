import os
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring
import sys
sys.path.insert(0,'com/rijin/dqhi')
from connection_wrapper import ConnectionWrapper
from query_executor import DBExecutor

def main():
    generated_data_quality_rule()
    generated_data_quality_result()
    genereate_data_quality_statistics()


def generated_data_quality_rule():
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    currentDate = "2017-02-05"
    data_quality_rule = "select 'project' as 'schemaName', 'BigData' as 'databaseType', b.ruleid as 'ruleID',a.rule_class as 'ruleClass',a.rule_presentation_name as 'ruleName',b.rule_condition as 'ruleCondition',a.rule_description as 'ruleDescription',b.dimension as 'typeID','field' as 'objectType',c.table_id as 'tableID', c.table_name as 'tableName', c.column_id as 'fieldID',c.column_name as 'fieldName',  c.column_datatype as 'supportDataType',b.threshold as 'threshold',((case when b.dimension = 'RANGE' then b.rule_parameter  else '' end)) as 'valueRange',((case when b.dimension = 'CONFORMITY' then b.rule_parameter  else '' end)) as 'enumeration', '' as 'auditExpression', ((case when b.dimension = 'CONFORMITY' then CAST(c.column_precision AS varchar)  else '' end)) as 'fieldLength','1' as 'action' from dq_rules_definition a JOIN dq_fields_score b ON a.rule_id=b.ruleid AND b.source in ('CEM') JOIN (select * from dq_fields_definition where id in (select max(id) from dq_fields_definition where computed_date='{}' group by table_name,column_name)) c ON b.field_def_id=c.id;".format(currentDate);
    result_set = sai_db_query_executor.fetch_result(data_quality_rule)
    table = Element("dataQualityRule")
    root_list = Element("ruleList")
    for record in result_set:
        list_of_tags = [
                        "ruleID," + str(record[2]),
                        "ruleClass," + str(record[3]),
                        "ruleName," + str(record[4]),
                        "ruleCondition," + str(record[5]),
                        "ruleDescription," + str(record[6]),
                        "typeID," + str(record[7]),
                        "objectType," + str(record[8]),
                        "schemaName," + str(record[0]),
                        "databaseType," + str(record[1]),
                        "tableID," + str(record[9]),
                        "tableName," + str(record[10]),
                        "fieldID," + str(record[11]),
                        "fieldName," + str(record[12]),
                        "supportDataType," + str(record[13]),
                        "threshold," + str(record[14]),
                        "valueRange," + str(record[15]),
                        "enumeration," + str(record[16]),
                        "fieldLength," + str(record[18]),
                        "auditExpression," + str(record[17]),
                        "action," + str(record[19]),
                        ]
        root_tag = Element("rule")
        root_list.append(root_tag)
        for tag in list_of_tags:
            tag_name = tag.split(",")[0]
            tag_value = tag.split(",")[1]
            tag = SubElement(root_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    xml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/actual/dataQualityRule.xml"))
    f = open(xml_path, "w")
    f.write(prettify(table))
    f.close()
  

def generated_data_quality_result():
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    data_quality_result_query = "select * from (select c.table_id as 'tableID', c.table_name as 'tableName', c.column_id as 'fieldID',c.column_name as 'fieldName','project' as 'schemaName',b.ruleid as 'ruleID',a.rule_presentation_name as 'ruleName',a.rule_description as 'ruleDescription', b.dimension as 'typeID','field' as 'objectType', c.column_datatype as 'supportDataType',b.threshold as 'threshold', b.total_row_count as 'totalRecord', b.bad_record_count as 'badRecord', b.score as 'score', b.check_pass as 'isCheckPass', strftime('%Y/%m/%d' , c.computed_date) as 'dataTime', strftime('%Y/%m/%d %H:%M:%S', b.start_time) as 'startTime',strftime('%Y/%m/%d %H:%M:%S', b.end_time) as 'endTime' , 'BigData' as 'databaseType' from dq_rules_definition a JOIN dq_fields_score b ON a.rule_id=b.ruleid AND b.source in ('CEM') JOIN (select * from dq_fields_definition where id in (select max(id) from dq_fields_definition where computed_date='2017-02-05' group by table_name,column_name)) c ON b.field_def_id=c.id) order by tableID,fieldID,ruleID"
    result_set = sai_db_query_executor.fetch_result(data_quality_result_query)
    table = Element("dataQualityResult")
    root_list = Element("ruleList")
    for record in result_set:
        list_of_tags = [
                        "schemaName," + str(record[4]),
                        "databaseType," + str(record[19]),
                        "tableID," + str(record[0]),
                        "tableName," + str(record[1]),
                        "fieldID," + str(record[2]),
                        "fieldName," + str(record[3]),
                        "ruleID," + str(record[5]),
                        "ruleName," + str(record[6]),
                        "ruleDesc," + str(record[7]),
                        "typeID," + str(record[8]),
                        "objectType," + str(record[9]),
                        "supportDataType," + str(record[10]),
                        "threshold," + str(record[11]),
                        "totalRecord," + str(record[12]),
                        "badRecord," + str(record[13]),
                        "score," + str(record[14]),
                        "isCheckPass," + str(record[15])

                        ]
        root_tag = Element("rule")
        root_list.append(root_tag)
        for tag in list_of_tags:
            tag_name = tag.split(",")[0]
            tag_value = tag.split(",")[1]
            tag = SubElement(root_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    xml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/actual/dataQualityResult.xml"))
    f = open(xml_path, "w")
    f.write(prettify(table))
    f.close()


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def genereate_data_quality_statistics():
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    data_quality_rule = "select object_type as 'objectType',((case when object_type = 'KPI' then ''  else 'project' end)) as 'schemaName', 'BigData' as 'databaseType',((case when object_type = 'KPI' then ''  else CAST(parent_object_id AS varchar) end)) as 'parentObjectID',object_id as 'objectID', kpi_or_cde_name as 'objectName', kpi_or_cde_description as 'objectDescription', overall_score as 'score', ((case when completeness_score = '-1' then 'NA'  else CAST(completeness_score AS varchar) end)) as 'completenessScore',((case when conformance_score = '-1' then 'NA'  else CAST(conformance_score AS varchar) end)) as 'conformityScore',((case when consistence_score = '-1' then 'NA'  else CAST(consistence_score AS varchar) end)) as 'consistencyScore',((case when integrity_score = '-1' then 'NA'  else CAST(integrity_score AS varchar) end)) as 'integrityScore',((case when range_score = '-1' then 'NA'  else CAST(range_score AS varchar) end)) as 'rangeScore',((case when uniqueness_score = '-1' then 'NA'  else CAST(uniqueness_score AS varchar) end)) as 'uniquenessScore' from dq_kpi_cde_scores where computed_date=strftime('%Y-%m-%d', 'now','-1 days') and load_date=(select max(load_date) from dq_kpi_cde_scores)"
    result_set = sai_db_query_executor.fetch_result(data_quality_rule)
    table = Element("dataQualityResult")
    root_list = Element("resultList")  
    for record in result_set:
        list_of_tags = [
                        "objectType," + str(record[0]),
                        "parentObjectID," + str(record[3]),
                        "schemaName," + str(record[1]),
                        "databaseType," + str(record[2]),
                        "objectID," + str(record[4]),
                        "objectName," + str(record[5]),
                        "objectDescription," + str(record[6]),
                        "score," + str(record[7]),
                        "completenessScore," + str(record[8]),
                        "conformityScore," + str(record[9]),
                        "consistencyScore," + str(record[10]),
                        "integrityScore," + str(record[11]),
                        "rangeScore," + str(record[12]),
                        "uniquenessScore," + str(record[13])
                        
                        ]
        statistics_tag = Element("statistics")
        root_list.append(statistics_tag)
        for tag in list_of_tags:
            tag_name = tag.split(",")[0]
            tag_value = tag.split(",")[1]
            tag = SubElement(statistics_tag, tag_name)
            tag.text = tag_value
    table.append(root_list)
    xml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "xmls/actual/dataQualityStatistics.xml"))
    f = open(xml_path, "w")    
    f.write(prettify(table))
    f.close()


if __name__ == '__main__':
    main()
    

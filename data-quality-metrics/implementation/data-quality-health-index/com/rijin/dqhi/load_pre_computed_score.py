from xml.dom import minidom
from com.rijin.dqhi import XMLTagConstants
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.connection_wrapper import ConnectionWrapper
from com.rijin.dqhi import constants
from com.rijin.dqhi import query_builder
from com.rijin.dqhi.query_executor import DBExecutor

LOGGER = common_utils.get_logger()

def main():
    
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    
    table_column_ids = sai_db_query_executor.fetch_table_column_details(constants.USAGE_ENTITY_QUERY)[0]
    
    rule_name_id_dict = sai_db_query_executor.fetch_rule_name_id_dict()
     
    pre_computed_xml_file = constants.PRE_COMPUTED_SCORE_FILE_NAME
    xml_doc = minidom.parse(pre_computed_xml_file)
    fields = xml_doc.getElementsByTagName(XMLTagConstants.FIELDS)
    
    fields_dfn_param = {}
    fields_score_params = {}
    
    for field in fields:
        column_name = field.getElementsByTagName(XMLTagConstants.NAME)[0].firstChild.nodeValue
        table_name = field.getElementsByTagName(XMLTagConstants.TABLE_NAME)[0].firstChild.nodeValue
        fields_dfn_param["table_name"] = table_name
        fields_dfn_param["table_id"] = table_column_ids[table_name][table_name]
        fields_dfn_param["column_name"] = column_name
        fields_dfn_param["column_id"] = table_column_ids[table_name][column_name]
        fields_dfn_param["computed_date"] = field.getElementsByTagName(XMLTagConstants.COMPUTED_DATE)[0].firstChild.nodeValue
        fields_dfn_param["total_row_count"] = field.getElementsByTagName(XMLTagConstants.TOTAL_ROW_COUNT)[0].firstChild.nodeValue
        fields_dfn_param["column_precision"] = field.getElementsByTagName(XMLTagConstants.COLUMN_PRECISION)[0].firstChild.nodeValue
        fields_dfn_param["column_datatype"] = field.getElementsByTagName(XMLTagConstants.COLUMN_DATA_TYPE)[0].firstChild.nodeValue
        
        dq_fields_definition_query = query_builder.construct_fields_insert_query(fields_dfn_param)
        LOGGER.debug("Query to insert into dq_fields_definition table: {} ".format(dq_fields_definition_query))
        sai_db_query_executor.execute_query(dq_fields_definition_query)
        auto_generated_id = sai_db_query_executor.fetch_dqhi_columns_details(constants.DQ_FIELDS_DEFINITION_MAX_INSERTED_QUERY)[0]
        
        dimensions = field.getElementsByTagName(XMLTagConstants.DIMENSIONS)
        
        for dimension in dimensions:
            fields_score_params["auto_generated_id"] = auto_generated_id
            fields_score_params["dimension"] = dimension.getElementsByTagName(XMLTagConstants.TYPE)[0].firstChild.nodeValue
            rule_name = dimension.getElementsByTagName(XMLTagConstants.RULE_NAME)[0].firstChild.nodeValue
            
            fields_score_params["rule_id"] = int(rule_name_id_dict[rule_name].split(",")[0])
            fields_score_params["rule_parameter"] = dimension.getElementsByTagName(XMLTagConstants.RULE_PARAMETER)[0].firstChild.nodeValue
            
            fields_score_params["bad_record_count"] = dimension.getElementsByTagName(XMLTagConstants.BAD_RECORD_COUNT)[0].firstChild.nodeValue
            fields_score_params["source"] = constants.SOURCE_CUSTOM
            fields_score_params["score"] = dimension.getElementsByTagName(XMLTagConstants.SCORE)[0].firstChild.nodeValue
            fields_score_params["weight"] = dimension.getElementsByTagName(XMLTagConstants.WEIGHT)[0].firstChild.nodeValue
            fields_score_params["threshold"] = dimension.getElementsByTagName(XMLTagConstants.THRESHOLD)[0].firstChild.nodeValue
            fields_score_params["check_pass"] = dimension.getElementsByTagName(XMLTagConstants.CHECK_PASS)[0].firstChild.nodeValue
            fields_score_params["start_time"] = dimension.getElementsByTagName(XMLTagConstants.START)[0].firstChild.nodeValue
            fields_score_params["end_time"] = dimension.getElementsByTagName(XMLTagConstants.END)[0].firstChild.nodeValue
            
            fields_score_query = query_builder.construct_dq_fields_score_query(fields_score_params)
            LOGGER.debug("Query to insert into dq_fields_score table: {} ".format(fields_score_query))
            sai_db_query_executor.execute_query(fields_score_query)
            
    sai_db_query_executor.close_connection()

if __name__ == '__main__':
    main()

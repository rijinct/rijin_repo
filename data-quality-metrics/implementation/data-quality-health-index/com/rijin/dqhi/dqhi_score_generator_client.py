import os
import sys
import time
from com.rijin.dqhi import common_constants
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.connection_wrapper import ConnectionWrapper
from com.rijin.dqhi import constants
from com.rijin.dqhi import date_utils
from com.rijin.dqhi.df_excel_util import dfExcelUtil
from com.rijin.dqhi.dqhi_score_handler import DQHIScoreHandler 
from com.rijin.dqhi import  hdfs_file_utils
from com.rijin.dqhi.query_executor import DBExecutor
from com.rijin.dqhi import spark_session_initializer
from com.rijin.dqhi import prevalidate_dqhi_excel

LOGGER = common_utils.get_logger()


def main(argv=sys.argv):
    LOGGER.info("Triggered dqhi score generator")
    start_time = time.time()
    
    spark = spark_session_initializer.get_spark_session()
    
    lb_ub = date_utils.get_lower_upper_bound(argv[1])
    lower_bound = lb_ub["LB"]
    upper_bound = lb_ub["UB"]
    LOGGER.info("Executing for LB:{} , UB:{} ".format(lower_bound, upper_bound))
    
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    
    metastore_db_connection_obj = ConnectionWrapper.get_hive_metastore_connection_instance()
    
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    
    metastore_db_query_executor = DBExecutor(metastore_db_connection_obj)
    hive_tables = metastore_db_query_executor.fetch_result(constants.HIVE_TABLE_WITH_LOCATION_QUERY)
    LOGGER.info("list of hive tables: {} ".format(hive_tables))
    
    table_column_ids_dict_and_list = sai_db_query_executor.fetch_table_column_details(constants.USAGE_ENTITY_QUERY)
    
    table_column_ids = table_column_ids_dict_and_list[0]
    
    table_column_list = table_column_ids_dict_and_list[1]
    
    rule_name_id_dict = sai_db_query_executor.fetch_rule_name_id_dict()
    
    if constants.ENABLE_LOCAL_EXECUTION is True:
        dq_field_def = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/test-data/excel/DQ_RULES_AND_FIELD_DEFINITIONS.xlsx"))
    else:
        dq_field_def = common_utils.get_custom_default_filename(constants.DQHI_CUSTOM_CONF + '/' + constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE, constants.DQHI_CONF + '/' + constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE)
    df_excel_field_def = dfExcelUtil(dq_field_def, 4, 'FIELD_DEFINITIONS')
    df_excel_field_def.lowercase_df_column(constants.DQ_LOWERCASE_FIELD_NAMES)
    df_excel_field_def.drop_nan_values_df('field_name')
    df_excel_field_def.add_df_with_new_column_and_indexes(constants.DQ_FIELD_DEF_COL_NAMES, ["field_table_name", "field_name"])
    dq_field_def_df = df_excel_field_def.get_df_excel()
    prevalidate_dqhi_excel.execute_prevalidation(table_column_list, dq_field_def_df)
    dq_field_def_df.set_index(["field_table_name", "field_name"], inplace=True)
    counter = 0
    hive_tables = filter_tables(hive_tables, dq_field_def_df)
    for response in  hive_tables:
        counter += 1
        hive_table_name = response[0].lower()
        hdfsLocation = response[1]
        LOGGER.info("Calculating DQHI scores for table: {} ".format(hive_table_name))
        LOGGER.info("Executing {} out of {} ".format(counter, len(hive_tables)))
        data_files = hdfs_file_utils.read_data(hdfsLocation, lower_bound, upper_bound)
        if len(data_files) == 0:
            LOGGER.info("Empty Partition hence skipping for table {} ".format(hive_table_name)) 
            continue
        if constants.ENABLE_LOCAL_EXECUTION is True:
            df = spark.read.option("header","true").csv(*data_files)
        else:
            df = spark.read.parquet(*data_files)
            
        total_hive_row_count = df.count()
        if total_hive_row_count > 0:
            dqhi_score_handler_obj = DQHIScoreHandler(hive_table_name,
                                                  total_hive_row_count,
                                                  df,
                                                  dq_field_def_df,
                                                  argv[1],
                                                  table_column_ids,
                                                  rule_name_id_dict);
 
            dqhi_score_handler_obj.score_handler()
        else:
            LOGGER.info("No Records found in the table : {} ".format(hive_table_name))

    sai_db_query_executor.close_connection()
    metastore_db_query_executor.close_connection()
    LOGGER.info("--- %s Total seconds ---" % (time.time() - start_time))


def filter_tables(hive_tables, dq_field_def_df):
    table_names = common_constants.TABLE_NAMES_TO_CONSIDER
    if(common_constants.CONSIDER_ALL_COULUMN and len(table_names) != 0):
        hive_tables = [response for response in hive_tables if (response[0].upper() in table_names)]
    else:
        list_of_table = []
        for index in dq_field_def_df.index:
            list_of_table.append(index[0])
        table_names = list(dict.fromkeys(list_of_table))
        hive_tables = [response for response in hive_tables if (response[0] in table_names)]
    LOGGER.info("list of hive tables after filtering : {} ".format(hive_tables))
    return hive_tables

    
if __name__ == '__main__':
    main()


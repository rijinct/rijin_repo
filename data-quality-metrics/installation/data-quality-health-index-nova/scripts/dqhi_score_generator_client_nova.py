import os
import sys
import time
import subprocess
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
import common_constants
import common_utils
from connection_wrapper_nova import ConnectionWrapper
import constants
import constants_nova
import date_utils
from df_excel_util import dfExcelUtil
import file_util
from query_executor_nova import DBExecutor
import prevalidate_dqhi_excel
from create_sqlite_connection import SqliteDbConnection
from sqlite_query_executor import SqliteDBExecutor
from com.rijin.dqhi import dq_hdfs_to_sqlite_loader

LOGGER = common_utils.get_logger_nova()


def execute(argv=sys.argv):
    LOGGER.info("Triggered dqhi score generator")
    start_time = time.time()
    
    lb_ub = date_utils.get_lower_upper_bound(argv[1])
    lower_bound = lb_ub["LB"]
    upper_bound = lb_ub["UB"]
    LOGGER.info("Executing for LB:{} , UB:{} ".format(lower_bound, upper_bound))
    
    connectionWrapper_obj = ConnectionWrapper()
    
    sai_db_connection_obj = connectionWrapper_obj.get_postgres_connection_instance()
    
    metastore_db_connection_obj = connectionWrapper_obj.get_hive_metastore_connection_instance()
    
    
    sqlite_db_connection_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
    
    sqlite_db_query_executor = SqliteDBExecutor(sqlite_db_connection_obj)
    
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
     
    metastore_db_query_executor = DBExecutor(metastore_db_connection_obj)
    hive_tables = metastore_db_query_executor.fetch_result_as_tuple(constants.HIVE_TABLE_WITH_LOCATION_QUERY)
    LOGGER.info("list of hive tables: {} ".format(hive_tables))
    
    table_column_ids_dict_and_list = sai_db_query_executor.fetch_table_column_details(constants.USAGE_ENTITY_QUERY)
    
    table_column_ids = table_column_ids_dict_and_list[0]
    
    table_column_list = table_column_ids_dict_and_list[1]
    
    rule_name_id_dict = sqlite_db_query_executor.fetch_rule_name_id_dict()
    
    file_util.write_dict_to_file(table_column_ids,constants.TABLE_COL_DICT)
    
    file_util.write_list_to_file(table_column_list,constants.TABLE_COL_LIST)
    
    file_util.write_dict_to_file(rule_name_id_dict,constants.RULE_NAME_ID_DICT)
    
    sai_db_query_executor.close_connection()
    connectionWrapper_obj.close_connection_gateway()
    
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
    dq_field_def_df.to_csv('{}/dq_field_def_df.csv'.format(constants.DQHI_CONF))
    
    dq_field_def_df.set_index(["field_table_name", "field_name"], inplace=True)
    hive_tables = filter_tables(hive_tables, dq_field_def_df)
    file_util.write_tuple_to_file(hive_tables,constants.HIVE_TABLE_LIST)
    script_name = '{c}/{s}'.format(c=constants.DQHI_SCRIPTS,s=constants.SPARK_SUBMIT_SCRIPT)
    LOGGER.info("Score calculation begins for LB:{l} and UB:{u} ".format(l=lower_bound,u=upper_bound))
    
    for response in hive_tables:
        hive_tbl = response[0].lower()
        hdfs_path = response[1]
        skip_tbl = sqlite_db_query_executor.fetch_result(constants_nova.COMPUTED_TBL_QUERY.format(argv[1]))
        if (any(hive_tbl in tbl for tbl in skip_tbl)) :
            LOGGER.info("Data already present in the local DB, hence skipping for the table {}". format(hive_tbl))
            continue
        stat = subprocess.check_output(['sh', script_name, '{l} {u} {t} {h}'.format(l=lower_bound,u=upper_bound, t=hive_tbl, h=hdfs_path)])
        LOGGER.info("JobStatus for the table {t} is {s}".format(t=hive_tbl, s=stat))
        dq_hdfs_to_sqlite_loader.execute()


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
    execute()

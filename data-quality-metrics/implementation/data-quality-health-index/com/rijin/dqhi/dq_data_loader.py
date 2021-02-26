import os
import sys
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi.logger_util import *
from com.rijin.dqhi import common_utils
from com.rijin.dqhi import constants
from com.rijin.dqhi.df_db_util import dfDbUtil
from com.rijin.dqhi.df_excel_util import dfExcelUtil
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor
if os.getenv('IS_K8S') == 'true':
    from com.rijin.dqhi.connection_wrapper_nova import ConnectionWrapper
    LOGGER = common_utils.get_logger_nova()
else:
    from com.rijin.dqhi.query_executor import DBExecutor
    from com.rijin.dqhi.connection_wrapper import ConnectionWrapper
    exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))
    LOGGER = common_utils.get_logger()

class DataLoader:

    def __init__(self, file, sheetName, columnList, header, action, tableName,column_key):
        self.file = file
        self.sheetName = sheetName
        self.columns = columnList
        self.header = header
        self.action = action
        self.tableName = tableName
        self.dbUtil = dfDbUtil()
        self.decorate_df_with_columns()
        self.df = self.df_excel_util_obj.get_df_excel().dropna(subset=[column_key])

    def decorate_df_with_columns(self):
        self.df_excel_util_obj = dfExcelUtil(self.file, self.header, self.sheetName)
        self.df_excel_util_obj.add_df_with_new_column_names(self.columns)

    def decorate_with_tableid_columnid(self):
        self.df['table_id'] = (self.df['table_name'].map(self.tablename_id_dict)).str.get(0)

    def insert_into_db(self):
        self.dbUtil.insert_db_df(self.df, self.tableName, 'sairepo', self.action, False)


    def insert_into_db_nova(self):
        self.dbUtil.insert_db_df(self.df, self.tableName, None,self.action, False)

        
    def load_rules_definition_table(self):
        sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
        sai_db_query_executor = DBExecutor(sai_db_connection_obj)
        sai_db_query_executor.execute_query(constants.TRUNCATE_RULES_QUERY)
        self.insert_into_db()

    def load_rules_definition_table_nova(self):
        db_conn_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
        sqlite_db_query_executor = SqliteDBExecutor(db_conn_obj)
        sqlite_db_query_executor.execute_query(constants.TRUNCATE_RULES_QUERY_NOVA)
        sqlite_db_query_executor.close_connection()
        self.insert_into_db_nova()


def main():
    file = common_utils.get_custom_default_filename(constants.DQHI_CUSTOM_CONF + '/' + constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE, constants.DQHI_CONF + '/' + constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE)
    dataLoader = DataLoader(file, 'RULE_DEFINITIONS', constants.RULE_DEFINITIONS_COLUMN_LIST, 3, 'append', constants.DQ_RULES_DEFINITION_SHEET,'rule_presentation_name')
    if os.getenv('IS_K8S') == 'true':
        dataLoader.load_rules_definition_table_nova()
    else: 
        dataLoader.load_rules_definition_table()
    LOGGER.info("Successfully loaded the rules into rules_definition table")



if __name__ == "__main__":
    main()
  

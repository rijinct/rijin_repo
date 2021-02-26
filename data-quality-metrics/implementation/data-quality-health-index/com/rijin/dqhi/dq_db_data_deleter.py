import sys, os
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from datetime import date, timedelta
from com.rijin.dqhi import common_constants
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.connection_wrapper import ConnectionWrapper
from com.rijin.dqhi import constants
from com.rijin.dqhi import constants_nova 
from com.rijin.dqhi.query_executor import DBExecutor
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor
from com.rijin.dqhi import dq_fs_cleanup


LOGGER = common_utils.get_logger()
today = date.today()
retDate = today - timedelta(days=common_constants.DATA_RETENTION_DAYS)

def delete():
    LOGGER.info('Deleting records older than {}'.format(retDate))
    query = (constants.DELETE_QUERY).replace('COMP_DATE', retDate.strftime('%Y-%m-%d'))
    sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
    sai_db_query_executor = DBExecutor(sai_db_connection_obj)
    sai_db_query_executor.execute_query(query)
    LOGGER.info('Deletion Successful')
    sai_db_query_executor.close_connection()

def delete_nova():
    db_conn_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
    sqlite_db_query_executor = SqliteDBExecutor(db_conn_obj)
    sqlite_db_query_executor.execute_query(constants_nova.DELETE_SQLITE_SEQ_QUERY)
    sqlite_db_query_executor.execute_query(constants_nova.TRUNCATE_DQ_DEFINITION_QUERY)
    sqlite_db_query_executor.execute_query(constants_nova.TRUNCATE_DQ_SCORES_QUERY)
    sqlite_db_query_executor.execute_query(constants_nova.TRUNCATE_KPI_CDE_QUERY)
    sqlite_db_query_executor.close_connection

def main():
    if os.getenv('IS_K8S') == 'true':
        dq_fs_cleanup.execute()
        delete_nova()
    else:
        delete()


if __name__ == "__main__":
    main()

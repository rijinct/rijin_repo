import os,sys
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
import constants
import constants_nova
from create_sqlite_connection import SqliteDbConnection
from sqlite_query_executor import SqliteDBExecutor 



def create_delete_table(db_executor,filename):
    sql_file = '{d}/{f}'.format(d=constants_nova.DQ_SCHEMA_LOC,f=filename)
    print(sql_file)
    sql_file_handler = open(sql_file, 'r').read()
    sql_query = sql_file_handler.split(';')
    for sql in sql_query:
        db_executor.execute_query(sql)
    print("Successfully created/deleted the tables")
    db_executor.close_connection()

def execute(argv=sys.argv):
    db_conn_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
    db_executor = SqliteDBExecutor(db_conn_obj)
    if argv[1] == "CREATE":
        file_name= constants.CREATE_SQL_FILE
    else:
        file_name= constants.DELETE_SQL_FILE
    create_delete_table(db_executor,file_name)


if __name__=='__main__':
    execute()
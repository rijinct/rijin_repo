import psycopg2
import traceback
import subprocess
from datetime import datetime

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").
     read().replace("(", "(\"").replace(")", "\")"))
from loggerUtil import loggerUtil

currentDate = datetime.now().strftime("%Y-%m-%d")
logFile = 'postgres_connection_'.format(currentDate)
LOGGER = loggerUtil.__call__().get_logger(logFile)


class PostgresConnection:
    __instance = None

    def __init__(self, hive_metastore=None):
        self.hive_metastore = hive_metastore
        if PostgresConnection.__instance is not None:
            raise Exception("The class is a singleton")
        else:
            PostgresConnection.__instance = self
            if self.hive_metastore is not None:
                self.get_connection(self.hive_metastore)
            else:
                self.get_connection()

    @staticmethod
    def get_instance():
        if PostgresConnection.__instance is None:
            PostgresConnection()
        return PostgresConnection.__instance

    def get_connection(self, hive_metastore=None):
        try:
            if hive_metastore is not None:

                database = self.get_db_host_details('cemod_hive_dbname')
                user = self.get_db_host_details(
                    'cemod_hive_metastore_db_user')
                host = self.get_db_host_details(
                    'cemod_postgres_active_fip_host')
                port = self.get_db_host_details(
                    'cemod_hive_metastore_db_port')
                self.con = psycopg2.connect(database=database, user=user,
                                            password="", host=host,
                                            port=port)
            else:
                self.con = psycopg2.connect(
                    database=cemod_sdk_db_name,
                    user=cemod_application_sdk_database_linux_user,
                    password="",
                    host=cemod_postgres_sdk_fip_active_host,
                    port=cemod_application_sdk_db_port)
        except:
            LOGGER.error(traceback.format_exc())

    def get_db_host_details(self, node):
        PARENT_NODE = cemod_hive_hosts.split(' ')[0]
        return subprocess.getoutput(
            '''ssh %s 'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${%s}' '''%
            (PARENT_NODE, node)).split(' ')[0]

    def execute_query(self, sql):
        cur = self.con.cursor()
        cur.execute(sql)
        self.con.commit()

    def execute_query_with_record(self, sql, record):
        cur = self.con.cursor()
        cur.execute(sql, record)
        self.con.commit()

    def fetch_records_as_list(self, sql):
        myList = []
        cur = self.con.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        for row in result:
            myList.append(row[0] + "," + row[1])
        return myList

    def fetch_records(self, sql):
        # returns a list of tuples
        sql = sql.replace("'", "\'").replace('"', '\"')
        return self.fetch_all_records(sql)
        
    def fetch_all_records(self,sql):
        cur = self.con.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def fetch_dqhi_columns_details(self, sql):
        cur = self.con.cursor()
        cur.execute(sql)
        return cur.fetchone()

    def close_connection(self):
        self.con.close()

    @staticmethod
    def clear_instance():
        PostgresConnection.__instance = None

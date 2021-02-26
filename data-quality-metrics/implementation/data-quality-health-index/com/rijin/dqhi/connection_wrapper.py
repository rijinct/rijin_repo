import os
import sqlite3

import constants
from query_executor import DBExecutor


class ConnectionWrapper:

    @staticmethod
    def get_postgres_connection_instance():
        connection = None
        if constants.ENABLE_LOCAL_EXECUTION:
            sai_repo_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/SAIREPO.db"))
            connection = sqlite3.connect(sai_repo_db_path)
        else:
            from com.rijin.dqhi.postgres_sairepo_connection import PostgresConnection
            
            connection = PostgresConnection.get_instance().get_connection()
        return connection
    
    @staticmethod
    def get_hive_metastore_connection_instance():
        connection = None
        if constants.ENABLE_LOCAL_EXECUTION:
            sai_repo_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/SAIREPO.db"))
            connection = sqlite3.connect(sai_repo_db_path)
        else:
            from com.rijin.dqhi.postgres_hive_connection import PostgresHiveConnection
            connection = PostgresHiveConnection.get_instance().get_connection()
        return connection
        

import traceback

import psycopg2

from com.rijin.dqhi import common_utils

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))

LOGGER = common_utils.get_logger()


class PostgresConnection:
    __instance = None
    
    def __init__(self):
        if PostgresConnection.__instance is not None:
            raise Exception("The class is a singleton")
        else:
            PostgresConnection.__instance = self
            self.initialize_connection()
            
    @staticmethod
    def get_instance():
        if PostgresConnection.__instance is None:
            PostgresConnection()
        return PostgresConnection.__instance
    
    def initialize_connection(self):
        try:
            self.con = psycopg2.connect(database=project_sdk_db_name, user=project_application_sdk_database_linux_user, password="", host=project_postgres_sdk_fip_active_host, port=project_application_sdk_db_port)
        except:
            LOGGER.error(traceback.format_exc())

    def get_connection(self):
        return self.con
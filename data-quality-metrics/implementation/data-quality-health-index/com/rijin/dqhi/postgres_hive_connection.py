import subprocess
import traceback

import psycopg2

from com.rijin.dqhi import common_utils, constants

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))
LOGGER = common_utils.get_logger()


class PostgresHiveConnection:
    __instance = None
    
    @staticmethod
    def get_instance():
        if PostgresHiveConnection.__instance is None:
            PostgresHiveConnection()
        return PostgresHiveConnection.__instance
    
    def __init__(self):
        if PostgresHiveConnection.__instance is not  None:
            raise Exception("The class is a singleton")
        else:
            PostgresHiveConnection.__instance = self
            self.database = subprocess.getoutput('ssh %s \'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${project_hive_dbname}\' ' % (project_hive_hosts.split(' ')[0])).split(' ')[0]
            self.user = subprocess.getoutput('ssh %s \'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${project_hive_metastore_db_user}\' ' % (project_hive_hosts.split(' ')[0])).split(' ')[0]
            self.host = subprocess.getoutput('ssh %s \'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${project_postgres_active_fip_host}\' ' % (project_hive_hosts.split(' ')[0])).split(' ')[0] 
            self.port = subprocess.getoutput('ssh %s \'source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh;echo ${project_hive_metastore_db_port}\' ' % (project_hive_hosts.split(' ')[0])).split(' ')[0]
            self.initialize_connection()
            
    def initialize_connection(self):
        try:
            self.connection = psycopg2.connect(database=self.database, user=self.user, password="", host=self.host, port=self.port)
        except:
            LOGGER.error(traceback.format_exc())

    def get_connection(self):
        return self.connection
    

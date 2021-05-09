from healthmonitoring.framework import config
from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework.db.postgres import PostgresDBConnection
from healthmonitoring.framework.util.specification import SpecificationUtil
from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class PostgresHiveConnection(PostgresDBConnection):

    def __init__(self):
        logger.debug('Creating new connection for hive postgres db')
        self._set_connection_specific_details()
        self._create_connection()

    @staticmethod
    def get_instance():
        if DBConnection._instance is None:
            DBConnection._instance = PostgresHiveConnection()
        logger.debug('Obtained connection for hive postgres db')
        return DBConnection._instance

    def _set_connection_specific_details(self):
        postgres_details = SpecificationUtil.get_host('postgres_hive')
        self._database = postgres_details.fields['cemod_hive_dbname']
        self._user = postgres_details.fields['cemod_hive_metastore_db_user']
        self._password = ""
        self._host = postgres_details.fields['cemod_postgres_active_fip_host']
        self._port = postgres_details.fields['cemod_hive_metastore_db_port']

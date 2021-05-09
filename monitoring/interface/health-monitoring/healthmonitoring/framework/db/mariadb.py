import pymysql

from logger import Logger

from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework import config
from healthmonitoring.framework.util.specification import SpecificationUtil

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class MariaDBConnection(DBConnection):

    def __init__(self):
        self.set_connection_specific_details()
        self.create_connection()

    @staticmethod
    def get_instance():
        if DBConnection._instance is None:
            DBConnection._instance = MariaDBConnection()
        logger.debug('Obtained connection for mariadb')
        return DBConnection._instance

    def set_connection_specific_details(self):
        mariadb_details = SpecificationUtil.get_host('mariadb')
        self._database = mariadb_details.fields['mariadb_db_name']
        self._user = mariadb_details.fields[
            'mariadb_user']
        self._password = mariadb_details.fields[
            'mariadb_password']
        self._host = mariadb_details.fields[
            'mariadb_active_host']
        self._connect_timeout = mariadb_details.fields[
            'mariadb_connect_timeout']

    def create_connection(self):
        logger.info('Creating mariadb connection')
        self._connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            db=self._database,
            connect_timeout=self._connect_timeout)

    def get_table_name(self):
        return "monitoring.hm_stats"

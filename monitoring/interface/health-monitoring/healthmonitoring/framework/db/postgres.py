import psycopg2

from healthmonitoring.framework import config
from healthmonitoring.framework.db.connection import DBConnection
from logger import Logger


logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class PostgresDBConnection(DBConnection):

    def _set_connection_specific_details(self):
        pass

    def _create_connection(self):
        logger.info('Creating postgres connection')
        self._connection = psycopg2.connect(
            database=self._database,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port)

    def _get_table_name(self):
        return "sairepo.hm_stats"

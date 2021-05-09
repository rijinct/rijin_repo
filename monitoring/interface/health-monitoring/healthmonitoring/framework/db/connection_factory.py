from builtins import ConnectionError

import psycopg2
import pymysql

from healthmonitoring.framework.specification.defs import DBType
from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework.db.postgres_hive import PostgresHiveConnection
from healthmonitoring.framework.db.postgres_sdk import PostgresSDKConnection
from healthmonitoring.framework.db.mariadb import MariaDBConnection
from healthmonitoring.framework import config
from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class ConnectionFactory:

    @staticmethod
    def instantiate_connection(db_type):
        try:
            logger.debug(
                'Instantiating connection for db: {}'.format(db_type))
            if db_type == DBType.POSTGRES_HIVE:
                PostgresHiveConnection.get_instance()
            elif db_type == DBType.POSTGRES_SDK:
                PostgresSDKConnection.get_instance()
            else:
                MariaDBConnection.get_instance()
        except (ValueError, psycopg2.DatabaseError,
                pymysql.DatabaseError) as e:
            raise ConnectionError(
                "Connection could not be created for db type: {}".format(
                    db_type), e)

    @staticmethod
    def get_connection():
        return DBConnection._instance

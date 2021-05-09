from healthmonitoring.framework import config
from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework.db.postgres import PostgresDBConnection
from healthmonitoring.framework.util import string_util
from healthmonitoring.framework.util.specification import SpecificationUtil
from logger import Logger

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)


class PostgresSDKConnection(PostgresDBConnection):
    def __init__(self):
        self._set_connection_specific_details()
        self._create_connection()

    @staticmethod
    def get_instance():
        if DBConnection._instance is None:
            DBConnection._instance = PostgresSDKConnection()
        logger.debug('Obtained connection for sdk postgres db')
        return DBConnection._instance

    def _set_connection_specific_details(self):
        postgres_details = SpecificationUtil.get_host('postgres_sdk')
        self._set_database_details(postgres_details)

    def _set_database_details(self, postgres_details):
        self._database = self._get_sdk_db_name(postgres_details)
        self._user = postgres_details.fields.get(
            'cemod_application_sdk_database_linux_user') or "postgres"
        self._password = ""
        self._host = self._get_fip_host(postgres_details)
        self._port = self._get_port(postgres_details)

    def _get_sdk_db_name(self, postgres_details):
        sdk_db_name = postgres_details.fields['cemod_sdk_db_name']
        if not sdk_db_name:
            sdk_db_url = postgres_details.fields['cemod_sdk_db_url']
            interim_url = string_util.substring_after("/", sdk_db_url, -1)
            sdk_db_name = string_util.substring_before("?", interim_url)
        return sdk_db_name

    def _get_fip_host(self, postgres_details):
        fip_host = postgres_details.fields[
            'cemod_postgres_sdk_fip_active_host']
        if not fip_host:
            sdk_db_url = postgres_details.fields['cemod_sdk_db_url']
            interim_fip_host = string_util.substring_after(
                "//", sdk_db_url, -1)
            fip_host = string_util.substring_before(":", interim_fip_host)
        return fip_host

    def _get_port(self, postgres_details):
        postgres_port = postgres_details.fields[
            'cemod_application_sdk_db_port']
        if not postgres_port:
            sdk_db_url = postgres_details.fields['cemod_sdk_db_url']
            interim_port = string_util.substring_after(":", sdk_db_url, -1)
            postgres_port = string_util.substring_before("/", interim_port)
        return postgres_port

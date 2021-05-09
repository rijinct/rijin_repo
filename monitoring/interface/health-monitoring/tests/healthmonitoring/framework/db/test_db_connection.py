import unittest

from mock.mock import patch
import psycopg2
import pymysql

import healthmonitoring.framework
from healthmonitoring.framework.db import mariadb
from healthmonitoring.framework.db.connection import DBConnection
from healthmonitoring.framework.db.connection_factory import ConnectionFactory
from healthmonitoring.framework.db.mariadb import MariaDBConnection
from healthmonitoring.framework.db.postgres_hive import PostgresHiveConnection
from healthmonitoring.framework.db.postgres_sdk import PostgresSDKConnection
from healthmonitoring.framework.specification.defs import DBType
from tests.healthmonitoring.framework.utils import TestUtil


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestConnectionFactory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_create_postgres_hive_connection(self, MockPsycopg2,
                                             mock_get_config_dir_path,
                                             mock_repopulate_hosts,
                                             mock_get_config_map_time_stamp):
        DBConnection._instance = None
        ConnectionFactory.instantiate_connection(DBType.POSTGRES_HIVE)
        connection = ConnectionFactory.get_connection()
        self.assertIsInstance(connection, PostgresHiveConnection)

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_create_postgres_sdk_connection(self, MockPsycopg2,
                                            mock_get_config_dir_path,
                                            mock_repopulate_hosts,
                                            mock_get_config_map_time_stamp):
        DBConnection._instance = None
        ConnectionFactory.instantiate_connection(DBType.POSTGRES_SDK)
        connection = ConnectionFactory.get_connection()
        self.assertIsInstance(connection, PostgresSDKConnection)

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_create_mariadb_connection(self, MockPymysql,
                                       mock_get_config_dir_path,
                                       mock_repopulate_hosts,
                                       mock_get_config_map_time_stamp):
        DBConnection._instance = None
        ConnectionFactory.instantiate_connection(str(DBType.MARIADB))
        connection = ConnectionFactory.get_connection()
        self.assertIsInstance(connection, MariaDBConnection)

    def test_create_connection_exception(self, mock_get_config_dir_path,
                                         mock_repopulate_hosts,
                                         mock_get_config_map_time_stamp):
        DBConnection._instance = None
        self.assertRaises(ConnectionError,
                          ConnectionFactory.instantiate_connection, 'test')


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestPostgresConnection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        healthmonitoring.framework.db.postgres.hosts_spec = \
            healthmonitoring.framework.config.hosts_spec

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_get_sdk_instance(self, MockPsycopg2, mock_get_config_dir_path,
                              mock_repopulate_hosts,
                              mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresSDKConnection.get_instance()
        self.assertIsInstance(postgres_connection, PostgresSDKConnection)

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_get_hive_instance(self, MockPsycopg2, mock_get_config_dir_path,
                               mock_repopulate_hosts,
                               mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresHiveConnection.get_instance()
        self.assertIsInstance(postgres_connection, PostgresHiveConnection)

    def test_create_connection_with_exception(self, mock_get_config_dir_path,
                                              mock_repopulate_hosts,
                                              mock_get_config_map_time_stamp):
        DBConnection._instance = None
        self.assertRaises(psycopg2.DatabaseError,
                          PostgresSDKConnection.get_instance)

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_insert_values(self, MockPsycopg2, mock_get_config_dir_path,
                           mock_repopulate_hosts,
                           mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresHiveConnection.get_instance()
        values = ["value1", "value2"]
        postgres_connection.insert_values('test', values)
        MockPsycopg2.connect().cursor().execute.assert_called()
        MockPsycopg2.connect().commit.assert_called()

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_fetch_records(self, MockPsycopg2, mock_get_config_dir_path,
                           mock_repopulate_hosts,
                           mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresHiveConnection.get_instance()
        postgres_connection.fetch_records("")
        MockPsycopg2.connect().cursor().fetchall.assert_called()

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_get_table_name(self, MockPsycopg2, mock_get_config_dir_path,
                            mock_repopulate_hosts,
                            mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresHiveConnection.get_instance()
        self.assertEqual(postgres_connection._get_table_name(),
                         "sairepo.hm_stats")

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_get_instance_multiple_times(self, MockPsycopg2,
                                         mock_get_config_dir_path,
                                         mock_repopulate_hosts,
                                         mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection1 = PostgresHiveConnection.get_instance()
        postgres_connection2 = PostgresHiveConnection.get_instance()
        self.assertEqual(postgres_connection1, postgres_connection2)

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_close_connection(self, MockPsycopg2, mock_get_config_dir_path,
                              mock_repopulate_hosts,
                              mock_get_config_map_time_stamp):
        DBConnection._instance = None
        postgres_connection = PostgresHiveConnection.get_instance()
        postgres_connection.close_connection()
        MockPsycopg2.connect().close.assert_called()


@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_dir_path',  # noqa: E501
    return_value="./../../resources")
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._repopulate_hosts')
@patch(
    'healthmonitoring.framework.runner.SpecificationLoader._get_config_map_time_stamp',  # noqa: E501
    return_value=0)
class TestMariadbConnection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    def setUp(self):
        healthmonitoring.framework.db.mariadb.hosts_spec = \
            healthmonitoring.framework.config.hosts_spec

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_get_instance(self, MockPymysql, mock_get_config_dir_path,
                          mock_repopulate_hosts,
                          mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection = MariaDBConnection.get_instance()
        self.assertIsInstance(connection, MariaDBConnection)

    def test_create_connection_with_exception(self, mock_get_config_dir_path,
                                              mock_repopulate_hosts,
                                              mock_get_config_map_time_stamp):
        DBConnection._instance = None
        mariadb.pymysql = pymysql
        self.assertRaises(pymysql.DatabaseError,
                          MariaDBConnection.get_instance)

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_insert_values(self, MockPymysql, mock_get_config_dir_path,
                           mock_repopulate_hosts,
                           mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection = MariaDBConnection.get_instance()
        values = ["value1", "value2"]
        connection.insert_values('test', values)
        MockPymysql.connect().cursor().execute.assert_called()
        MockPymysql.connect().commit.assert_called()

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_fetch_records(self, MockPymysql, mock_get_config_dir_path,
                           mock_repopulate_hosts,
                           mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection = MariaDBConnection.get_instance()
        connection.fetch_records("")
        MockPymysql.connect().cursor().fetchall.assert_called()

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_get_table_name(self, MockPymysql, mock_get_config_dir_path,
                            mock_repopulate_hosts,
                            mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection = MariaDBConnection.get_instance()
        self.assertEqual(connection.get_table_name(), "monitoring.hm_stats")

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_get_instance_multiple_times(self, MockPymysql,
                                         mock_get_config_dir_path,
                                         mock_repopulate_hosts,
                                         mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection1 = MariaDBConnection.get_instance()
        connection2 = MariaDBConnection.get_instance()
        self.assertEqual(connection1, connection2)

    @patch('healthmonitoring.framework.db.mariadb.pymysql')
    def test_close_connection(self, MockPymysql, mock_get_config_dir_path,
                              mock_repopulate_hosts,
                              mock_get_config_map_time_stamp):
        DBConnection._instance = None
        connection = MariaDBConnection.get_instance()
        connection.close_connection()
        MockPymysql.connect().close.assert_called()


if __name__ == "__main__":
    unittest.main()

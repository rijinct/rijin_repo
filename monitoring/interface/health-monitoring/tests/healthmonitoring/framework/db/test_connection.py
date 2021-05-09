import unittest
from mock.mock import patch

from healthmonitoring.framework.db.connection import DBConnection
from tests.healthmonitoring.framework.utils import TestUtil


class MockDerviedDBConnection(DBConnection):

    def __init__(self):
        self.set_connection_specific_details()
        self.create_connection()
        DBConnection._instance = self

    @staticmethod
    def get_instance():
        if DBConnection._instance is None:
            MockDerviedDBConnection()
        return DBConnection._instance

    def create_connection(self):
        pass


class TestDBConnection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        TestUtil.load_specification()

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_fetch_records(self, MockPsycopg2):
        DBConnection._instance = None
        db_connection = MockDerviedDBConnection.get_instance()
        db_connection._connection = MockPsycopg2.connect()
        db_connection.fetch_records("select * from test")
        MockPsycopg2.connect().cursor().fetchall.assert_called()

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_insert_values(self, MockPsycopg2):
        db_connection = MockDerviedDBConnection.get_instance()
        db_connection._connection = MockPsycopg2.connect()
        db_connection.insert_values("test", ["test1", "test2"])
        MockPsycopg2.connect().cursor().execute.assert_called()
        MockPsycopg2.connect().commit.assert_called()

    def test_multiple_instance_of_same_class_creation(self):
        db_connection1 = MockDerviedDBConnection.get_instance()
        db_connection2 = MockDerviedDBConnection.get_instance()
        self.assertEqual(db_connection1, db_connection2)

    def test_get_table_name(self):
        db_connection = DBConnection()
        db_connection._get_table_name()
        self.assertIsNone(db_connection._get_table_name())

    @patch('healthmonitoring.framework.db.postgres.psycopg2')
    def test_close_connection(self, MockPsycopg2):
        db_connection1 = MockDerviedDBConnection.get_instance()
        db_connection1._connection = MockPsycopg2.connect()
        db_connection1.close_connection()
        MockPsycopg2.connect().close.assert_called()


if __name__ == "__main__":
    unittest.main()

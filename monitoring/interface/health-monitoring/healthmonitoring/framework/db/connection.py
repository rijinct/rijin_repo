from datetime import datetime


class DBConnection:
    _instance = None

    def set_connection_specific_details(self):
        pass

    def create_connection(self):
        pass

    def fetch_records(self, sql):
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            raise ConnectionError("Error fetching records", e)

    def insert_values(self, utility_type, values):
        sql_query = 'insert into {} values (%s, %s, %s)'.format(
            self._get_table_name())
        current_timestamp = datetime.now()
        for value in values:
            record = (current_timestamp, utility_type, value)
            self._execute_query(sql_query, record)

    def _get_table_name(self):
        pass

    def close_connection(self):
        if self._connection is not None:
            self._connection.close()

    def _execute_query(self, sql_query, record):
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql_query, record)
            self._connection.commit()
        except Exception as e:
            raise ConnectionError("Error inserting records", e)

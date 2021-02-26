'''
Created on 11 Aug, 2020

@author: amitakum
'''

from dataframe import LocalDataFrame


class LocalSession:
    BUILDER = None

    def __init__(self, app_name):
        self._session = LocalSession.BUILDER. \
            appName(app_name).master("yarn"). \
            enableHiveSupport().getOrCreate()

    @property
    def session(self):
        return self._session

    def add_file(self, file):
        self._session.sparkContext.addFile(file)

    def set(self, key, value):
        self._session.conf.set(key, value)

    def get_logger(self):
        return self._session.sparkContext._jvm.org.apache.log4j

    def get_schema(self, table_name):
        return self._session.table(table_name).schema

    def get_data_types(self, table_name, drop_col):
        df = self._session.table(table_name)
        for col in drop_col:
            df = df.drop(col)
        return df.dtypes

    def read_parquet(self, loc):
        return LocalDataFrame(self._session.read.parquet(loc))

    def read_schema(self, schema, *locs):
        return LocalDataFrame(self._session.read.schema(schema).parquet(*locs))

    def createDataFrame(self, rows, schema):
        return LocalDataFrame(self._session.createDataFrame(rows, schema))

    def sql(self, query):
        return LocalDataFrame(self._session.sql(query))

    def update_metastore(self, table_name):
        self._session.catalog.recoverPartitions(table_name)

    def get_conf(self):
        return self._session.sparkContext.getConf().getAll()

    def stop(self):
        self._session.stop()

    def set_current_database(self, dbname):
        return self._session.catalog.setCurrentDatabase(dbname)

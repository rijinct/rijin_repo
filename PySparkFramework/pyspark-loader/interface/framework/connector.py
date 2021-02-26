from connection import LocalSession


class SparkConnector:
    spark_session = None

    def __init__(self, name):
        if SparkConnector.spark_session is not None:
            raise Exception('Only one instance of the same "{0}" class is '
                            'allowed'.format(self.__class__.__name__))
        else:
            app_name = "pyspark_{}".format(name)
            SparkConnector.spark_session = LocalSession(app_name)

    @staticmethod
    def get_spark_session(name):
        if SparkConnector.spark_session is None:
            SparkConnector(name)
        return SparkConnector.spark_session

    @staticmethod
    def add_files(files):
        for file in files:
            SparkConnector.spark_session.add_file(file)

    @staticmethod
    def add_hints(hints):
        for key, value in hints.items():
            SparkConnector.spark_session.set(key, value)

    @staticmethod
    def get_logger(name):
        log4jlogger = SparkConnector.spark_session.get_logger()
        logger = log4jlogger.LogManager.getLogger(name)
        logger.setLevel(log4jlogger.Level.INFO)
        return logger

    @staticmethod
    def set_current_db(db_name):
        SparkConnector.spark_session.set_current_database(db_name)

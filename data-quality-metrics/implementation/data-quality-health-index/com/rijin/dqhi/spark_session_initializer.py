from pyspark.sql.session import SparkSession

from com.rijin.dqhi import constants


def get_spark_session():
    spark = None
    if constants.ENABLE_LOCAL_EXECUTION:
        spark = SparkSession.builder.appName("DQHI").\
        master("local").getOrCreate()
    else:
        spark = SparkSession.builder.appName("DQHI").\
        master("yarn-client").\
        config("spark.yarn.queue", "root.rijin.ca4ci.DQI").\
        config("spark.sql.shuffle.partitions", '100').\
        config("spark.dynamicAllocation.enabled", "true").\
        config("spark.executor.memory", "2g").\
        config("spark.driver.memory", "2g").\
        config("spark.shuffle.service.enabled", "true").\
        config("spark.sql.crossJoin.enabled", "true").\
        config("spark.sql.execution.arrow.enabled", "true").\
        config("spark.sql.parquet.compression.codec", "snappy").\
        config("spark.files.maxPartitionBytes", 67108864).\
        config("spark.sql.broadcastTimeout", '14400').getOrCreate()
    return spark


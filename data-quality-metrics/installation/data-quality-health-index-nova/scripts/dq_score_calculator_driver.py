import sys
from pyspark.sql.session import SparkSession
from dqhi_score_handler import DQHIScoreHandler 
import hdfs_file_utils
import constants
import file_util
import pandas as pd
import common_utils

LOGGER = common_utils.get_logger()

def get_spark_session():
    spark_session = SparkSession.builder.appName("DQHI").master("yarn").getOrCreate()
    return spark_session

def execute(argv=sys.argv):
    lower_bound = int(sys.argv[1])
    upper_bound = int(sys.argv[2])
    hive_table_name = sys.argv[3]
    hdfsLocation = sys.argv[4]
    table_column_ids = file_util.get_dict_properties(constants.TABLE_COL_DICT)
    rule_name_id_dict = file_util.get_dict_properties(constants.RULE_NAME_ID_DICT)
    dq_field_def_df = pd.read_csv('dq_field_def_df.csv')
    dq_field_def_df.set_index(["field_table_name", "field_name"], inplace=True)
    spark = get_spark_session()
    counter = 0
    hdfs_file_utils.removeDirectory()
    data_files = hdfs_file_utils.read_data(hdfsLocation, lower_bound, upper_bound)
    if len(data_files) == 0:
        LOGGER.info("Empty Partition hence skipping for table {} ".format(hive_table_name))
        return
        
    if constants.ENABLE_LOCAL_EXECUTION is True:
        df = spark.read.option("header","true").csv(*data_files)
    else:
        df = spark.read.parquet(*data_files)
    total_hive_row_count = df.count()
    LOGGER.info('total_hive_row_count is {c} for the table {t}'.format(c=total_hive_row_count,t=hive_table_name))
        
    if total_hive_row_count > 0:
        dqhi_score_handler_obj = DQHIScoreHandler(hive_table_name,
                                                  total_hive_row_count,
                                                  df,
                                                  dq_field_def_df,
                                                  argv[1],
                                                  table_column_ids,
                                                  rule_name_id_dict, spark, counter,execution="k8s");

        dqhi_score_handler_obj.score_handler()
        counter= dqhi_score_handler_obj.get_counter()
    else:
        LOGGER.info("No Records found in the table : {} ".format(hive_table_name))
    

if __name__ == '__main__':
    execute()

'''
Created on 17-Nov-2020

@author: rithomas
'''
import sys
import glob
import pandas as pd
import datetime
import os
import shutil
sys.path.insert(0, '/usr/local/lib/python3.6/site-packages/')
sys.path.insert(0, '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi.logger_util import *
from py4j.java_gateway import Py4JError
from py4j.java_gateway import traceback
from com.rijin.dqhi.connection_wrapper_nova import ConnectionWrapper
from com.rijin.dqhi.logger_util import *
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.df_db_util import dfDbUtil
from com.rijin.dqhi import constants_nova
 
LOGGER = common_utils.get_logger_nova()


def check_file_timestamp(hdfs_file_timestamp): 
    hdfs_file_timestamp = hdfs_file_timestamp[:-3]
    actual_file_timestamp = str(datetime.datetime.fromtimestamp(int(hdfs_file_timestamp)).strftime('%Y-%m-%d'))
    today_timestamp = str(datetime.date.today())
    status = False
    if today_timestamp == actual_file_timestamp:
        status = True
    return status 


def copy_hdfs_files():
    global gateway_obj
    global backlog_manager_obj
    try:
        gateway_obj = ConnectionWrapper().get_gateway()
        backlog_manager_obj = gateway_obj.jvm.com.rijin.dqhi.hdfs.\
                    HdfsUtil(constants_nova.HADOOP_CONF)
        hdfs_file_timestamp = str(backlog_manager_obj.getFileModificationTime(constants_nova.DQ_HDFS_FILES_CDLK))
        if hdfs_file_timestamp != 'None' and check_file_timestamp(hdfs_file_timestamp):
                LOGGER.info('hdfs files are present for the timestamp: {}, copying the hdfs-files to local FS'.format(hdfs_file_timestamp))
                if os.path.isdir(constants_nova.DQ_HDFS_FILES_LOCAL):
                    shutil.rmtree(constants_nova.DQ_HDFS_FILES_LOCAL)          
                backlog_manager_obj.copyFilesToLocal(constants_nova.DQ_HDFS_FILES_LOCAL,constants_nova.DQ_HDFS_FILES_CDLK)
        else: 
            LOGGER.info('No score files present in hdfs, hence exiting')
            sys.exit(1)
    except Py4JError:
        LOGGER.exception(traceback.print_exc())
        print(traceback.print_exc())
    finally:
        gateway_obj.shutdown()
 

def get_combined_csv(col_names, tablename):
    all_filenames = [i for i in glob.glob('{p}/{t}/**/*.{c}'.format(p=constants_nova.DQ_HDFS_FILES_LOCAL, t=tablename, c='csv'), recursive = True)]
    df_combined_csv = pd.DataFrame()
    for f in all_filenames:
        df_f = pd.read_csv(f, names=col_names, header=None)
        df_combined_csv = df_combined_csv.append(df_f)
    return df_combined_csv
        
def load_hdfs_files():
    dbUtil = dfDbUtil()
    df_dq_def_csv = get_combined_csv(constants_nova.DQ_FIELDS_DEFINITION_COL,constants_nova.DQ_FIELDS_DEF_TBL)
    if not df_dq_def_csv.empty:
        df_dq_def_csv['computed_date'] = df_dq_def_csv['computed_date'].map(lambda x: str(x)[:-3])
        df_dq_def_csv['computed_date'] = pd.to_datetime(df_dq_def_csv['computed_date'], unit='s')
        df_dq_score_csv = get_combined_csv(constants_nova.DQ_FIELDS_SCORE_COL,constants_nova.DQ_FIELDS_SCORE_TBL)
        dbUtil.insert_db_df(df_dq_def_csv, constants_nova.DQ_FIELDS_DEF_TBL, None, 'append', False)
        dbUtil.insert_db_df(df_dq_score_csv, constants_nova.DQ_FIELDS_SCORE_TBL, None, 'append', False)
    else: 
        LOGGER.info("Empty Dataframe")
        

def execute():
    copy_hdfs_files()
    load_hdfs_files()
if __name__ == '__main__':
    execute()


'''
Created on 24-Nov-2020

@author: rithomas
'''
import os
import sys
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor
from com.rijin.dqhi import constants
from com.rijin.dqhi import dq_hdfs_to_sqlite_loader
from com.rijin.dqhi import common_utils
LOGGER = common_utils.get_logger_nova()

def execute(calculator):
    status = os.system(calculator)
    check_status(status, calculator)


def check_status(status, calculator):
    if 0 == status:
        LOGGER.info("Successfully executed {}".format(calculator))
    else:
        LOGGER.error("Error in executing {}".format(calculator))
        exit(0)

def trigger_dqhi_calculation(date):
    score_calculator = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.SCORE_CALCULATOR, date)
    execute(score_calculator)
    LOGGER.info("Starting KPI CDE Score Calculation....")
    kpi_cde_score_calculator = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.KPI_CDE_SCORE_CALCULATOR, date)
    execute(kpi_cde_score_calculator)
    LOGGER.info("Exporting Rules, Results and Statistics XMLs....")
    dq_data_exporter = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.DQ_DATA_EXPORTER, date)
    execute(dq_data_exporter)

def check_for_trigger_entry(sqlite_db_query_executor, rerun):
    if rerun == 'rerun':
        delete_trigger_entry()
    global trigger_date
    trigger_date = sqlite_db_query_executor.fetch_result(constants.DQHI_TRIGGER_FETCH_QUERY)
    trigger_date = trigger_date[0][0]
    if trigger_date is None:
        LOGGER.info('No trigger entry, hence exiting')
        exit(0)
    LOGGER.info('Score calculation is re-triggered for the date {} for remaining set of tables'. format(trigger_date))
    trigger_dqhi_calculation(trigger_date)
    
def delete_trigger_entry(sqlite_db_query_executor):
    sqlite_db_query_executor.execute_query(constants.DQHI_TRIGGER_DELETE_QUERY)
    
    
def execute_wrapper_for_trigger_date(argv=sys.argv):
    rerun=''
    if len(sys.argv) == 2:
        rerun=sys.argv[1]
    db_conn_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
    sqlite_db_query_executor = SqliteDBExecutor(db_conn_obj)
    check_for_trigger_entry(sqlite_db_query_executor, rerun)
    delete_trigger_entry(sqlite_db_query_executor)
    sqlite_db_query_executor.close_connection()
    
if __name__:
    execute_wrapper_for_trigger_date()
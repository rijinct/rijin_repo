import os
import sys
from datetime import timedelta, datetime
import dateutil.parser
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi import constants
from com.rijin.dqhi import common_constants
from com.rijin.dqhi import date_utils
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor

if os.getenv('IS_K8S') == 'true':
    LOGGER = common_utils.get_logger_nova()
else:
    LOGGER = common_utils.get_logger()
    os.putenv("PYSPARK_PYTHON", constants.PYTHON3_HOME)
    os.putenv("PYSPARK_DRIVER_PYTHON", constants.PYTHON3_HOME)
    os.putenv("SPARK_HOME", constants.SPARK_HOME)
    os.putenv("PYTHONPATH", constants.PYTHONPATH)
    os.putenv("HADOOP_CONF_DIR", constants.HADOOP_CONF_DIR)
    os.putenv("CLASSPATH", constants.CLASSPATH)


def insert_trigger_table(date,sqlite_db_query_executor):
    sqlite_db_query_executor.execute_query(constants.DQHI_TRIGGER_DELETE_QUERY)
    sqlite_db_query_executor.execute_query(constants.DQHI_TRIGGER_INSERT_QUERY.format(s='score_calculator_wrapper', d=date))

def delete_trigger_entry(sqlite_db_query_executor):
    sqlite_db_query_executor.execute_query(constants.DQHI_TRIGGER_DELETE_QUERY)
    sqlite_db_query_executor.close_connection()

def execute(calculator):
    status = os.system(calculator)
    check_status(status, calculator)


def check_status(status, calculator):
    if 0 == status:
        LOGGER.info("Successfully executed {}".format(calculator))
    else:
        LOGGER.error("Error in executing {}".format(calculator))
        exit(0)


def invoke_data_loader():
    LOGGER.info("Loading Rules to database.... ")
    data_loader = '{} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.DQ_DATA_LOADER)
    execute(data_loader)


def trigger_dqhi_calculation(date):
    validate(date)
    if os.getenv('IS_K8S') == 'true':
        db_conn_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
        sqlite_db_query_executor = SqliteDBExecutor(db_conn_obj)
        insert_trigger_table(date,sqlite_db_query_executor)
        LOGGER.info("Inserted entry into Trigger table....")
    LOGGER.info("Dropping DQ data older than {} days....".format(common_constants.DATA_RETENTION_DAYS))
    dq_db_data_deleter = '{} {} {}'.format(constants.PYTHON3_HOME, "-m",  constants.DQ_DB_DATA_DELETER)
    execute(dq_db_data_deleter)
    LOGGER.info("Starting Score Calculation....")
    score_calculator = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.SCORE_CALCULATOR, date)
    execute(score_calculator)
    LOGGER.info("Starting KPI CDE Score Calculation....")
    kpi_cde_score_calculator = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.KPI_CDE_SCORE_CALCULATOR, date)
    execute(kpi_cde_score_calculator)
    LOGGER.info("Exporting Rules, Results and Statistics XMLs....")
    dq_data_exporter = '{} {} {} {}'.format(constants.PYTHON3_HOME, "-m", constants.DQ_DATA_EXPORTER, date)
    execute(dq_data_exporter)
    if os.getenv('IS_K8S') == 'true':
        delete_trigger_entry(sqlite_db_query_executor)



def execute_for_date_range(args):
    startDate = datetime.strptime(args[1], datetimeFormat)
    endDate = datetime.strptime(args[2], datetimeFormat)
    date_range = endDate - startDate
    for i in range(date_range.days + 1):
        trigger_dqhi_calculation((startDate + timedelta(days=i)).strftime(datetimeFormat))


def usage(args):
    LOGGER.info("USAGE 1. {} {}".format(constants.PYTHON3_HOME, args[0]))
    LOGGER.info("USAGE 2. {} {} <date_to_calculate_dq>".format(constants.PYTHON3_HOME, args[0]))
    LOGGER.info("USAGE 3. {} {} <start_date_to_calculate_dq> <end_date_to_calculate_dq>".format(constants.PYTHON3_HOME, args[0]))
    LOGGER.info("NOTE : Date format should be yyyy-mm-dd")


def validate(startDate):
    start = dateutil.parser.parse(startDate)
    daterange = datetime.now() - start
    if daterange.days > common_constants.MAX_START_DATE_RANGE:
        LOGGER.info("Stopped execution : Start/Compute date cannot be older than  : {} days".format(common_constants.MAX_START_DATE_RANGE))
        sys.exit(1)


def main():
    global datetimeFormat
    invoke_data_loader()
    datetimeFormat = '%Y-%m-%d'
    if (len(sys.argv) == 3):
        execute_for_date_range(sys.argv)
    elif(len(sys.argv) == 2 and (sys.argv[1] != '--help')):
        trigger_dqhi_calculation(sys.argv[1])
    elif ((len(sys.argv) == 2) and (sys.argv[1] == '--help')):
        usage(sys.argv)
    else:
        LOGGER.info("No arguments provided. By default DQ will be calculated for previous date : {}".format(date_utils.get_previous_date()))
        LOGGER.info("run command : {} {} --help for details on how to run ".format(constants.PYTHON3_HOME, sys.argv[0]))
        trigger_dqhi_calculation(date_utils.get_previous_date().strftime(datetimeFormat))


if __name__ == "__main__":
    main()

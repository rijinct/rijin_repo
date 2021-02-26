from datetime import datetime
import os
import sys
import traceback
import xml.dom.minidom
import psycopg2
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi import common_utils
from com.rijin.dqhi.connection_wrapper import ConnectionWrapper
from com.rijin.dqhi import constants
from com.rijin.dqhi import date_utils
from com.rijin.dqhi.query_executor import DBExecutor
from com.rijin.dqhi import dq_db_xml_export
from com.rijin.dqhi.create_sqlite_connection import SqliteDbConnection
from com.rijin.dqhi.sqlite_query_executor import SqliteDBExecutor

if os.getenv('IS_K8S') != "true": 
    exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read().replace("(", "(\"").replace(")", "\")"))
    LOGGER = common_utils.get_logger()
else:
    LOGGER = common_utils.get_logger_nova()

currentDate = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def exportDataToXML(date):
    outputDirectory = constants.DQHI_OUTPUT
    outputfilenames = []
    if not os.path.exists(outputDirectory):
        os.makedirs(outputDirectory)
        os.system('chown ngdb:ninstall {}'.format(outputDirectory))
    with os.scandir(constants.DQHI_EXPORT_SQL) as it:
        for entry in it:
            if entry.name.endswith(".sql") and entry.is_file():
                query = open(entry.path, 'r').read().replace('DATA_DATE', date)
                fileName = (entry.name).replace('.sql', '')
                outputFileName = '{outputDirectory}/{fileName}_{currentDate}.xml'.format(outputDirectory=outputDirectory, fileName=fileName, currentDate=currentDate)
                #os.mknod(outputFileName)
                try:
                    if os.getenv('IS_K8S') == "true": 
                        dq_db_xml_export.execute(date,fileName,outputFileName)
                    else:
                        con = psycopg2.connect(database=project_sdk_db_name, user=project_sdk_schema_name, password="", host=project_postgres_sdk_fip_active_host, port=project_application_sdk_db_port)
                        cur = con.cursor()
                        with open(outputFileName, 'w') as file_obj:
                            cur.copy_expert(query, file_obj, size=8192)
                        withoutNewLine = open(outputFileName, 'r').read().replace('\\n', '')
                        open(outputFileName, 'w').write(withoutNewLine)
                        dom = xml.dom.minidom.parse(outputFileName)
                        pretty_xml = dom.toprettyxml()
                        open(outputFileName, 'w').write(pretty_xml)
                        con.close()
                    LOGGER.info("{} data exported to {}".format(fileName, outputFileName))                   
                    outputfilenames.append(outputFileName)
                except:
                        LOGGER.info("Error executing query to export")
                        LOGGER.info(traceback.format_exc())
                        
    generateStatistics(outputfilenames, date)


def generateStatistics(outputfilenames, date):
    LOGGER.info("----------------------------------------------------------------------------------------------")
    LOGGER.info("|                      DQHI Statistics for the date {}".format(date))
    LOGGER.info("----------------------------------------------------------------------------------------------")
    if os.getenv('IS_K8S') == "true": 
        query = (constants.STATISTICS_QUERY_NOVA).replace('DATA_DATE', date).replace('TODAY_DATE', date_utils.get_current_date().strftime('%Y-%m-%d'))
        sqlite_db_connection_obj = SqliteDbConnection(constants.DQHI_SQLITE_DB).get_connection()
        sqlite_db_query_executor = SqliteDBExecutor(sqlite_db_connection_obj)
        statlist = sqlite_db_query_executor.fetch_result(query)
        kpi_list = sqlite_db_query_executor.fetch_result(constants.KPI_CDE_STATS_QUERY_NOVA.format(date))
    else:
        query = (constants.STATISTICS_QUERY).replace('DATA_DATE', date).replace('TODAY_DATE', date_utils.get_current_date().strftime('%Y-%m-%d'))
        sai_db_connection_obj = ConnectionWrapper.get_postgres_connection_instance()
        sai_db_query_executor = DBExecutor(sai_db_connection_obj)
        statlist = sai_db_query_executor.fetch_list_of_records(query)
        kpi_list = sai_db_query_executor.fetch_list_of_records(constants.KPI_CDE_STATS_QUERY.format(date))
        sai_db_query_executor.close_connection()
    for entry in statlist:
        LOGGER.info("|     {}".format(entry))
    for filename in outputfilenames:
        LOGGER.info("|     Count of exported data in {} is : {}".format(os.path.basename(filename), open(filename, 'r').read().replace('<rule>','<row>').replace('<statistics>','<row>').replace('<result>','<row>').count("<row>")))
    LOGGER.info("----------------------------------------------------------------------------------------------")
    LOGGER.info("|                      KPI CDE Statistics")
    LOGGER.info("----------------------------------------------------------------------------------------------")
    if not kpi_list:
        LOGGER.info("|     KPI-CDE sheet looks to be empty. Hence, nothing to export!")
    else:    
        LOGGER.info("|     Successfully computed the KPI-CDE scores for the KPI's:{}".format(kpi_list))
    LOGGER.info("----------------------------------------------------------------------------------------------")
    
    
def main():

    exportDataToXML(sys.argv[1])


if __name__ == "__main__":
    main()

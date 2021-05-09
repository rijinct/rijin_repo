#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script create MONITORING Table, Indices & Events
# Date:   15-10-2019
#############################################################################
#############################################################################
# Code Modification History
# 1.
#
# 2.
#############################################################################
import os,sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from dbConnectionManager import DbConnection
from propertyFileUtil import PropertyFileUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

def createDbEntities():
    logger.info("Starting Event Scheduler..")
    scheduler_query = PropertyFileUtil('eventSchedulerStartQuery','mariadbSqlSection').getValueForKey()
    logger.debug("Event Scheduler Query is: '%s' ",scheduler_query)
    DbConnection().getMonitoringConnectionObject(scheduler_query)
    logger.info("Creating Events for auto-clean up..")
    allMariaDbItems = PropertyFileUtil('allitems','mariadbSqlSection').getAllItemsInSection()
    for key in allMariaDbItems.keys():
        if "eventquery" in key:
            event_query = allMariaDbItems[key]
            event_name = allMariaDbItems[key].split(" ")[2]
            logger.info("Event Query is: '%s' and Event Name is : '%s' ",event_query, event_name)
            event_exists_query = PropertyFileUtil('eventExistsQuery','mariadbSqlSection').getValueForKey()
            final_event_exists_query = event_exists_query.replace("EVENTNAME",event_name)
            query_res = DbConnection().getMonitoringConnectionObject(final_event_exists_query)
            if not query_res:
                DbConnection().getMonitoringConnectionObject(allMariaDbItems[key])
            else:
                logger.debug("Event '%s' already exists", event_name)
        else:
            logger.debug("It is not Event Query, not executing")
    logger.info("Events creation Successful")

def create_table():
    tableCreateQuery = PropertyFileUtil('tableCreationQuery','mariadbSqlSection').getValueForKey()
    indexCreateQuery = PropertyFileUtil('indexCreationQuery','mariadbSqlSection').getValueForKey()
    DbConnection().getMonitoringConnectionObject(tableCreateQuery)
    DbConnection().getMonitoringConnectionObject(indexCreateQuery)
    logger.info("Table & Index creation successful")

def checkTableExists():
    tableExistsSql = PropertyFileUtil('tableExistsQuery','mariadbSqlSection').getValueForKey()
    queryRes = DbConnection().getMonitoringConnectionObject(tableExistsSql)
    if queryRes:
        logger.info("Table HM_STATS already exists")
        createDbEntities()
    else:
        create_table()
        createDbEntities()


def main():
    checkTableExists()


if __name__=='__main__':
    main()
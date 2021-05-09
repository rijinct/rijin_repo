#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  Monitoring Team
# Version: 0.1
# Purpose: This class gets connection to the hdfs
# Object
# Date: 23-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
from dbConnectionManager import DbConnection
from py4j.java_gateway import Py4JError, traceback
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class HdfsManager:


        def getCountofFiles(self,directory,fileFormat=None):
                gatewayObj = DbConnection().initialiseGateway()
                try:
                    backlogManager = gatewayObj.jvm.com.nokia.monitoring.hive.BacklogManager("/etc/hadoop/conf/")
                    logger.info("Established Connection to HDFS")                    
                    filesCount = backlogManager.getFileCount(directory,fileFormat)
                except Py4JError:
                        traceback.print_exc()
                finally:
                        gatewayObj.shutdown()
                return filesCount

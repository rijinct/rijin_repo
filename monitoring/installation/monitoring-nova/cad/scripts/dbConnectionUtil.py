#! /usr/bin/python
#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This class launches gateway server in JVM & gets connection
# Object
# Date: 23-01-2018
#############################################################################
#############################################################################
# Code Modification History
# 1. First draft with basic functionality
#
# 2.
#############################################################################
import os

from generalPythonUtils import PythonUtils
from logger_util import *
from propertyFileUtil import PropertyFileUtil
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters, launch_gateway, Py4JError, traceback
import pandas as pd
import numpy as np
from dbConnectionManager import DbConnection

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class DbConnectionUtil:

         def getDataframeFromSql(self,sql):
                gatewayObj = DbConnection().initialiseGateway()
                try:
                        gatewayObj.jvm.Class.forName(os.environ['CEMOD_SDK_DB_DRIVER'])
                        dbUrl = os.environ['CEMOD_SDK_DB_URL']
                        dbUser = os.environ['CEMOD_SDK_SCHEMA_NAME']
                        dbPassword = ""
                        connectionObject = gatewayObj.jvm.java.sql.DriverManager.getConnection(dbUrl,dbUser,dbPassword)
                        logger.info("Established DB Connection for Postgres")
                        statement = connectionObject.createStatement()
                        rs = statement.executeQuery(sql)
                        connectionObject.close()
                        columns = PythonUtils(rs).get_columns_as_string()
                        colCount = rs.getMetaData().getColumnCount()+1
                        index = np.arange(1, colCount)
                        df = pd.DataFrame(columns=index)
                        while rs.next():
                            myList = []
                            for i in range(1,colCount):
                                myList.append(rs.getString(i))
                            newRow = pd.DataFrame([myList], columns = list(df.columns))
                            df = df.append([newRow], ignore_index=True)
                        df.columns = columns.split(",")
                except Py4JError:
                        traceback.print_exc()
                finally:
                        gatewayObj.shutdown()
                return df

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

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

class DbConnection:


        def initialiseGateway(self,db_type=None):
                if not db_type:
                    db_type="hive"
                gatewayPort = launch_gateway(classpath="/opt/nsn/ngdb/monitoring/ext-dependencies/{0}-lib/*".format(db_type))
                gatewayObj = JavaGateway(gateway_parameters=GatewayParameters(port=gatewayPort),callback_server_parameters=CallbackServerParameters(port=0))
                pythonPort = gatewayObj.get_callback_server().get_listening_port()
                gatewayObj.java_gateway_server.resetCallbackClient(gatewayObj.java_gateway_server.getCallbackClient().getAddress(),pythonPort)
                return gatewayObj


        def getConnectionAndExecuteSql(self,sql,dbType,qsType=None,columns=None):
                gatewayObj = DbConnection().initialiseGateway()
                try:
                        if dbType.lower() == 'postgres':
                                gatewayObj.jvm.Class.forName(os.environ['CEMOD_SDK_DB_DRIVER'])
                                dbUrl = os.environ['CEMOD_SDK_DB_URL']
                                dbUser = os.environ['CEMOD_SDK_SCHEMA_NAME']
                                dbPassword = ""
                        elif dbType.lower() == 'mariadb':
                                gatewayObj.jvm.Class.forName(os.environ["CEMOD_MARIA_DB_DRIVER"])
                                if qsType.lower() == "webservice":
                                        dbUrl = 'jdbc:mariadb://{0}.{1}:{2}/{3}'.format(os.environ['CEMOD_WS_SERVICE_NAME'],os.environ['CEMOD_DOMAIN_NAME'],os.environ['CEMOD_WS_PORT'],os.environ['CEMOD_WS_DB_SCHEMA_NAME'])
                                        dbUser= os.environ['CEMOD_WS_USER']
                                        dbPassword = os.environ['CEMOD_WS_PASSWORD']
                                elif qsType.lower() == "queryscheduler":
                                        dbUrl = 'jdbc:mariadb://{0}.{1}:{2}/{3}'.format(os.environ['CEMOD_QS_SERVICE_NAME'],os.environ['CEMOD_DOMAIN_NAME'],os.environ['CEMOD_QS_PORT'],os.environ['CEMOD_QS_DB_SCHEMA_NAME'])
                                        dbUser = os.environ['CEMOD_QS_USER']
                                        dbPassword = os.environ['CEMOD_QS_PASSWORD']
                        connectionObject = gatewayObj.jvm.java.sql.DriverManager.getConnection(dbUrl,dbUser,dbPassword)
                        logger.info("Established DB Connection for '%s' ",dbType)
                        statement = connectionObject.createStatement()
                        rs = statement.executeQuery(sql)
                        connectionObject.close()
                        if columns:
                            return PythonUtils(rs).get_columns_as_string()
                        queryResult = PythonUtils(rs).convertRsToString()
                except Py4JError:
                        traceback.print_exc()
                finally:
                        gatewayObj.shutdown()
                return queryResult

        def getHiveConnectionAndExecute(self,hiveSql):
                gatewayObj = DbConnection().initialiseGateway()
                connectionObject = 'NULL'
                try:
                    gatewayObj.jvm.com.nokia.monitoring.hive.HiveConnectionInitialiser.initialiseConnection()
                    connectionObject = gatewayObj.jvm.com.nokia.monitoring.hive.HiveCommandExecutor.getConnection()
                    logger.info("Established Db connection to Hive")
                    statement = connectionObject.createStatement()
                    queryHints = PropertyFileUtil('hiveHints','hiveSqlSection').getValueForKey()
                    for queryHint in queryHints.split(","):
                        statement.execute(queryHint.strip())
                    rs = statement.executeQuery(hiveSql)
                    queryResult = PythonUtils(rs).convertRsToString()
                except Py4JError:
                    traceback.print_exc()
                finally:
                    if connectionObject != 'NULL':
                        gatewayObj.jvm.com.nokia.monitoring.hive.HiveCommandExecutor.closeConnection(connectionObject)
                    gatewayObj.shutdown()
                return queryResult

        def getMonitoringConnectionObject(self,sql,columns=None):
                gatewayObj = DbConnection().initialiseGateway()
                try:
                    gatewayObj.jvm.Class.forName(os.environ["CEMOD_MARIA_DB_DRIVER"])
                    dbUrl = 'jdbc:mariadb://{0}.{1}:{2}/{3}'.format(os.environ['CEMOD_MONITORING_SERVICE_NAME'],os.environ['CEMOD_DOMAIN_NAME'],os.environ['CEMOD_MONITORING_PORT'],os.environ['CEMOD_MONITORING_SCHEMA_NAME'])
                    dbUser= os.environ['CEMOD_MONITORING_USER']
                    dbPassword = os.environ['CEMOD_MONITORING_PASSWORD']
                    connectionObject = gatewayObj.jvm.java.sql.DriverManager.getConnection(dbUrl,dbUser,dbPassword)
                    statement=connectionObject.createStatement()
                    rs = statement.executeQuery(sql)
                    connectionObject.close()
                    if columns:
                        return PythonUtils(rs).get_columns_as_string()
                    queryResult = PythonUtils(rs).convertRsToString()
                except Py4JError:
                    traceback.print_exc()
                finally:
                    gatewayObj.shutdown()
                return queryResult

        def get_spark_connection_and_execute(self,sql,thrift_server_name):
                gatewayObj = DbConnection().initialiseGateway("spark")
                connectionObject = 'NULL'
                try:
                    gatewayObj.jvm.com.nokia.monitoring.spark.SparkConnectionInitialiser.initialiseConnection(thrift_server_name)
                    connectionObject = gatewayObj.jvm.com.nokia.monitoring.spark.SparkCommandExecutor.getConnection()
                    logger.info("Established Db connection to Spark")
                    statement = connectionObject.createStatement()
                    rs = statement.executeQuery(sql)
                    queryResult = PythonUtils(rs).convertRsToString()
                except Py4JError:
                    traceback.print_exc()
                finally:
                    if connectionObject != 'NULL':
                        gatewayObj.jvm.com.nokia.monitoring.spark.SparkCommandExecutor.closeConnection(connectionObject)
                    gatewayObj.shutdown()
                return queryResult

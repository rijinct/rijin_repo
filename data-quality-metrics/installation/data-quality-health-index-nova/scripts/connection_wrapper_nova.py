import os,sys
sys.path.insert(0,'/usr/local/lib/python3.6/site-packages')
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters, launch_gateway, Py4JError, traceback

class ConnectionWrapper:


        def __init__(self):
            self.gatewayPort = launch_gateway(classpath="/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/ext-dependencies/*")
            self.gatewayObj = JavaGateway(gateway_parameters=GatewayParameters(port=self.gatewayPort),callback_server_parameters=CallbackServerParameters(port=0))
            self.pythonPort = self.gatewayObj.get_callback_server().get_listening_port()
            self.gatewayObj.java_gateway_server.resetCallbackClient(self.gatewayObj.java_gateway_server.getCallbackClient().getAddress(),self.pythonPort)
            
        def get_gateway(self):
            return self.gatewayObj
            
        def get_postgres_connection_instance(self):
            try:
                self.gatewayObj.jvm.Class.forName(os.environ['PROJECT_SDK_DB_DRIVER'])
                dbUrl = os.environ['PROJECT_SDK_DB_URL']
                dbUser = os.environ['PROJECT_SDK_SCHEMA_NAME']
                dbPassword = ""
                connectionObject = self.gatewayObj.jvm.java.sql.DriverManager.getConnection(dbUrl,dbUser,dbPassword)
            except Py4JError:
                traceback.print_exc()
            return connectionObject
        
        def get_hive_metastore_connection_instance(self):
            try:
                self.gatewayObj.jvm.Class.forName(os.environ['PROJECT_MARIA_DB_DRIVER'])
                hiveMetastoreIp = os.environ['PROJECT_HIVE_METASTORE_IP']
                hiveMetastorePort = os.environ['PROJECT_HIVE_METASTORE_PORT']
                hiveMetastoreDb = os.environ['PROJECT_HIVE_METASTORE_DB']
                dbUser = os.environ['PROJECT_HIVE_METASTORE_USERNAME']
                dbPassword = os.environ['PROJECT_HIVE_METASTORE_PASSWORD']
                dbUrl = 'jdbc:mariadb://{i}:{p}/{d}'.format(i=hiveMetastoreIp,p=hiveMetastorePort,d=hiveMetastoreDb)
                connectionObject = self.gatewayObj.jvm.java.sql.DriverManager.getConnection(dbUrl,dbUser,dbPassword)
            except Py4JError:
                traceback.print_exc()
            return connectionObject
        
        def close_connection_gateway(self):
            self.gatewayObj.shutdown()

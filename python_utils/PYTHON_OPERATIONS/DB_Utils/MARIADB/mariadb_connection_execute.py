'''
Created on 23-Aug-2020

@author: rithomas
'''

import mysql.connector
from mysql.connector import errorcode
from WORKFLOW_CONTEXT.workflow_context import WorkflowContext
from abc import ABC,abstractmethod

class MariaDBConnExec(ABC):
    
    def __init__(self,context):
        self.context = context
   
    def connectMariadb_executeQuery(self):
        try:
            self.connection = mysql.connector.connect(user='root', password='zaq12wsx', host='localhost')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            print("You are connected!")
            #executeSQL(connection, query)
            

    def close_connection(self):
        self.connection.close()
    
    @abstractmethod    
    def execute_TLCquery(self):
        pass
        
    def execute(self,context):
        self.context = context
        self.connectMariadb_executeQuery()
        if (self.context.getProperty('TLC') == 'True'):
            self.execute_TLCquery()
        self.close_connection()
        
if __name__ == "__main__":
    MariaDBConnExec().execute()
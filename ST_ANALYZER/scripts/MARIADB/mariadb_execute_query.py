'''
Created on 22-Aug-2020

@author: rithomas
'''

import mysql.connector
from mysql.connector import errorcode
 

def executeSQL(conn, query):
    cursor = conn.cursor()
    #query = "select ASIN, Description from Amazon where Description like " + queryTerm
    cursor.execute(query)
    conn.commit()
    #result = cursor.fetchall()    
    #for row in result:
        #print (row)

def connectMariadb_executeQuery(query):
    try:
        connection = mysql.connector.connect(user='root', password='zaq12wsx', host='localhost')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        #print("You are connected!")
        executeSQL(connection, query)
    
    connection.close()

if __name__ == '__main__':
    query = "SELECT * FROM testrijdb.st_analysis_5 limit 10"
    connectMariadb_executeQuery(query)

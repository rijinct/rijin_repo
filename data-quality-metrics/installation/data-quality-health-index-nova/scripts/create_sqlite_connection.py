import sqlite3
from sqlite3 import Error


class SqliteDbConnection:
    
    def __init__(self, file):
        self.file = file
        self.conn = None 

    def get_connection(self):
        try:
            self.conn = sqlite3.connect(self.file)
        except Error as e:
            print(e)   
        return self.conn
         

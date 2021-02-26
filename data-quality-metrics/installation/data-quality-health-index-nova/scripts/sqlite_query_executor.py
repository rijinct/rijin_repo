from com.rijin.dqhi import constants
from com.rijin.dqhi.generalPythonUtils import PythonUtils
from sqlite3 import Error

class SqliteDBExecutor:
    
    def __init__(self, connection):
        self.connection = connection
        self.cur = self.connection.cursor()
    
    def execute_query(self, sql):
        try:
            self.connection.execute(sql)
            self.connection.commit()
        except Error as e:
            print(e)  
                  
    def fetch_result(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()
    
    def fetch_table_column_details(self, queries):
        pass
    
    def fetch_rule_name_id_dict(self):
        result = self.fetch_result(constants.QUERY_RULE_NAME_RULE_ID)
        dict = {}
        for row in result:
            if row[3] != None:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip() + "," + row[3].strip().replace(",","|")
            else:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip()+ ", " 
        return dict

    def close_connection(self):
        self.connection.close()

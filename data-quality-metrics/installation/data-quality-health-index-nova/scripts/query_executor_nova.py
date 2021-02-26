from com.rijin.dqhi import constants
from com.rijin.dqhi.generalPythonUtils import PythonUtils


class DBExecutor:
    
    def __init__(self, connection):
        self.connection = connection
        self.statement = self.connection.createStatement()
    
    def execute_query(self, sql):
        self.statement.executeUpdate(sql)
        #self.connection.commit()
    
    def fetch_result(self, sql):
        rs = self.statement.executeQuery(sql)
        records = PythonUtils(rs).convertRsToString()
        return list(records.split("\n"))
    
    def fetch_result_as_tuple(self, sql):
        rs = self.statement.executeQuery(sql)
        records = PythonUtils(rs).convertRsToString()
        d = dict(x.split(",") for x in records.split("\n"))
        result = []
        for key, value in d.items():
            result.append((key,value))
        return result                         
    
    def fetch_table_column_details(self, queries):
        response_dictionary = {}
        table_column_list = []
        for query in queries:
            result = self.fetch_result(query)
            for row in result:
                row = list(row.split(","))
                response_dictionary[row[0]] = dictionaryElements = response_dictionary.get(row[0], {})
                dict_obj = ([ (row[0], row[1]) , (row[2], row[3]) , (str(row[2]) + "_datatype", row[4]) , (str(row[2]) + "_precision" , row[5])])
                dictionaryElements.update(dict_obj)
                table_column_list.append([row[0], row[2]])
        return response_dictionary, table_column_list
    
    def fetch_records_as_list(self, sql):
        myList = []
        result = self.fetch_result(sql)
        for row in result:
            row = list(row.split("\n"))
            myList.append(row[0] + "," + row[1])
        return myList
        
    
    def fetch_list_of_records(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        myList = list(sum(result, ()))
        return myList

    def fetch_dqhi_columns_details(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        return cur.fetchone()

    def fetch_rule_name_id_dict(self):
        result = self.fetch_result(constants.QUERY_RULE_NAME_RULE_ID)
        dict = {}
        for row in result:
            row = list(row.split(","))
            if row[3] != None:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip() + "," + row[3].strip().replace(",","|")
            else:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip()+ ", " 
        return dict
    
    def close_connection(self):
        self.connection.close()

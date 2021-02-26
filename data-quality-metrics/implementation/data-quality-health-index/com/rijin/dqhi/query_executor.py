import constants


class DBExecutor:
    
    def __init__(self, connection):
        self.connection = connection
    
    def execute_query(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        self.connection.commit()
    
    def fetch_result(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        return records
    
    def fetch_table_column_details(self, queries):
        cur = self.connection.cursor()
        response_dictionary = {}
        table_column_list = []
        for query in queries:
            cur.execute(query)
            result = list(cur.fetchall())
            for row in result:
                response_dictionary[row[0]] = dictionaryElements = response_dictionary.get(row[0], {})
                dict_obj = ([ (row[0], row[1]) , (row[2], row[3]) , (str(row[2]) + "_datatype", row[4]) , (str(row[2]) + "_precision" , row[5])])
                dictionaryElements.update(dict_obj)
                table_column_list.append([row[0], row[2]])
        return response_dictionary, table_column_list
    
    def fetch_records_as_list(self, sql):
        myList = []
        cur = self.connection.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        for row in result:
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
        cur = self.connection.cursor()
        cur.execute(constants.QUERY_RULE_NAME_RULE_ID)
        result = cur.fetchall()
        dict = {}
        for row in result:
            if row[3] != None:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip() + "," + row[3].strip().replace(",","|")
            else:
                dict[row[0].strip()] = str(row[1]) + "," + row[2].strip()+ ", " 
        return dict
    
    def close_connection(self):
        self.connection.close()

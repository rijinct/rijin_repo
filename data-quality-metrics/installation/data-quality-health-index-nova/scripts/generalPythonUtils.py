from com.rijin.dqhi.propertyFileUtil import PropertyFileUtil
from ast import literal_eval

class PythonUtils:


        def __init__(self,resultSet):
                self.resultSet = resultSet
                self.rsmd = self.resultSet.getMetaData()
                self.columnNum = self.rsmd.getColumnCount()+1

        def convertRsToString(self):
                flag = 0
                rowString = " "
                finalString = " "
                while self.resultSet.next():
                        for i in range(1,self.columnNum):
                                colValue = self.resultSet.getString(i)
                                if self.resultSet.getString(i) is None:
                                        colValue = 'NULL'
                                rowString = rowString + "," + str(colValue)
                                flag = flag + 1
                                if flag is self.columnNum-1:
                                        rowString = rowString.strip().lstrip(",") + "\n"
                                        finalString = finalString + rowString
                                        rowString = " "
                                        flag = 0
                return finalString.strip()

        def get_columns_as_string(self):
            finalString = ""
            for i in range(1, self.rsmd.getColumnCount()+1):
                finalString = finalString + "," + self.rsmd.getColumnLabel(i)
            return finalString.rstrip(",").lstrip(",")


        def getValueForColumn(self,columnName):
                while self.resultSet.next():
                        for i in range(1,self.columnNum):
                                if columnName.lower() == str(self.rsmd.getColumnName(i)):
                                        if self.resultSet.getString(i) is None:
                                                colValue = "0"
                                        else:
                                                colValue = self.resultSet.getString(i)
                                        return int(colValue)


        def storageManagement():
                cleanupDirectories=PropertyFileUtil.getAllDirectories()
                now = time.time()
                for directory in cleanupDirectories:
                        for filename in os.listdir(directory):
                                if os.path.getmtime(os.path.join(directory, filename)) < now - 1 * 86400:
                                        os.remove(os.path.join(directory, filename))

        @staticmethod
        def convert_string_to_list_dict(string_dict):
               string_dict_list = string_dict.split("\n")
               return [ literal_eval(item) for item in string_dict_list]

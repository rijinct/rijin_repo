import os,sys,time
from datetime import datetime,date
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from jsonUtils import JsonUtils
from csvUtil import CsvUtil
from propertyFileUtil import PropertyFileUtil
from dbUtils import DBUtil

def createDirAndFiles():
    global fileName
    #creating backup file
    os.system('dd if=/dev/zero of=/tmp/hadoop_monitoring.test  bs=1M count=20000  oflag=direct')
    #creating hadoop dir
    os.system('hdfs dfs -mkdir /tmp/hadoop.delete')
    #creating local dir
    os.system('mkdir -p /tmp/hadoopread')
    fileName = "hdfsPerformance_{0}.csv".format(datetime.now().strftime('%Y-%m-%d-%H'))

def deletingTempFiles():
    os.system('hdfs dfs -rm -r /tmp/hadoop.delete')
    os.system('rm -r /tmp/hadoopread')
    os.system('rm /tmp/hadoop_monitoring.test')

def getHdfsPerformance():
    #time taken to write to hdfs
    start_time=time.time()
    os.system('hdfs dfs -copyFromLocal /tmp/hadoop_monitoring.test /tmp/hadoop.delete')
    hdfswritetime=time.time()-start_time
    #time taken to read from hdfs
    start_time=time.time()
    os.system('hdfs dfs -copyToLocal /tmp/hadoop.delete /tmp/hadoopread/')
    hdfsreadtime=time.time()-start_time
    #writing to csv
    log=str(date.today())+","+str(int(round(hdfsreadtime,0)))+","+str(int(round(hdfswritetime,0)))
    CsvUtil().writeToCsv(fileName,'hdfsPerformance',log)

def pushDataToPostgresDB():
    outputFilePath = PropertyFileUtil('hdfsPerformance','DirectorySection').getValueForKey()
    csvFile = outputFilePath + fileName
    jsonFileName=JsonUtils().convertCsvToJson(csvFile)
    DBUtil().pushDataToPostgresDB(jsonFileName,"hdfsPerformance")

def main():
    try:
        createDirAndFiles()
        getHdfsPerformance()
        pushDataToPostgresDB()
    except Exception as e:
        print('Error : %s'%e.message)
    finally:
        deletingTempFiles()

if __name__=='__main__':
    main()
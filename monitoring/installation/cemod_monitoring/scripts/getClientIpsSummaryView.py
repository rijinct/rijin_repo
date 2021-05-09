#############################################################################
#############################################################################
# (c)2016 NOKIA
# Author: deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose: This script parses the ssl access logs and creates the summary
# view of client Ip's analysis
# Date : 18-11-2019
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
import commands,sys,datetime,os
from xml.dom import minidom
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from loggerUtil import loggerUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from propertyFileUtil import PropertyFileUtil
from writeCsvUtils import CsvWriter

exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(","(\"").replace(")","\")")
currentDate= datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
scriptName = os.path.basename(sys.argv[0]).replace('.py','')
logFile= '{scriptName}_{currentDate}'.format(scriptName=scriptName,currentDate=currentDate)
LOGGER = loggerUtil.__call__().get_logger(logFile)

def writeToCsv(portalHost,summary):
        global outputFileName
        outputDirectory = PropertyFileUtil('queriesPerClient','DirectorySection').getValueForKey()
        outputFileName = outputDirectory + 'queriesScreenSummary_{0}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
        header = PropertyFileUtil('queriesPerClient','HeaderSection').getValueForKey()
        for key in summary.keys():
                content = '{0},{1},{2},{3},{4},{5}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),portalHost,key,summary[key]["totalQueries"],summary[key]["querieswith401"],summary[key]["querieswith403"])
                LOGGER.info("Writing to csv for the portal node".format(portalHost))
                csvWriter = CsvWriter('queriesPerClient','queriesScreenSummary_{0}.csv'.format(datetime.datetime.now().strftime("%Y-%m-%d")),content)

def pushToPostgres():
        global outputFileName
        try:
                jsonFileName=JsonUtils().convertCsvToJson(outputFileName)
                LOGGER.info("Inserting data to DB")
                DBUtil().pushDataToPostgresDB(jsonFileName,"queriesPerClient")
        except NameError:
                LOGGER.info("No File to convert to json & insert to db")

def parseAndPrepareSummary(lastHourData,portalHost):
        clientIps = set()
        for item in lastHourData:
                print item
                clientIps.add(item.strip("\n").split(" ")[0])
        queriesSummary = {}
        for clientIp in clientIps:
                totalQueries = 0
                querieswith401 = 0
                querieswith403 = 0
                queriesOutput = {}
                for item in lastHourData:
                        if clientIp in item.strip("\n"):
                                totalQueries = totalQueries + 1
                        if clientIp in item.strip("\n") and 'HTTP/1.1" 401' in item:
                                querieswith401 = querieswith401 + 1
                        if clientIp in item.strip("\n") and 'HTTP/1.1" 403' in item:
                                querieswith403 = querieswith403 + 1
                queriesOutput["totalQueries"] = totalQueries
                queriesOutput["querieswith401"] = querieswith401
                queriesOutput["querieswith403"] = querieswith403
                queriesSummary[clientIp] = queriesOutput
        LOGGER.info('Queries Summary is {0}'.format(queriesSummary))
        writeToCsv(portalHost,queriesSummary)


def fetchLastHourDataFromAccessLog():
        portalHosts = cemod_portal_hosts.split(" ")
        allRequestsPattern = "/cemboard-service-api/api/cemod/v1/insights HTTP/1.1"
        lastHourFormat = (datetime.datetime.now()-datetime.timedelta(hours=1)).strftime("%d/%b/%Y:%H")
        if len(portalHosts) >= 1:
                for portalHost in portalHosts:
                        LOGGER.info('Parsing the logs for {0}'.format(portalHost))
                        latestLogFile = commands.getoutput("""ssh {0} "ls -lrt /etc/httpd/logs/*ssl_access_log* | tail -1 | gawk -F\\" \\" '{{print \\$9}}' " """.format(portalHost))
                        lastHourData = commands.getoutput(""" ssh {0} "grep \\"{1}\\" {2} | grep \\"{3}\\" " """.format(portalHost,lastHourFormat,latestLogFile,allRequestsPattern))
                        if lastHourData:
                                parseAndPrepareSummary(lastHourData.split("\n"),portalHost)
                        else:
                                LOGGER.info("No Matching data found to parse & prepare summary")

def main():
        fetchLastHourDataFromAccessLog()
        pushToPostgres()

if __name__=='__main__':
        main()
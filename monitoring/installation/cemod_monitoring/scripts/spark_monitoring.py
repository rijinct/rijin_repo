#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  deeraj.kumar_y_m_v@nokia.com
# Version: 0.1
# Purpose:This script sends rest request to spark
# and collects the required statistics
#
# .
# Date : 29-01-2020
#############################################################################
#############################################################################
# Code Modification History
# 1. First Draft
#
# 2.
#############################################################################
import commands, sys, datetime, os, json, itertools, ast
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from loggerUtil import loggerUtil
from csvUtil import CsvUtil
from dbUtils import DBUtil
from propertyFileUtil import PropertyFileUtil
from jsonUtils import JsonUtils

current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_file = '{scriptName}_{currentDate}'.format(scriptName=script_name, currentDate=current_date)
LOGGER = loggerUtil.__call__().get_logger(log_file)
exec(open("/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh").read()).replace("(", "(\"").replace(")", "\")")


def get_json_rest_response(rest_url):
    rest_response = commands.getoutput(rest_url)
    return json.loads(rest_response)


def get_all_applications():
    LOGGER.info('Spark is running on {0} '.format(cemod_spark_hosts))
    LOGGER.info('Getting all applications via end point')
    rest_url = """curl -s http://{0}:18089/api/v1/applications""".format(cemod_spark_hosts)
    rest_response = get_json_rest_response(rest_url)
    LOGGER.info('Response Fetched from the end point')
    return rest_response


def get_only_appids(json_response_objects):
    applicationids = []
    LOGGER.info('Parsing the response..')
    LOGGER.info('Total applications present : {0}'.format(len(json_response_objects)))
    for json_response_obj in json_response_objects:
            applicationids.append(json_response_obj['id'])
    return applicationids


def get_lasthour_appids(appids):
    lasthour_date = (datetime.datetime.now() - datetime.timedelta(hours=6)).strftime("%Y/%m/%d-%H:%M:%S")
    #lasthour_date ="2020/02/17-11:00:00"
    final_appids = []
    for appid in appids:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}""".format(cemod_spark_hosts, appid)
        json_objects = get_json_rest_response(rest_url)
        start_time = json_objects['attempts'][0]['startTime'].rstrip('GMT').replace('T', ' ')
        formatted_start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f').strftime("%Y/%m/%d-%H:%M:%S")
        if formatted_start_time > lasthour_date:
            final_appids.append(appid)
    LOGGER.info("Last Hour Pids are {0}".format(final_appids))
    return final_appids


def get_stageids_information(required_appids):
    appid_stageid_dict = {}
    stage_ids = []
    for required_appid in required_appids:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}/jobs""".format(cemod_spark_hosts, required_appid)        
        try:
            json_objects = get_json_rest_response(rest_url)
            for jobid in json_objects:
                stage_ids.append(jobid['stageIds'])
            stage_ids = []
            appid_stageid_dict[required_appid] = stage_ids
        except ValueError:
            print("Jobs information is not present, so skipping this application id")
    return appid_stageid_dict


def get_count_of_executors(json_response, appid):    
    executor_info = json_response[0]
    executor_summary = executor_info['executorSummary']    
    executor_count = len(executor_summary)    
    return executor_count



def get_queryinfo(appid, stagelist):
    global pyspark_appids
    query_info = {}
    for stageid in stagelist:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}/stages/{2}""".format(cemod_spark_hosts, appid, stageid)
        if rest_url:
            pyspark_appids.append(appid)
        json_response = get_json_rest_response(rest_url)
        get_count_of_executors(json_response, appid)
        stage_info = json_response[0]
        try:
            query_info['query'] = stage_info['description']
            query_info['status'] = stage_info['status']
            query_info['start_time'] = stage_info['submissionTime']
            query_info['end_time'] = stage_info['completionTime']
        except KeyError:
            LOGGER.info("Application Id {0} has different status {1}".format(appid, stage_info['status']))
            if stage_info['status'] == 'SKIPPED':
                query_info['query'] = "No Query Found"
                query_info['status'] = stage_info['status']
                query_info['start_time'] = "No Start Time"
                query_info['end_time'] = "No End Time"
        return (stageid, query_info)

def parse_stages_getquery_info(appids_stageids_dict):
    query_dict = {}
    stageid = ""
    for appid in appids_stageids_dict.keys():
        try:
            no_of_stages_list = len(appids_stageids_dict[appid])
            stages_list = list(itertools.chain.from_iterable(appids_stageids_dict[appid]))
            stageid, query_info = get_queryinfo(appid, stages_list)
            query_dict[appid + '_' + str(stageid)] = query_info
        except (ValueError, TypeError):
            LOGGER.info("Application id {0} has no stage information for the stage {1}".format(appid, stageid))
    LOGGER.info("Final Query Info Dict is {0}".format(query_dict))
    return query_dict


def write_to_csv(queryinfo_dict):
    spark_file_name = "sparkMonitoring_{0}.csv".format(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    for appid in queryinfo_dict.keys():
        query_temp_dict = {}
        query_temp_dict['Date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            query_temp_dict['Application Id'] = appid
            query_temp_dict['Query'] = queryinfo_dict[appid]['query']
            query_temp_dict['Start Time'] = queryinfo_dict[appid]['start_time']
            query_temp_dict['End Time'] = queryinfo_dict[appid]['end_time']
            query_temp_dict['Status'] = queryinfo_dict[appid]['status']
            CsvUtil().writeDictToCsv('sparkMonitoring', query_temp_dict,spark_file_name)
        except KeyError:
            LOGGER.info("No information present to write to Csv")
    LOGGER.info("Writing to the CSV has been completed")

def write_pyspark_info_to_csv(queryinfo_dict):
    py_spark_file_name = "pySparkMonitoring_{0}.csv".format((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    for appid in queryinfo_dict.keys():
        query_temp_dict = {}
        query_temp_dict['Date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            query_temp_dict['Application Id'] = appid
            query_temp_dict['JobName'] = queryinfo_dict[appid]['JobName']
            query_temp_dict['Start Time'] = queryinfo_dict[appid]['StartTime']
            query_temp_dict['End Time'] = queryinfo_dict[appid]['EndTime']
            query_temp_dict['Duration'] = queryinfo_dict[appid]['Duration']
            CsvUtil().writeDictToCsv('pySparkMonitoring', query_temp_dict, py_spark_file_name)
        except KeyError:
            LOGGER.info("No information present to write to Csv")
    LOGGER.info("Writing to the CSV has been completed")


def parse_pyspark_appids_and_get_info(pyspark_appids):
    query_info = {}
    LOGGER.info("Pyspark Appids list is '%s' ", pyspark_appids)
    for appid in pyspark_appids:
        query_info_temp_dict = {}
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}""".format(cemod_spark_hosts, appid)
        json_response = get_json_rest_response(rest_url)
        LOGGER.info("Json Response is {0}".format(json_response))
        query_info_temp_dict['JobName'] = json_response['name']
        query_info_temp_dict['StartTime'] = json_response['attempts'][0]['startTime']
        query_info_temp_dict['EndTime'] = json_response['attempts'][0]['endTime']
        query_info_temp_dict['Duration']= json_response['attempts'][0]['duration']
        query_info[json_response['id']] = query_info_temp_dict
        LOGGER.info("1 value is {0}".format(query_info))
    LOGGER.info("Py Spark Dict is {0}".format(query_info))
    return query_info

def push_to_db():
    py_spark_file_name = "pySparkMonitoring_{0}.csv".format((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    outputFilePath = PropertyFileUtil('pySparkMonitoring','DirectorySection').getValueForKey()
    if os.path.exists(outputFilePath+py_spark_file_name):
        jsonFileName=JsonUtils().convertCsvToJson(outputFilePath+py_spark_file_name)
        DBUtil().pushDataToPostgresDB(jsonFileName,"pySparkMonitoring")
    else:
        LOGGER.info("No info to write for py spark queries")
    spark_file_name = "sparkMonitoring_{0}.csv".format(datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    outputFilePath = PropertyFileUtil('sparkMonitoring','DirectorySection').getValueForKey()
    if os.path.exists(outputFilePath+spark_file_name):
        jsonFileName=JsonUtils().convertCsvToJson(outputFilePath+spark_file_name)
        DBUtil().pushDataToPostgresDB(jsonFileName,"sparkMonitoring")
    else:
        LOGGER.info("No info to write for spark queries")


def main():
    global pyspark_appids,queryinfo_dict, pysparkinfo_dict
    pyspark_appids = []
    rest_response = get_all_applications()
    appids = get_only_appids(rest_response)
    required_appids = get_lasthour_appids(appids)
    appids_stageids_dict = get_stageids_information(required_appids)
    queryinfo_dict = parse_stages_getquery_info(appids_stageids_dict)    
    pysparkinfo_dict = parse_pyspark_appids_and_get_info(pyspark_appids)
    write_to_csv(queryinfo_dict)
    write_pyspark_info_to_csv(pysparkinfo_dict)
    push_to_db()


if __name__ == '__main__':
    main()

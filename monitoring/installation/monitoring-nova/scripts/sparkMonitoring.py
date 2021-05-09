#############################################################################
#############################################################################
# (c)2018 NOKIA
# Author:  Monitoring Team
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
from datetime import datetime as dt
import subprocess, sys, datetime, os, json, itertools, ast
from time import gmtime
import time

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')

from csvUtil import CsvUtil
from dbUtils import DBUtil
from jsonUtils import JsonUtils
from logger_util import *
from propertyFileUtil import PropertyFileUtil


create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))

cemod_spark_hosts = open("/opt/nsn/ngdb/monitoring/extEndPointsConfigMap/CEMOD_HISTORYSERVER_ADDRESS",'r').read()

def get_json_rest_response(rest_url):
    rest_response = subprocess.getoutput(rest_url)
    return json.loads(rest_response)


def get_all_applications():    
    rest_url = """sudo wget --no-check-certificate -O- -q {0}/api/v1/applications/""".format(cemod_spark_hosts)
    rest_response = get_json_rest_response(rest_url)
    return rest_response


def get_only_appids(json_response_objects):
    applicationids = []
    logger.debug('Parsing the response..')
    for json_response_obj in json_response_objects:
            applicationids.append(json_response_obj['id'])
    return applicationids



def get_lasthour_appids(appids):
    global lasthour_date
    gm_str_time = time.strftime("%Y/%m/%d-%H:%M:%S", time.gmtime())
    start_time_obj = dt.strptime(gm_str_time,"%Y/%m/%d-%H:%M:%S")
    lasthour_date = dt.strptime(gm_str_time,"%Y/%m/%d-%H:%M:%S") - datetime.timedelta(hours=1)
    final_appids = []
    for appid in appids:        
        rest_url = """sudo wget --no-check-certificate -O- -q {0}/api/v1/applications/{1}""".format(cemod_spark_hosts, appid)
        json_objects = get_json_rest_response(rest_url)
        start_time = json_objects['attempts'][0]['lastUpdated'].rstrip('GMT').replace('T', ' ')
        formatted_start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f').strftime("%Y/%m/%d-%H:%M:%S")
        formatted_start_obj = dt.strptime(formatted_start_time,"%Y/%m/%d-%H:%M:%S")
        if formatted_start_obj > lasthour_date:
            final_appids.append(appid)
            check_for_pyspark_ids_add(json_objects,appid)
    return final_appids

def check_for_pyspark_ids_add(json_response,appid):
    global pyspark_appids
    try:
        name = json_response['name']
        user = json_response['attempts'][0]['sparkUser']
        if 'SparkSQL' not in appid and user == 'ngdb':
            pyspark_appids.append(appid)
    except KeyError:
        logger.debug("Appid doesn't have name & user Information, so not able to determine as PySpark Application")


def get_stageids_information(required_appids):
    appid_stageid_dict = {}
    stage_ids = []
    for required_appid in required_appids:
        rest_url = """sudo wget --no-check-certificate -O- -q {0}/api/v1/applications/{1}/jobs""".format(cemod_spark_hosts, required_appid)        
        try:
            json_objects = get_json_rest_response(rest_url)
            for jobid in json_objects:
                stage_ids.append(jobid['stageIds'])
            appid_stageid_dict[required_appid] = stage_ids
        except ValueError:
            logger.debug("Jobs information is not present, so skipping the application id")
    return appid_stageid_dict


def get_count_of_executors(json_response, appid):
    executor_info = json_response[0]
    executor_summary = executor_info['executorSummary']
    executor_count = len(executor_summary)
    return executor_count

def query_analysis(query,start_time, end_time,status,stageid):
    global query_hash_map
    if "STREAMTABLE" in query:
        query_list = query.split(" ")
        hash_id = [ item for item in query_list if "STREAMTABLE" in item ][0]
        logger.debug("Hash Id is %s", hash_id)
        if len(query_hash_map) == 0:
            query_hash_map[hash_id] = {'start_time':start_time,'end_time':end_time,'status':status,'query':query,'stageids':'{0}'.format(stageid)}
            logger.debug("1st time Query Hash Map is %s", query_hash_map)
        else:
            if hash_id in query_hash_map:
                end_time_in_hash_map = query_hash_map[hash_id]['end_time']
                query_hash_map[hash_id]['stageids'] = query_hash_map[hash_id]['stageids']+","+str(stageid)
                start_time_in_hash_map = query_hash_map[hash_id]['start_time']
                formatted_start_obj = convert_gmt_time_to_date_obj(start_time_in_hash_map)
                logger.debug("Formatted Start Obj is %s and Last Hour Date is %s ", formatted_start_obj, lasthour_date)
                if formatted_start_obj > lasthour_date:
                    if end_time > end_time_in_hash_map:
                        query_hash_map[hash_id]['end_time'] = end_time
                        query_hash_map[hash_id]['status'] = status
                    if start_time < start_time_in_hash_map:
                        query_hash_map[hash_id]['start_time'] = start_time
            else:
                query_hash_map[hash_id] = {'start_time':start_time, 'end_time':end_time, 'status':status, 'query':query, 'stageids':'{0}'.format(stageid)}
    else:
        query_hash_map["BeelineQuery_{0}_{1}".format(start_time, stageid)] = {'start_time':start_time, 'end_time':end_time, 'status':status, 'query':query, 'stageids':'{0}'.format(stageid)}


def convert_gmt_time_to_date_obj(gmt_time):
    time_without_gmt = gmt_time.rstrip('GMT').replace('T', ' ')
    formatted_time = datetime.datetime.strptime(time_without_gmt, '%Y-%m-%d %H:%M:%S.%f').strftime("%Y/%m/%d-%H:%M:%S")
    formatted_obj = dt.strptime(formatted_time, "%Y/%m/%d-%H:%M:%S")
    return formatted_obj


def get_json_rest_response_for_stages(rest_url):
    rest_response = subprocess.getoutput(rest_url)
    if "server returned error" in rest_response:
        return ""
    else:
        return json.loads(rest_response)


def get_queryinfo(appid , stageid):
    rest_url = """sudo wget --no-check-certificate -O- -q {0}/api/v1/applications/{1}/stages/{2}""".format(cemod_spark_hosts, appid, stageid)
    json_response = get_json_rest_response_for_stages(rest_url)
    if json_response:
        stage_info = json_response[0]
        try:
            query = stage_info['description']
            start_time = stage_info['submissionTime']
            end_time = stage_info['completionTime']
            status = stage_info['status']
            start_time_obj = convert_gmt_time_to_date_obj(start_time)
            if query and start_time_obj > lasthour_date:
                query_analysis(query,start_time,end_time,status,stageid)
        except KeyError:
            logger.debug("No Query Info for the appid '%s'", appid)


def parse_stages_getquery_info(appids_stageids_dict):
    global query_hash_map
    query_dict = {}
    stageid = ""
    for appid in appids_stageids_dict.keys():
        query_hash_map = {}
        try:
            no_of_stages_list = len(appids_stageids_dict[appid])
            stages_list = list(itertools.chain.from_iterable(appids_stageids_dict[appid]))
            stages_set = set(stages_list)
            for stageid in stages_set:
                get_queryinfo(appid, stageid)
        except (ValueError, TypeError):
            logger.debug("Issue with stage Information '%s':'%s'",appid, stageid)
        query_dict[appid] = query_hash_map
    return query_dict
	
	
def convert_to_date_obj(date_string):
    return dt.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f")	


def write_to_csv(queryinfo_dict):
    spark_file_name = "sparkMonitoring_{0}.csv".format((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    for appid in queryinfo_dict.keys():
        query_temp_dict = {}
        query_temp_dict['Date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            query_temp_dict['Application Id'] = appid
            for hash_id in queryinfo_dict[appid].keys():
                query_temp_dict['Query'] = queryinfo_dict[appid][hash_id]['query']
                query_temp_dict['Start Time'] = queryinfo_dict[appid][hash_id]['start_time']
                query_temp_dict['End Time'] = queryinfo_dict[appid][hash_id]['end_time']
                query_temp_dict['Status'] = queryinfo_dict[appid][hash_id]['status']
                query_temp_dict['Stages'] = queryinfo_dict[appid][hash_id]['stageids']
                end_time = convert_to_date_obj(queryinfo_dict[appid][hash_id]['end_time'].rstrip('GMT').replace('T', ' '))
                start_time = convert_to_date_obj(queryinfo_dict[appid][hash_id]['start_time'].rstrip('GMT').replace('T', ' '))
                query_temp_dict['Duration'] = (end_time - start_time).total_seconds()				
                CsvUtil().writeDictToCsvDictWriter('sparkMonitoring', query_temp_dict,spark_file_name)
        except KeyError:
            logger.debug("No information present to write to Csv")


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
            CsvUtil().writeDictToCsvDictWriter('pySparkMonitoring', query_temp_dict, py_spark_file_name)
        except KeyError:
            logger.debug("No information present to write to Csv")



def parse_pyspark_appids_and_get_info(pyspark_appids):
    query_info = {}
    logger.debug("Pyspark Appids list is '%s' ", pyspark_appids)
    for appid in pyspark_appids:
        query_info_temp_dict = {}        
        rest_url = """sudo wget --no-check-certificate -O- -q {0}/api/v1/applications/{1}""".format(cemod_spark_hosts, appid)
        json_response = get_json_rest_response(rest_url)
        query_info_temp_dict['JobName'] = json_response['name']
        query_info_temp_dict['StartTime'] = json_response['attempts'][0]['startTime']
        query_info_temp_dict['EndTime'] = json_response['attempts'][0]['endTime']
        query_info_temp_dict['Duration']= json_response['attempts'][0]['duration']
        query_info[json_response['id']] = query_info_temp_dict
    return query_info

def push_to_db():
    py_spark_file_name = "pySparkMonitoring_{0}.csv".format((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    outputFilePath = PropertyFileUtil('pySparkMonitoring','DirectorySection').getValueForKey()
    if os.path.exists(outputFilePath+py_spark_file_name):
        jsonFileName=JsonUtils().convertCsvToJson(outputFilePath+py_spark_file_name)
        DBUtil().pushToMariaDB(jsonFileName,"pySparkMonitoring")
    else:
        logger.debug("No information to write for py spark queries")
    spark_file_name = "sparkMonitoring_{0}.csv".format((datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d_%H"))
    outputFilePath = PropertyFileUtil('sparkMonitoring','DirectorySection').getValueForKey()
    if os.path.exists(outputFilePath+spark_file_name):
        jsonFileName=JsonUtils().convertCsvToJson(outputFilePath+spark_file_name)
        new_json_file_name = modify_content(jsonFileName,outputFilePath+spark_file_name)
        DBUtil().pushToMariaDB(new_json_file_name,"sparkMonitoring")
    else:
        logger.debug("No information to write for spark queries")


def modify_content(json_filename, new_file_name):
    modified_content = []
    json_content = ""
    with open(json_filename,'r') as json_file_reader:
        json_content = json.load(json_file_reader)
    for each_query in json_content:
        temp_dict = {}
        for key in each_query.keys():
            if key != 'Query':
                temp_dict[key] = each_query[key]
            else:
                temp_dict[key] = each_query[key].replace('\'',r'\'').replace('\"',r'\'')
        modified_content.append(temp_dict)
        new_file_name = new_file_name.rstrip(".csv")
        new_json_file_name = new_file_name+".json"
    with open(new_json_file_name,'w') as json_file_writer:
        json_file_writer.write(json.dumps(modified_content))
    return new_json_file_name


def main():
    global pyspark_appids,queryinfo_dict, pysparkinfo_dict, query_hash_map
    pyspark_appids = []
    logger.info('Getting Application Id from spark History Server')
    rest_response = get_all_applications()
    appids = get_only_appids(rest_response)
    required_appids = get_lasthour_appids(appids)
    appids_stageids_dict = get_stageids_information(required_appids)
    queryinfo_dict = parse_stages_getquery_info(appids_stageids_dict)
    pysparkinfo_dict = parse_pyspark_appids_and_get_info(pyspark_appids)
    logger.info('Parsed Spark and Pyspark App Ids')
    write_to_csv(queryinfo_dict)
    write_pyspark_info_to_csv(pysparkinfo_dict)
    push_to_db()
    logger.info('Writing csv and db for Spark Completed')


if __name__ == '__main__':
    main()

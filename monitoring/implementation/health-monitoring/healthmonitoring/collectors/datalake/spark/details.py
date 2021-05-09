'''
Created on 19-Apr-2020

@author: deerakum
'''
import subprocess
import datetime
import json
import itertools
from healthmonitoring.collectors.utils.app_variables import AppVariables
from healthmonitoring.collectors import _LocalLogger

logger = _LocalLogger.get_logger(__name__)

spark_hosts = AppVariables.load_app_variables()['cemod_spark_hosts']


def get_json_rest_response(rest_url):
    rest_response = subprocess.getoutput(rest_url)
    return json.loads(rest_response)


def get_all_applications():
    logger.debug('Spark is running on {0} '.format(spark_hosts))
    logger.info('Getting all applications via end point')
    rest_url = """curl -s http://{0}:18089/api/v1/applications""".format(
        spark_hosts)
    rest_response = get_json_rest_response(rest_url)
    logger.debug('Response Fetched from the end point')
    return rest_response


def get_only_appids(json_response_objects):
    applicationids = []
    logger.info('Parsing the response..')
    logger.debug('Total applications present : {0}'.format(
        len(json_response_objects)))
    for json_response_obj in json_response_objects:
        applicationids.append(json_response_obj['id'])
    return applicationids


def get_lasthour_appids(appids):
    lasthour_date = (datetime.datetime.now() -
                     datetime.timedelta(hours=1)).strftime("%Y/%m/%d-%H:%M:%S")
    final_appids = []
    for appid in appids:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}""".format(# noqa: 501
            spark_hosts, appid)  # noqa: 501
        json_objects = get_json_rest_response(rest_url)
        start_time = json_objects['attempts'][0]['startTime'].rstrip(
            'GMT').replace('T', ' ')
        formatted_start_time = datetime.datetime.strptime(
            start_time, '%Y-%m-%d %H:%M:%S.%f').strftime("%Y/%m/%d-%H:%M:%S")
        if formatted_start_time > lasthour_date:
            final_appids.append(appid)
    return final_appids


def get_stageids_information(required_appids):
    appid_stageid_dict = {}
    stage_ids = []
    for required_appid in required_appids:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}/jobs""".format(# noqa: 501
            spark_hosts, required_appid)
        json_objects = get_json_rest_response(rest_url)
        for jobid in json_objects:
            stage_ids.append(jobid['stageIds'])
        stage_ids = []
        appid_stageid_dict[required_appid] = stage_ids
    return appid_stageid_dict


def get_queryinfo(appid, stagelist):
    query_info = {}
    for stageid in stagelist:
        rest_url = """curl -s http://{0}:18089/api/v1/applications/{1}/stages/{2}""".format(# noqa: 501
            spark_hosts, appid, stageid)
        json_response = get_json_rest_response(rest_url)
        stage_info = json_response[0]
        try:
            query_info['query'] = stage_info['description']
            query_info['status'] = stage_info['status']
            query_info['start_time'] = stage_info['submissionTime']
            query_info['end_time'] = stage_info['completionTime']
        except KeyError:
            logger.info("Application Id {0} has different status {1}".format(
                appid, stage_info['status']))
            if stage_info['status'] == 'SKIPPED':
                query_info['query'] = "No Query Found"
                query_info['status'] = stage_info['status']
                query_info['start_time'] = "No Start Time"
                query_info['end_time'] = "No End Time"
        return (stageid, query_info)


def parse_stages_getquery_info(appids_stageids_dict):
    query_dict = {}
    for appid in list(appids_stageids_dict.keys()):
        try:
            stages_list = list(
                itertools.chain.from_iterable(appids_stageids_dict[appid]))
            stageid, query_info = get_queryinfo(appid, stages_list)
            query_dict[appid + '_' + str(stageid)] = query_info
        except (ValueError, TypeError):
            logger.info(
                "Application id {0} has no stage information for the stage {1}"
                .format(appid, stageid))
    logger.info("Final Query Info Dict is {0}".format(query_dict))
    return query_dict


def main():
    rest_response = get_all_applications()
    appids = get_only_appids(rest_response)
    required_appids = get_lasthour_appids(appids)
    appids_stageids_dict = get_stageids_information(required_appids)
    queryinfo_dict = parse_stages_getquery_info(appids_stageids_dict)
    return queryinfo_dict


if __name__ == '__main__':
    main()

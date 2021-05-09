#############################################################################
#                           Monitor
#############################################################################
#############################################################################
# (c)2019 NOKIA
# Author:Monitoring Team
# Version: 0.1
# Purpose:To check the bucket and nodes stats in couchbase
#
# Date:    24-03-2020
#############################################################################
#############################################################################
import os
import sys
import json
import requests

sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')
from dbUtils import DBUtil
from logger_util import *

create_logger()
logger = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
logger.setLevel(get_logging_instance(default_log_level))


def get_couchbase_url_response():
    logger.debug('Getting response')
    url = 'http://%s.%s:%s/cacheserviceagent/api/agent/v1/bucket-info' % (
        os.environ['CEMOD_API_AGGREGATOR_SERVICE_NAME'],
        os.environ['CEMOD_DOMAIN_NAME'],
        os.environ['CEMOD_API_AGGREGATOR_PORT'])
    return json.loads(requests.get(url).text)


def push_bucket_stats_to_db(buckets):
    logger.debug('Pushing buckets stats to db')
    json = str(buckets).replace("'", '"')
    DBUtil().jsonPushToMariaDB(json, "bucketStats")


def push_cpu_stats_to_db(buckets):
    logger.debug('Pushing total ram stats to db')
    cpu_stats = get_total_ram_usage(buckets)
    json = '[%s]' % str(dict(zip(['TotalRamQuota', 'TotalRamUsed', 'RamUsage'],
                                 cpu_stats))).replace("'", '"')
    DBUtil().jsonPushToMariaDB(json, 'bucketRamStats')


def get_total_ram_usage(buckets):
    ram_quota = sum([float(bucket['ramQuota']) for bucket in buckets])
    ram_used = sum([float(bucket['ramUsage']) for bucket in buckets])
    ram_usage = round((ram_used / ram_quota) * 100, 2)
    return [ram_quota, ram_used, ram_usage]


def push_node_stats_to_db(nodes):
    logger.debug('Pushing nodes stats to db')
    json = str(nodes).replace("'", '"')
    DBUtil().jsonPushToMariaDB(json, "nodeStats")


def main():
    buckets, nodes = get_couchbase_url_response()
    logger.info("Rest Response is '%s: %s' ", buckets, nodes)
    push_bucket_stats_to_db(buckets)
    push_cpu_stats_to_db(buckets)
    push_node_stats_to_db(nodes)
    logger.info("Data related to bucket stats, cpu stats and nodes stats are inserted to db")


if __name__ == "__main__":
    main()

import unittest
import os
import json
import requests
import couchbase_stats


class TestCouchbaseStats(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestCouchbaseStats, self).__init__(*args, **kwargs)
        self.response = [
              [
                {
                  "opsPerSec": "0",
                  "quotaPercentUsed": "47.89",
                  "ramQuota": "300.00",
                  "dataUsed": "209.28",
                  "diskFetches": "0",
                  "ramUsage": "143.67",
                  "diskUsed": "261.89",
                  "name": "DATA_AVAILABILITY",
                  "itemCount": "165755"
                }
              ],
              [
                {
                  "hostname": "sslave1",
                  "swap_total": "32768.00",
                  "mem_free": "30602.79",
                  "cpu_utilization_rate": "90.95",
                  "mem_total": "257673.14",
                  "swap_used": "2937.52"
                }
              ]
            ]
        os.environ['CEMOD_API_AGGREGATOR_SERVICE_NAME'] = ''
        os.environ['CEMOD_DOMAIN_NAME'] = ''
        os.environ['CEMOD_API_AGGREGATOR_PORT'] = ''
        requests.get = self.get_response

    def get_response(self, url, params=None, **kwargs):
            response = requests.models.Response()
            response.code = 'Status'
            response.status_code = 200
            response._content = json.dumps(self.response).encode('utf-8')
            return response

    def test_get_total_ram_usage(self):
        cpu_stats = couchbase_stats.get_total_ram_usage(self.response[0])
        self.assertEqual(cpu_stats, [300.0, 143.67, 47.89])

    def test_get_couchbase_url_response(self):
        bucket_stats, node_stats = couchbase_stats.get_couchbase_url_response()
        self.assertEqual(bucket_stats, self.response[0])
        self.assertEqual(node_stats, self.response[1])

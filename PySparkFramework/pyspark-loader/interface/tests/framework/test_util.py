'''
Created on 23-Jul-2020

@author: pratyaks
'''
import unittest
import sys
sys.path.append('framework/')
import framework.util as util  # noqa E402


class Test(unittest.TestCase):
    es_hdfs_path = "hdfs://projectcluster/ngdb/es/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"
    ps_hdfs_path = "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"
    us_hdfs_path = "hdfs://projectcluster/ngdb/us/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"

    YAML_PATH = util.get_resource_path("pyspark_settings.yaml", True)

    def test_get_day_one_of_last_month(self):
        self.assertEqual(1590949800000,
                         util.get_day_one_of_last_month(1595497055000))

    def test_get_job_name(self):
        self.assertEqual('Entity_BB_WS_SEGG_1_DAY',
                         util.get_job_name(self.es_hdfs_path))
        self.assertEqual('Perf_BB_WS_SEGG_1_DAY',
                         util.get_job_name(self.ps_hdfs_path))
        self.assertEqual('Usage_BB_WS_SEGG_1_DAY',
                         util.get_job_name(self.us_hdfs_path))

    def test_get_application_host(self):
        util.project_hive_schema = "host1"
        self.assertEqual(['host1'],
                         util.get_application_host("project_hive_schema"))

    def test_get_resource_path(self):
        self.assertEqual(self.YAML_PATH,
                         util.get_resource_path("pyspark_settings.yaml", True))
        self.YAML_PATH = util.get_resource_path("pyspark_settings.yaml", False)
        self.assertEqual(
            self.YAML_PATH,
            util.get_resource_path("pyspark_settings.yaml", False))

    def test_get_intermediate_table_name(self):
        self.assertEqual('ext_BB_WS_SEGG_1_DAY',
                         util.get_intermediate_table_name(self.es_hdfs_path))

    def test_get_dynamic_dimension_views(self):
        output = {
            'VIEW_EXT_DIM_EXTENSION': 'EXT_BB_WS_SEGG_1_DAY_VIEW',
            'VIEW_ES_DIM_EXTENSION': 'ES_BB_WS_SEGG_1_DAY_VIEW',
            'VIEW_ES_DIM_ID': 'ES_BB_WS_SEGG_1_DAY_VIEW'
        }
        self.assertEqual(output,
                         util.get_dynamic_dimension_views(self.es_hdfs_path))


if __name__ == "__main__":
    unittest.main()

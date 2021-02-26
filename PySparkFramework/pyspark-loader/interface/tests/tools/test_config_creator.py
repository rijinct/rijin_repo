'''
Created on 23 Jul, 2020

@author: amitakum
'''
import argparse
import os
import sys
import unittest

sys.path.extend(['tools/', 'framework'])
import framework.util  # noqa E402
import tools.config_creator  # noqa E402
import tools.config_reader  # noqa E402


class TestConfigCreator(unittest.TestCase):
    def setUp(self):
        os.environ['IS_K8S'] = 'true'
        os.environ[
            tools.config_reader.SPARK_EVENT_LOG] = \
            'hdfs://namenodeHA/user/spark/spark2ApplicationHistory'
        os.environ[
            tools.config_reader.SPARK_HISTORY_SERVER] = \
            'http://ilscha03-cxmd-cdlkc1.uscc.com:18089'  # noqa 501
        os.environ['DB_SCHEMA'] = 'project'

    def test_process_args(self):
        sys.argv = 'config_generator.py process -action conf -f ' \
                   'pyspark_settings.yml ' \
                   '-j Perf_BB_AggregateJob -l ' \
                   'sysy}'.split(" ")
        expected_output = argparse.Namespace(action='conf', cmd='process',
                                             conf_files=None,
                                             job_name=None,
                                             lib_path='/etc/hive/lib',
                                             lower_bound='sysy}',
                                             query_file=None,
                                             source_path=None,
                                             target_path=None,
                                             timezone='Default',
                                             upper_bound=None,
                                             hints_file='pyspark_settings.yml')
        self.assertEqual(expected_output,
                         tools.config_creator.process_args())

    def test_get_hints_global(self):
        yamlpath = framework.util.get_resource_path(
                'pyspark_settings.yml',
                True)
        sys.argv[1:] = 'process -action conf -n ' \
                       'Perf_LTE_SQM_SRV_1_15MIN_AggregateJob -f ' \
                       r''.split(
                ) + [str(yamlpath)]

        self.assertEqual(
                '--jars  --conf spark.dynamicAllocation.enabled=true '
                '--conf spark.shuffle.service.enabled=true --conf '
                'spark.sql.crossJoin.enabled=true --conf '
                'spark.sql.execution.arrow.enabled=true --conf '
                'spark.sql.parquet.compression.codec=snappy --conf '
                'spark.ui.port=4041 --conf '
                'spark.sql.broadcastTimeout=14400 --conf '
                'spark.executorEnv.FILE_READER=com.nsn.ngdb.common.hive'
                '.io.reader.ParquetFileReader --conf '
                'spark.yarn.appMasterEnv.STATIC_PARTITION_SIZE=268435456 '
                '--conf '
                'spark.yarn.appMasterEnv.DYNAMIC_RECORD_COUNT=3000000 '
                '--conf '
                'spark.yarn.queue=root.rijin.ca4ci.pyspark-15minutes '
                '--conf spark.files.maxPartitionBytes=67108864 --conf '
                'spark.sql.shuffle.partitions=45 --conf '
                'spark.eventLog.enabled=true --conf '
                'spark.eventLog.dir=hdfs://namenodeHA/user/spark'
                '/spark2ApplicationHistory --conf '
                'spark.yarn.historyServer.address=http://ilscha03-cxmd'
                '-cdlkc1.uscc.com:18089 --conf '
                'spark.yarn.appMasterEnv.HIVESCHEMA=project ',
                # noqa 501
                tools.config_creator.get_hints(
                        tools.config_creator.process_args(),
                        "Perf_LTE_SQM_SRV_1_15MIN_AggregateJob"))

    def test_get_hints_local(self):
        yamlpath = framework.util.get_resource_path(
                'pyspark_settings.yml',
                True)
        sys.argv[1:] = 'process -action conf -n ' \
                       'Perf_BB_STR_SEGG_1_1_15MIN_AggregateJob ' \
                       '-f '.split() + [str(yamlpath)]
        self.assertEqual(
                '--jars  --conf spark.dynamicAllocation.enabled=true '
                '--conf spark.shuffle.service.enabled=true --conf '
                'spark.sql.crossJoin.enabled=true --conf '
                'spark.sql.execution.arrow.enabled=true --conf '
                'spark.sql.parquet.compression.codec=snappy --conf '
                'spark.ui.port=4041 --conf '
                'spark.sql.broadcastTimeout=14400 --conf '
                'spark.executorEnv.FILE_READER=com.nsn.ngdb.common.hive'
                '.io.reader.ParquetFileReader --conf '
                'spark.yarn.appMasterEnv.STATIC_PARTITION_SIZE=268435456 '
                '--conf '
                'spark.yarn.appMasterEnv.DYNAMIC_RECORD_COUNT=3000000 '
                '--conf '
                'spark.yarn.queue=root.rijin.ca4ci.pyspark-15minutes '
                '--conf spark.files.maxPartitionBytes=67108864 --conf '
                'spark.sql.shuffle.partitions=45 --conf '
                'spark.executor.memory=2g --conf spark.driver.memory=1g '
                '--conf '
                'spark.eventLog.enabled=true --conf '
                'spark.eventLog.dir=hdfs://namenodeHA/user/spark'
                '/spark2ApplicationHistory --conf '
                'spark.yarn.historyServer.address=http://ilscha03-cxmd'
                '-cdlkc1.uscc.com:18089 --conf '
                'spark.yarn.appMasterEnv.HIVESCHEMA=project ',
                # noqa 501
                tools.config_creator.get_hints(
                        tools.config_creator.process_args(),
                        "Perf_BB_STR_SEGG_1_1_15MIN_AggregateJob"))

    def test_get_hints_custom_global(self):
        yamlpath = framework.util.get_resource_path(
                'custom_pyspark_settings.yml', True)
        sys.argv[1:] = 'process -action conf -n ' \
                       'Perf_BB_WS_ALL_1_HOUR_AggregateJob ' \
                       '-f '.split() + [str(yamlpath)]
        self.assertEqual(
                '--jars  --conf spark.dynamicAllocation.enabled=true '
                '--conf spark.shuffle.service.enabled=true --conf '
                'spark.sql.crossJoin.enabled=true --conf '
                'spark.sql.execution.arrow.enabled=true --conf '
                'spark.sql.parquet.compression.codec=snappy --conf '
                'spark.ui.port=4041 --conf '
                'spark.sql.broadcastTimeout=14400 --conf '
                'spark.eventLog.enabled=true --conf '
                'spark.eventLog.dir=hdfs://namenodeHA/user/spark'
                '/spark2ApplicationHistory --conf '
                'spark.yarn.historyServer.address=http://ilscha03-cxmd'
                '-cdlkc1.uscc.com:18089 --conf '
                'spark.yarn.appMasterEnv.HIVESCHEMA=project ',
                # noqa 501
                tools.config_creator.get_hints(
                        tools.config_creator.process_args(),
                        "Perf_BB_WS_ALL_1_HOUR_AggregateJob"))

    def test_get_hints_custom_local(self):
        yamlpath = framework.util.get_resource_path(
                'custom_pyspark_settings.yml', True)
        sys.argv[1:] = 'process -action conf -n ' \
                       'Perf_BB_STR_SEGG_1_1_15MIN_AggregateJob -f ' \
                       ''.split() + [
                           str(yamlpath)]
        self.assertEqual(
                '--jars  --conf spark.dynamicAllocation.enabled=true '
                '--conf spark.shuffle.service.enabled=true --conf '
                'spark.sql.crossJoin.enabled=true --conf '
                'spark.sql.execution.arrow.enabled=true --conf '
                'spark.sql.parquet.compression.codec=snappy --conf '
                'spark.ui.port=4041 --conf '
                'spark.sql.broadcastTimeout=14400 --conf '
                'spark.files.maxPartitionBytes=67108864 --conf '
                'spark.sql.shuffle.partitions=45 --conf '
                'spark.executor.memory=2g --conf '
                'spark.eventLog.enabled=true '
                '--conf spark.eventLog.dir=hdfs://namenodeHA/user/spark'
                '/spark2ApplicationHistory --conf '
                'spark.yarn.historyServer.address=http://ilscha03-cxmd'
                '-cdlkc1.uscc.com:18089 --conf '
                'spark.yarn.appMasterEnv.HIVESCHEMA=project ',
                # noqa 501
                tools.config_creator.get_hints(
                        tools.config_creator.process_args(),
                        "Perf_BB_STR_SEGG_1_1_15MIN_AggregateJob"))

    def test_get_job_name_if_us(self):
        target_path = "/ngdb/us/LTE_1/LTE_4G_1"
        self.assertEqual("Usage_LTE_4G_1_LoadJob",
                         tools.config_creator.get_job_name(target_path))

    def test_get_job_name_if_es(self):
        target_path = "HDFS://projectcluster/ngdb/es/SMS_1/SMS_TYPE_1"
        self.assertEqual("Entity_SMS_TYPE_1_CorrelationJob",
                         tools.config_creator.get_job_name(target_path))

    def test_get_job_name_default(self):
        target_path = "/ngdb/ps/VOLTE_1/VOLTE_SEGG_1_DAY"
        self.assertEqual("Perf_VOLTE_SEGG_1_DAY_AggregateJob",
                         tools.config_creator.get_job_name(target_path))

    def test_get_yaml_files(self):
        conf_files = "pyspark_setting.yml"
        extension = "dimension_extension.yml"
        self.assertEqual(
                'pyspark_setting.yml,dimension_extension.yml',
                tools.config_creator.get_yaml_files(conf_files, extension))

    def test_merger(self):
        tools.config_creator.DIM_EXTN_PATH = str(
                framework.util.get_resource_path("", True)) + "\\"
        self.assertEqual("merged_dimension_extension.yaml",
                         tools.config_creator.merger().split("\\")[-1])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

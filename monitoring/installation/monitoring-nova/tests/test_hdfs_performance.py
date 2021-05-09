import os
import unittest
import hdfs_performance
import dbConnectionManager


class TestHdfsPerformace(unittest.TestCase):

    class HdfsPerfCheckManger:

        def __init__(self, gateway_obj):
            pass

        def getPerfStats(self, backupFile, localPath, hdfsPath):
            return '2020-03-22 12:00:00,0,2'

    def get_gateway(self):
        pass

    def execute_finally(self, gateway_obj):
        pass

    def set_up_dependencies(self):
        os.environ['LOCAL_TZ'] = 'Asia/Kolkata'
        dbConnectionManager.DbConnection.initialiseGateway = self.get_gateway
        hdfs_performance.get_perf_manager_object = self.HdfsPerfCheckManger
        hdfs_performance.execute_finally = self.execute_finally

    def test_get_hdfs_performace_stats(self):
        self.set_up_dependencies()
        stats = hdfs_performance.get_hdfs_perfromance_stats()
        self.assertEqual(stats, {
            'Date': '2020-03-22 17:30:00',
            'hdfsreadtime': '0',
            'hdfswritetime': '2'
        })

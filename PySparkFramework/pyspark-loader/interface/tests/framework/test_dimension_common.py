'''
Created on 13-Aug-2020

@author: pratyaks
'''
import unittest
import argparse
import sys
sys.path.append('framework/')

import framework.dimension_common as dimension_common  # noqa E402
import specification  # noqa E402


class Test(unittest.TestCase):
    source = '/hdfs://projectcluster/ngdb/us/RADIO_1/RADIO_1/'
    target = '/hdfs://projectcluster/ngdb/ps/RADIO_1/RADIO_SEGG_1_HOUR/'
    expected_output = argparse.Namespace(source_paths=source,
                                         target_path=target)

    def test_process_args(self):
        sys.argv = 'dynamic_dimension_loader.py -s/hdfs://projectcluster/ngdb/us/RADIO_1/RADIO_1/ -t/hdfs://projectcluster/ngdb/ps/RADIO_1/RADIO_SEGG_1_HOUR/'.split(" ")  # noqa: 501
        self.assertEqual(self.expected_output, dimension_common.process_args())

    def test_get_sources_and_target(self):
        self.assertIsInstance(
            dimension_common.get_sources_and_target(
                self.expected_output)[0][0], specification.SourceSpecification)
        self.assertIsInstance(
            dimension_common.get_sources_and_target(self.expected_output)[1],
            specification.TargetSpecification)  # noqa: 501

    def test_get_hdfs_corrupt_filename(self):
        self.assertEqual(
            '/tmp/duplicate/RADIO_SEGG_1_HOUR/',
            dimension_common.get_hdfs_corrupt_filename(self.target))


if __name__ == "__main__":
    unittest.main()

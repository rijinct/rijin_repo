'''
Created on 24-Jul-2020

@author: nageb
'''
import unittest

from healthmonitoring.collectors.kubernetes.kubectl import _Util
from mock import patch, mock_open

from healthmonitoring.collectors.kubernetes import kubectl


class TestKubectl(unittest.TestCase):

    ENDPOINTS_TEST_DATA = '''NAME                                                     ENDPOINTS                                                                  AGE
ae-etltopology-av-streaming-sink                         <none>                                                                     58d
dis-nci-glusterfs                                        192.168.114.147:8083,192.168.114.147:8080                                  5d21h
ae-etltopology-ice-radio-enb-sink                        <none>                                                                     70d'''  # noqa: E501

    JOBS_WITHOUT_UNIVERSE = '''NAME                                                       COMPLETIONS   DURATION   AGE
ae-etltopology-gnup-sink-post-install                      1/1           27m        9d
ae-etltopology-s1mme-sink-post-install                     1/1           24m        81d
ae-etltopology-voice-sink-post-install                     1/1           4h30m      68d
'''  # noqa: E501

    JOBS_WITH_FAILED_UNIVERSE = '''NAME                                                       COMPLETIONS   DURATION   AGE
accuracy-universe-20.6.973-8-copy                          0/1           4s         48d
accuracy-usecases-20.5.946-8-install                       1/1           9s         67d
ae-etltopology-gnup-sink-post-install                      1/1           27m        9d
ae-etltopology-s1mme-sink-post-install                     1/1           24m        81d
ae-etltopology-voice-sink-post-install                     1/1           4h30m      68d
'''  # noqa: E501

    PODS_WITH_PROPER_CONTAINERS = '''NAME                                                           READY   STATUS              RESTARTS   AGE
belk-efkc-belk-curator-1595379600-lqwx2                        0/1     Completed           0          2d7h
belk-efkc-belk-curator-1595466000-l5jbq                        0/1     Completed           0          31h
belk-efkc-belk-curator-1595552400-l996t                        0/1     Completed           0          7h6m
belk-efkc-belk-elasticsearch-client-96644cb8b-964tk            1/1     Running             0          6d19h
belk-efkc-belk-elasticsearch-client-96644cb8b-g7s6p            1/1     Running             0          6d19h
'''  # noqa: E501

    PODS_WITH_FAILED_CONTAINERS = '''NAME                                                           READY   STATUS              RESTARTS   AGE
belk-efkc-belk-curator-1595379600-lqwx2                        0/1     Completed           0          2d7h
belk-efkc-belk-curator-1595466000-l5jbq                        0/1     Completed           0          31h
belk-efkc-belk-curator-1595552400-l996t                        0/1     Completed           0          7h6m
belk-efkc-belk-elasticsearch-client-96644cb8b-964tk            0/1     ImagePullBackOff    0          6d19h
belk-efkc-belk-elasticsearch-client-96644cb8b-g7s6p            1/1     Running             0          6d19h
'''  # noqa: E501

    def setUp(self):
        _Util.under_test = True

    def test_when_passed_no_arguments_should_fail(self):
        with self.assertRaises(ValueError):
            kubectl._execute([])

    def test_when_passed_multiple_arguments_should_fail(self):
        with self.assertRaises(ValueError):
            kubectl._execute(["a", "b"])

    def test_when_passed_invalid_argument_should_fail(self):
        with self.assertRaises(NameError):
            kubectl._execute(["a"])

    @patch('builtins.open', mock_open(read_data=ENDPOINTS_TEST_DATA))
    def test_get_glusterfs_status_when_up(self):
        actual = kubectl._execute(["get_glusterfs_status"])
        self.assertEqual(actual, "1")

    def test_get_glusterfs_status_when_down(self):
        actual = kubectl._execute(["get_glusterfs_status"])
        self.assertEqual(actual, "0")

    def test_get_universes_status_when_up(self):
        actual = kubectl._execute(["get_universes_status"])
        self.assertEqual(actual, "1")

    @patch('builtins.open', mock_open(read_data=JOBS_WITHOUT_UNIVERSE))
    def test_get_universes_status_when_universe_pod_not_present(self):
        actual = kubectl._execute(["get_universes_status"])
        self.assertEqual(actual, "0")

    @patch('builtins.open', mock_open(read_data=JOBS_WITH_FAILED_UNIVERSE))
    def test_get_universes_status_when_universe_pod_not_ready(self):
        actual = kubectl._execute(["get_universes_status"])
        self.assertEqual(actual, "0")

    @patch('builtins.open', mock_open(read_data=PODS_WITH_PROPER_CONTAINERS))
    def test_get_paas_status_when_up(self):
        actual = kubectl._execute(["get_paas_status"])
        self.assertEqual(actual, "1")

    def test_get_paas_status_when_down(self):
        actual = kubectl._execute(["get_paas_status"])
        self.assertEqual(actual, "0")

    def test_get_ncms_status_when_up(self):
        actual = kubectl._execute(["get_ncms_status"])
        self.assertEqual(actual, "1")

    @patch('builtins.open', mock_open(read_data=PODS_WITH_FAILED_CONTAINERS))
    def test_get_ncms_status_when_down(self):
        actual = kubectl._execute(["get_ncms_status"])
        self.assertEqual(actual, "0")

    def test_get_rook_status_when_up(self):
        actual = kubectl._execute(["get_rook_status"])
        self.assertEqual(actual, "1")

    @patch('builtins.open', mock_open(read_data=PODS_WITH_FAILED_CONTAINERS))
    def test_get_rook_status_when_down(self):
        actual = kubectl._execute(["get_rook_status"])
        self.assertEqual(actual, "0")

    def test_get_csfp_af_status_when_up(self):
        actual = kubectl._execute(["get_csfp_af_status"])
        self.assertEqual(actual, "1")

    @patch('builtins.open', mock_open(read_data=PODS_WITH_FAILED_CONTAINERS))
    def test_get_csfp_af_status_when_down(self):
        actual = kubectl._execute(["get_csfp_af_status"])
        self.assertEqual(actual, "0")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

import unittest
from unittest.mock import patch, MagicMock
from lxml import html
from dbUtils import DBUtil
from healthmonitoring.collectors.edgenode.rpms_info import _execute
from healthmonitoring.collectors.edgenode.rpms_info import Collector


class TestRpmsInfo(unittest.TestCase):
    def _get_html_object(self):
        with open("version.txt") as versions_reader:
            version_info = versions_reader.read()
        return html.fromstring(version_info)

    @patch.object(DBUtil, 'jsonPushToMariaDB',
                  MagicMock(return_value='success'))
    @patch.object(Collector, 'get_html_object', new=_get_html_object)
    def test_rpms(self):
        expected = ([{
            'Type': 'Adaptation',
            'Value': 'NSN-NGDB-SMS-ADAPTATION-21.02.4271-11_1'
        }], [{
            'Type': 'Udf',
            'Value': 'custom-hive-udf-CD-21.02.170-0.jar'
        }], [{
            'Type': 'PlatformApps',
            'Value': 'NSN-NGDB-SDK-CORE-CD-21.02.169-0_1'
        }], [{
            'Type': 'Cemod_version',
            'Value': 'CI21'
        }, {
            'Type': 'OS_version',
            'Value': 'Linux 3.10.0-1160.11.1.el7.x86_64'
        }, {
            'Type': 'Kerberos',
            'Value': 'Enabled'
        }, {
            'Type':
            'ContentApps',
            'Value':
            'CQI,FIXEDLINE,CEI2,OTT,FLEXIREPORT-CQI,TNP,'
            'OBR,PROFILE_INDEX,FL_Operational_Dashboard'
        }])
        actual = _execute()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()

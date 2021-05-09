import copy
import os
import unittest

from healthmonitoring.framework.util import file_util
from tools.populate_hosts import _get_hosts
from tools.populate_hosts import write_host_spec
import tools.populate_hosts as populate_hosts


def source_application_definition(host_dict):
    populate_hosts.cemod_hdfs_master_hosts = \
        'hactive.invinci.cem.com hpassive.invinci.cem.com'
    for key, value in host_dict.items():
        setattr(populate_hosts, key, value)


def create_temp_app_def_file(host_dict, path):
    lines = ""
    for key, value in host_dict.items():
        if len(value.split(" ")) > 1:
            lines += "{0}=({1})\n".format(key, value)
        else:
            lines += "{0}=\"{1}\"\n".format(key, value)
    with open(path, "w") as f:
        f.write(lines)


def getoutput(cmd):
    platform_lookup = {
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_hdfs_master[@]}'":
        "hactive.invinci.cem.com hpassive.invinci.cem.com",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_spark_master[@]}'":
        "hactive.invinci.cem.com",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_zookeep_install_hosts[@]}'":
        "hactive.invinci.cem.com hpassive.invinci.cem.com "
        "hslave1.invinci.cem.com",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_hive_dbname[@]}'":
        "sai",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_hive_metastore_db_user[@]}'":
        "postgres",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_postgres_active_fip_host[@]}'":
        "postgressdk-fip.nokia.com",
        "ssh hactive 'source /opt/nsn/ngdb/ifw/utils/platform_"
        "definition.sh; echo ${cemod_hive_metastore_db_port[@]}'":
        "5432"
    }
    return platform_lookup[cmd.strip()]


class TestPopulateHosts(unittest.TestCase):
    app_host_name = 'cemod_hdfs_master_hosts'
    HOST_DICTIONARY = {
        'cemod_couchbase_hosts':
        'sactive.nokia.com spassive.nokia.com analytics.nokia.com',
        'cemod_scheduler_active_fip_host':
        'scheduler-fip.nokia.com',
        'cemod_dalserver_active_fip_host':
        'dalserver-fip.nokia.com',
        'cemod_smtp_ip':
        '127.0.0.1',
        'cemod_smtp_host':
        'nsn.mail.com',
        'cemod_hdfs_master':
        'hactive.invinci.cem.com hpassive.invinci.cem.com',
        'cemod_spark_master':
        'hactive.invinci.cem.com',
        'cemod_zookeep_install_hosts':
        'hactive.invinci.cem.com hpassive.invinci.cem.com '
        'hslave1.invinci.cem.com',
        'cemod_smtp_sender_emailids':
        'root@localhost',
        'cemod_smtp_receiver_emailids':
        'root@localhost ngdb@localhost',
        'cemod_snmp_ip':
        '127.0.0.1',
        'cemod_snmp_port':
        '162',
        'cemod_snmp_community':
        '2011#Sai#2025',
        'cemod_sdk_db_name':
        'sai',
        'cemod_application_sdk_database_linux_user':
        'postgres',
        'cemod_postgres_sdk_fip_active_host':
        'postgressdk-fip.nokia.com',
        'cemod_application_sdk_db_port':
        '5432',
        'mariadb_db_name':
        'test',
        'mariadb_user':
        'mariadb',
        'mariadb_password':
        'test123',
        'mariadb_active_host':
        'mariadb.test.nokia.com',
        'mariadb_connect_timeout':
        '86400'
    }
    expected = {
        'hostgroups': {
            'application': {
                'couchbase': {
                    'ha_status':
                    False,
                    'name':
                    'cemod_couchbase_hosts',
                    'hosts': [
                        'sactive.nokia.com', 'spassive.nokia.com',
                        'analytics.nokia.com'
                    ],
                },
                'scheduler': {
                    'ha_status': True,
                    'name': 'cemod_scheduler_active_fip_host',
                    'hosts': ['scheduler-fip.nokia.com']
                },
                'dalserver': {
                    'name': 'cemod_dalserver_active_fip_host',
                    'hosts': ['dalserver-fip.nokia.com']
                },
                'email': {
                    'cemod_smtp_sender_emailids':
                    'root@localhost',
                    'cemod_smtp_ip':
                    '127.0.0.1',
                    'cemod_smtp_host':
                    'nsn.mail.com',
                    'cemod_smtp_receiver_emailids':
                    ['root@localhost', 'ngdb@localhost'],
                    'fields': [
                        'cemod_smtp_sender_emailids', 'cemod_smtp_ip',
                        'cemod_smtp_host', 'cemod_smtp_receiver_emailids'
                    ],
                },
                'snmp': {
                    'cemod_snmp_community':
                    '2011#Sai#2025',
                    'cemod_snmp_ip':
                    '127.0.0.1',
                    'cemod_snmp_port':
                    '162',
                    'fields': [
                        'cemod_snmp_ip', 'cemod_snmp_port',
                        'cemod_snmp_community'
                    ],
                    'list_fields': []
                }
            },
            'platform': {
                'hdfs': {
                    'name':
                    'cemod_hdfs_master',
                    'hosts':
                    ['hactive.invinci.cem.com', 'hpassive.invinci.cem.com']
                },
                'spark': {
                    'name': 'cemod_spark_master',
                    'hosts': ['hactive.invinci.cem.com']
                },
                'zookeeper': {
                    'name':
                    'cemod_zookeep_install_hosts',
                    'hosts': [
                        'hactive.invinci.cem.com', 'hpassive.invinci.cem.com',
                        'hslave1.invinci.cem.com'
                    ]
                },
                'postgres_hive': {
                    'fields': [
                        'cemod_hive_dbname',
                        'cemod_hive_metastore_db_user',
                        'cemod_postgres_active_fip_host',
                        'cemod_hive_metastore_db_port'
                    ],
                    'list_fields': [],
                    'cemod_hive_dbname': 'sai',
                    'cemod_hive_metastore_db_user':
                        'postgres',
                    'cemod_postgres_active_fip_host':
                        'postgressdk-fip.nokia.com',
                    'cemod_hive_metastore_db_port':
                        '5432'
                }
            }
        }
    }

    def setUp(self):
        file_util.under_test = True
        populate_hosts.template_path = file_util.get_resource_path(
            "hosts.yaml")
        self._config = populate_hosts.read_host_spec()
        source_application_definition(TestPopulateHosts.HOST_DICTIONARY)

    def test_source_application_definition(self):
        app_def_path = file_util.get_resource_path("application_definition.sh")
        create_temp_app_def_file(TestPopulateHosts.HOST_DICTIONARY,
                                 app_def_path)
        actual = "sactive.nokia.com spassive.nokia.com analytics.nokia.com"
        populate_hosts.source_application_definition(app_def_path)
        self.assertEqual(actual, populate_hosts.cemod_couchbase_hosts)
        os.remove(app_def_path)

    def test_get_application_host(self):
        expected = ['hactive.invinci.cem.com', 'hpassive.invinci.cem.com']
        actual = _get_hosts(populate_hosts.APP_SUB_GROUP, self.app_host_name)
        self.assertEqual(actual, expected)

    def test_fill_application_hosts(self):
        actual = copy.deepcopy(self._config)
        populate_hosts.fill_hosts(populate_hosts.APP_SUB_GROUP, actual)
        expected_values = set(
            TestPopulateHosts.expected['hostgroups']['application'])
        actual_values = set(actual['hostgroups']['application'])
        self.assertTrue(expected_values.issubset(actual_values))

    def test_fill_platform_hosts(self):
        actual = copy.deepcopy(self._config)
        populate_hosts.cemod_hive_hosts = "hactive"
        populate_hosts.subprocess.getoutput = getoutput
        populate_hosts.fill_hosts(populate_hosts.PLATFORM_SUB_GROUP, actual)
        expected_values = set(
            TestPopulateHosts.expected['hostgroups']['platform'])
        actual_values = set(actual['hostgroups']['platform'])
        self.assertTrue(expected_values.issubset(actual_values))

    def test_host_spec_file(self):
        populate_hosts.spec_path = file_util.get_resource_path("test.yaml")
        spec = populate_hosts.read_host_spec()
        write_host_spec(spec)
        populate_hosts.template_path = populate_hosts.spec_path
        test_spec = populate_hosts.read_host_spec()
        self.assertEqual(spec, test_spec)
        os.remove(populate_hosts.spec_path)

    def test_invalid_yaml_file(self):
        invalid_yaml = file_util.get_resource_path(".")
        populate_hosts.template_path = invalid_yaml
        populate_hosts.spec_path = invalid_yaml
        with self.assertRaises(SystemExit) as r:
            populate_hosts.read_host_spec()
        with self.assertRaises(SystemExit) as w:
            populate_hosts.write_host_spec("!invalid-yaml")
        error_codes = [r.exception.code, w.exception.code]
        for error_code in error_codes:
            self.assertEqual(error_code, 1)


if __name__ == "__main__":
    unittest.main()

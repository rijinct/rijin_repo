import os
import sys
import unittest

from yaml import dump

sys.path.append('framework/')
sys.path.append('tools/')
from framework.specification import Specification  # noqa E402
from framework.util import get_resource_path  # noqa E402
from framework.specification import SourceSpecification  # noqa E402
from framework.specification import TargetSpecification  # noqa E402
from tools.tools_util import read_yaml  # noqa E402


def _write_yaml_spec(path, spec):
    with open(path, 'w') as f:
        dump(spec, f)


def _get_target_spec():
    _write_yaml_spec("yaml_spec.yml", TestSpecification.SPEC)
    return TargetSpecification(
            "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY",
            "project",
            lower_bound=1594575000000,
            upper_bound=1594575900000,
            timezone='Default',
            days=0,
            yaml_files=[get_resource_path("yaml_spec.yml", True)])


def _get_source_spec():
    return SourceSpecification(
            "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY",
            "project",
            lower_bound=1594575000000,
            upper_bound=1594575900000,
            timezone='Default',
            days=0,
            extensions=read_yaml(
                    get_resource_path("dimension_extension.yaml", True)))


class TestSpecification(unittest.TestCase):
    HDFS_PATH = "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1" \
                "/BB_WS_SEGG_1_DAY"
    SPEC = {
        "sql": {
            "query": "select * from tab"
            },
        "subpartition_columns": ["region", "technology"]
        }
    YAML_PATH = get_resource_path("test.yaml", True)

    def setUp(self):
        self.agg = TargetSpecification(TestSpecification.HDFS_PATH,
                                       "project",
                                       1594575000000, 1594575900000,
                                       "Default")
        self.agg_with_days = Specification(TestSpecification.HDFS_PATH,
                                           "project", 1594575900000,
                                           1594575900000, "Default", 1)

    def test_property(self):
        self.assertEqual(self.agg.hdfs_path, TestSpecification.HDFS_PATH)
        self.assertEqual(self.agg.lower_bound, 1594575000000)
        self.assertEqual(self.agg.upper_bound, 1594575900000)
        self.assertEqual(self.agg.hive_schema, "project")
        self.assertEqual(self.agg.spec_type, "ps")
        self.assertEqual(self.agg.timezone, "Default")
        self.assertEqual(self.agg.days, 0)
        self.agg.hdfs_path = "hdfs://cluster:/ngdb/other/path"
        self.assertEqual(self.agg.hdfs_path,
                         "hdfs://cluster:/ngdb/other/path")

    def test_property_with_lb_calculation(self):
        self.assertEqual(self.agg_with_days.hdfs_path,
                         TestSpecification.HDFS_PATH)
        self.assertEqual(self.agg_with_days.lower_bound, 1594489500000)
        self.assertEqual(self.agg_with_days.upper_bound, 1594575900000)
        self.assertEqual(self.agg_with_days.hive_schema, "project")
        self.assertEqual(self.agg_with_days.spec_type, "ps")
        self.assertEqual(self.agg_with_days.timezone, "Default")
        self.assertEqual(self.agg_with_days.days, 1)

    def test_set_yaml_spec(self):
        _write_yaml_spec(TestSpecification.YAML_PATH,
                         TestSpecification.SPEC)
        spec = Specification.set_yaml_spec(TestSpecification.YAML_PATH)
        self.assertEqual(spec, TestSpecification.SPEC)
        os.remove(TestSpecification.YAML_PATH)

    def test_get_table_name(self):
        self.assertEqual(self.agg.get_table_name(),
                         "project.ps_bb_ws_segg_1_day")


class TestSourceSpecification(unittest.TestCase):
    hdfs_path = "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1" \
                "/BB_WS_SEGG_1_DAY"
    _hive_schema = "project"
    spec_type = "es"
    timezone = "Default"
    int_dt = "1594575000000"

    es_out = 'hdfs://projectcluster/ngdb/es/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY' \
             '/dt=1594575000000/tz=Default/'  # noqa: 501
    latest_out = [
        'hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY'
        '/file_load_info=latest/'
        # noqa: 501
        ]
    flag = 0

    @staticmethod
    def _get_all_dts(self):
        if TestSourceSpecification.flag == 1:
            return b'latest'
        else:
            return [1594575000000, 1594575300000, 1594580000000]

    @staticmethod
    def is_empty_path(dir_path):
        return False

    def setUp(self):
        self._target_spec = _get_target_spec()
        self._source_spec = _get_source_spec()
        SourceSpecification._get_all_dts = \
            TestSourceSpecification._get_all_dts
        SourceSpecification.is_empty_path = \
            TestSourceSpecification.is_empty_path

    def test_is_empty_path(self):
        self.assertEqual(False, SourceSpecification.is_empty_path('abc'))

    def test_get_all_dts(self):
        self.assertEqual([1594575000000, 1594575300000, 1594580000000],
                         self._source_spec._get_all_dts())

    def test_extensions(self):
        extension_conf = get_resource_path("dimension_extension.yaml",
                                           True)
        extensions = Specification.set_yaml_spec(extension_conf)
        self.assertEqual(extensions, self._source_spec.extensions)

    def test_get_dimension_tables(self):
        self.assertEqual(['es_subs_extension_1'],
                         self._source_spec.get_dimension_tables(
                                 "ps_av_streaming_segg_1_hour"))

    def test_represents_int(self):
        self.assertEqual(True, SourceSpecification.represents_int(10))
        self.assertEqual(False, SourceSpecification.represents_int("abc"))

    def test_get_ps_partition_paths(self):
        static_path = [
            'hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY'
            '/dt=1594575000000/tz=Default/',
            # noqa: 501
            'hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY'
            '/dt=1594575300000/tz=Default/'
            # noqa: 501
            ]
        self.assertEqual(static_path,
                         self._source_spec._get_ps_partition_paths())

    def test_get_locs(self):
        PS_OUT = [
            "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"
            "/dt=1594575000000/tz=Default/",
            # noqa: 501
            "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1/BB_WS_SEGG_1_DAY"
            "/dt=1594575300000/tz=Default/"
            # noqa: 501
            ]
        source_spec = _get_source_spec()
        self.assertEqual(PS_OUT, source_spec.get_locs(''))


class TestTargetSpecification(unittest.TestCase):
    HDFS_PATH = "hdfs://projectcluster/ngdb/ps/FL_BB_USAGE_1" \
                "/BB_WS_SEGG_1_DAY"
    HIVE_SCHEMA = "project"
    YAML_PATH = [get_resource_path("dimension_extension.yaml", True)]
    YAML_SPEC = [Specification.set_yaml_spec(file) for file in YAML_PATH]
    SPEC = {
        "sql": {
            "query": "select * from tab"
            },
        'partition': {
            'columns': ['dt', 'tz', 'technology', 'region'],
            'columns_with_values': ['dt=1598121000000', 'tz=default',
                                    'technology', 'region']
            }
        }

    def test_yaml_spec(self):
        a = TargetSpecification(self.HDFS_PATH, self.HIVE_SCHEMA, 10, 100,
                                'UTC', 10, self.YAML_PATH)
        self.assertEqual(self.YAML_SPEC,
                         TargetSpecification.yaml_spec.__get__(a))

    def test_get_sub_partition_col(self):
        self.assertEqual(['technology', 'region'],
                         TargetSpecification.get_dynamic_partition_col(
                                 self.SPEC))


if __name__ == "__main__":
    unittest.main()

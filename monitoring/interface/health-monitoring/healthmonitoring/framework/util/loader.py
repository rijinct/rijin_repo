import os
from pathlib import Path
from subprocess import run

from healthmonitoring.framework import config
from healthmonitoring.framework.specification.yaml import \
    YamlSpecificationLoader


class SpecificationLoader:

    _config_map_time_stamp = 0

    @staticmethod
    def load_if_required():
        PATH = SpecificationLoader._get_config_dir_path()
        if not config.hosts_spec:
            SpecificationLoader._load(PATH)
        elif SpecificationLoader._is_config_map_time_stamp_changed():
            SpecificationLoader._repopulate_hosts(PATH)
            SpecificationLoader._load(PATH)

    @staticmethod
    def _load(PATH):
        config.properties.read(
            (Path(PATH) / 'monitoring.properties').resolve())
        YamlSpecificationLoader().load_hosts_specification(PATH)
        YamlSpecificationLoader().load_monitoring_specification(PATH)

    @staticmethod
    def _get_config_dir_path():
        return os.environ.get('CONFIG_DIR_PATH',
                              "/opt/nsn/ngdb/monitoring/resources/")

    @staticmethod
    def _is_config_map_time_stamp_changed():
        return SpecificationLoader._config_map_time_stamp < \
            SpecificationLoader._get_config_map_time_stamp()

    @staticmethod
    def _get_config_map_time_stamp():
        config_map_path = Path(
            "/opt/nsn/ngdb/monitoring/extEndPointsConfigMap/..data")
        return config_map_path.stat().st_mtime

    @staticmethod
    def _repopulate_hosts(PATH):
        SpecificationLoader._config_map_time_stamp = \
            SpecificationLoader._get_config_map_time_stamp()
        arg_1 = str(SpecificationLoader._get_tools_path() /
                    "populate_hosts_k8s.py")
        arg_2 = arg_3 = str(Path(PATH) / "hosts.yaml")
        run(["python", arg_1, arg_2, arg_3])

    @staticmethod
    def _get_tools_path():
        return Path(
            SpecificationLoader._get_config_dir_path()).parent / "tools"

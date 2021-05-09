import os
import sys

from healthmonitoring.framework import runner

from tests.healthmonitoring.collectors.utils import get_resource_path
sys.path.insert(0, '/opt/nsn/ngdb/monitoring/utils')


def init():
    sys.modules['logger'] = __import__('mock_logger')
    os.environ['CONFIG_DIR_PATH'] = str(get_resource_path("."))
    print(str(get_resource_path(".")))
    runner.load_config(get_resource_path("monitoring.properties"))
    runner.load_all_specs(get_resource_path("."))


init()

'''
Created on 05-Feb-2020

@author: nageb
'''
import os
import sys

from logger import Logger


def main():
    logger = Logger.getLogger(__name__)
    logger.setLevel(config.log_level)
    logger.info("Loading configuration...")
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        path = 'resources/monitoring.properties'
    runner.load_config(path)
    logger.info("Loading specification...")
    config_dir_path = os.path.dirname(path)
    os.environ['CONFIG_DIR_PATH'] = config_dir_path
    runner.load_all_specs(config_dir_path)
    logger.info("Specifications loaded. Launching workflow...")
    runner.launch_workflow()


def _isK8s():
    return os.environ.get('IS_K8S') == "true"


if __name__ == "__main__":
    try:
        logger = Logger.create(ulog=_isK8s(), componentName="healthmonitoring")
        from healthmonitoring.framework import config, runner
    except RuntimeError:
        pass
    main()

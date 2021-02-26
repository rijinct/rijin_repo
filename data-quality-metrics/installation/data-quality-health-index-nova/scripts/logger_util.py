import logging
import os
from xml.dom.minidom import parse

from logger import Logger


def _isK8s():
    return os.environ.get('IS_K8S') == "true"

def create_logger():
    try:
        logger = Logger.create(ulog=_isK8s(), componentName="data-quality-health-index")
    except RuntimeError:
        pass

def get_logging_instance(log_level):
    if log_level.upper() == "INFO":
        return logging.INFO
    elif log_level.upper() == "DEBUG":
        return logging.DEBUG

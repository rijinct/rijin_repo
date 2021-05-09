'''
Created on 20-Jul-2020

@author: deerakum
'''
import logging
import os
from xml.dom.minidom import parse

from logger import Logger


def _isK8s():
    return os.environ.get('IS_K8S') == "true"

def create_logger():
    try:
        logger = Logger.create(ulog=_isK8s(), componentName="healthmonitoring")
    except RuntimeError:
        pass

def get_logging_instance(log_level):
    if log_level.upper() == "INFO":
        return logging.INFO
    elif log_level.upper() == "DEBUG":
        return logging.DEBUG


def get_default_log_level():
    monitoring_xml = get_monitoring_xml()
    log_level = monitoring_xml.getElementsByTagName('LOGLEVEL')[0]
    property_tag = log_level.getElementsByTagName('property')
    return [ element.attributes['value'].value for element in property_tag if element.hasAttribute('Name')][0]


def get_monitoring_xml():
    return parse(r'/opt/nsn/ngdb/monitoring/conf/monitoring.xml')

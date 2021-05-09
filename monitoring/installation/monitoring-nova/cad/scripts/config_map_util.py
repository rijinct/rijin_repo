from kubernetes import client,config
from kubernetes.client.rest import ApiException 
from yaml import safe_load
from flask import Response
import sys
sys.path.insert(0,'/opt/nsn/ngdb/monitoring/utils')
from logger_util import *
import json
import os

name = 'healthmonitoring-cad' 
namespace = os.environ['RELEASE_NAMESPACE']

create_logger()
LOGGER = Logger.getLogger(__name__)
default_log_level = get_default_log_level()
LOGGER.setLevel(get_logging_instance(default_log_level))

technicalMetadata = "technical_metadata_export_tables"
operationalMetadata = "operational_metadata_export_jobs"

class ConfigMapUtil: 
    def get_api_instance():
        config.load_incluster_config()
        return client.CoreV1Api()
    
    def read_cad_properties():
        try:
            api_response = ConfigMapUtil.get_api_instance().read_namespaced_config_map(name,namespace)
            return api_response.data
        except ApiException as e:
            LOGGER.error("Exception when calling CoreV1Api->read_namespaced_config_map: %s\n" % e)
    
    def read_cad_cm_data():
        try:
            config.load_incluster_config()
            api_instance = client.CoreV1Api() 
            api_response = api_instance.read_namespaced_config_map(name,namespace)
            return api_response.data
        except ApiException as e:
            LOGGER.error("Exception when calling CoreV1Api->read_namespaced_config_map: %s\n" % e)
    
    def retrieve_table_pattern():
        cadProp = ConfigMapUtil.read_cad_cm_data()
        return list(cadProp[technicalMetadata].split(','))
        
    def retrieve_export_jobs():
        cadProp = ConfigMapUtil.read_cad_properties()
        return list(cadProp[operationalMetadata].strip("{}").split(','))
    
    def retrieve_config_param(metadata):
        if metadata == 'technicalMetadata':
            output = ConfigMapUtil.retrieve_table_pattern()
        else:
            output = ConfigMapUtil.retrieve_export_jobs()
        
        message = json.dumps(
                {"response": "Config Param for {} is: {}".format(metadata, output)})
        resp = Response(message, status=200, mimetype='application/json')
        return resp

    def patch_configMap_data(metadata,pattern_list):
        cadProp = ConfigMapUtil.read_cad_properties()
        cadProp[globals()[metadata]] = pattern_list
        
        patch_data = {'data': cadProp}

        api_response = ConfigMapUtil.get_api_instance().patch_namespaced_config_map(name, namespace, patch_data)
        message = json.dumps(
                {"response": "Config Param for {} metadata is updated".format(metadata)})
        return Response(message, status=200, mimetype='application/json')
        
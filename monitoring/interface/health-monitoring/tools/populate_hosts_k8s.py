'''
Created on 09-Jul-2020

@author: deerakum
'''
import argparse
import sys
import os
from yaml import safe_load, dump
from pathlib import Path

from logger import Logger
from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)

HOST_GROUP = "hostgroups"
HOST_VAR_NAME = "name"
FIELD_NAME = "fields"
LIST_FIELD_NAME = "list_fields"
HOST_VALUE = "hosts"

env_variables = {key.lower(): value for key, value in dict(os.environ).items()}


def main():
    if len(sys.argv) != 3:
        raise ValueError("Expected two arguments")
    template_path, spec_path = sys.argv[1], sys.argv[2]
    _populate_config_map_as_dict()
    hosts_spec = read_host_spec(template_path)
    _fill_information(hosts_spec)
    write_host_spec(spec_path, hosts_spec)


def _populate_config_map_as_dict():
    global config_map_dict
    config_map_path = Path("/opt/nsn/ngdb/monitoring/extEndPointsConfigMap")
    files_list = list(item for item in config_map_path.iterdir()
                      if item.is_file())
    config_map_dict = {
        f.name.lower(): _read_content(config_map_path, f.name)
        for f in files_list
    }


def _read_content(path, file_name):
    with open(path / file_name) as file:
        return file.read()


def read_host_spec(template_path):
    try:
        with open(template_path, 'r') as f:
            return safe_load(f)
    except Exception:
        logger.exception("reading failed...")
        sys.exit(1)


def _fill_information(hosts_spec):
    for host_group in hosts_spec[HOST_GROUP]:
        service_names = hosts_spec[HOST_GROUP][host_group].keys()
        host_group = hosts_spec[HOST_GROUP][host_group]
        for service_name in service_names:
            _fill_fields(host_group, service_name)
            _fill_list_fields(host_group, service_name)
            _fill_hosts(host_group, service_name)


def _fill_fields(host_group, service_name):
    for field in host_group[service_name].get(FIELD_NAME, []):
        _populate_info(host_group, service_name, field.lower())


def _fill_list_fields(host_group, service_name):
    for field in host_group[service_name].get(LIST_FIELD_NAME, []):
        _populate_list_fields_info(host_group, service_name, field.lower())


def _populate_list_fields_info(host_group, service_name, field):
    if _field_exists_in_env(field):
        host_group[service_name][field] = env_variables[field].split()
    else:
        logger.debug("Populating the field '%s' through Config Map ", field)
        host_group[service_name][field] = config_map_dict.get(field).split()


def _fill_hosts(host_group, service_name):
    if HOST_VAR_NAME in host_group[service_name].keys():
        field = host_group[service_name][HOST_VAR_NAME]
        _populate_info(host_group, service_name, field.lower())


def _populate_info(host_group, service_name, field):
    if _field_exists_in_env(field):
        logger.debug("Populating the field '%s' through env variables ", field)
        host_group[service_name][field] = env_variables[field]
    else:
        logger.debug("Populating the field '%s' through config map ", field)
        host_group[service_name][field] = config_map_dict.get(field)


def _field_exists_in_env(field_name):
    return field_name in env_variables


def write_host_spec(spec_path, host_spec):
    try:
        with open(spec_path, 'w') as f:
            dump(host_spec, f)
    except Exception:
        logger.exception("writing configuration failed...")
        sys.exit(1)


if __name__ == "__main__":
    main()

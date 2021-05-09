import sys
import subprocess
import argparse
from yaml import safe_load, dump
from logger import Logger

from healthmonitoring.framework import config

logger = Logger.getLogger(__name__)
logger.setLevel(config.log_level)

APPLICATION_DEF_PATH = "/opt/nsn/ngdb/ifw/lib/application/utils/application_definition.sh"  # noqa: E501
PLATFORM_DEF_PATH = "/opt/nsn/ngdb/ifw/utils/platform_definition.sh"
template_path = ""
spec_path = ""
HOST_GROUP = "hostgroups"
PLATFORM_SUB_GROUP = "platform"
APP_SUB_GROUP = "application"
HOST_VALUE = "hosts"
HOST_VAR_NAME = "name"
FIELD_NAME = "fields"
LIST_FIELD_NAME = "list_fields"


def main():
    global template_path, spec_path
    source_application_definition(APPLICATION_DEF_PATH)
    args = process_args()
    template_path, spec_path = args.source, args.target
    host_spec = read_host_spec()
    fill_hosts(APP_SUB_GROUP, host_spec)
    fill_hosts(PLATFORM_SUB_GROUP, host_spec)
    write_host_spec(host_spec)


def source_application_definition(def_file_path):
    app_def_file = open(def_file_path)
    exec(app_def_file.read().replace("(", "(\"").replace(")", "\")"),
         globals())
    app_def_file.close()


def process_args():
    p = argparse.ArgumentParser(
        description="""Usage: populate_host_spec.py """)
    p.add_argument("-s", "--source", help="source template location")
    p.add_argument("-t", "--target", help="target file name with path")
    return p.parse_args()


def read_host_spec():
    try:
        with open(template_path, 'r') as f:
            return safe_load(f)
    except Exception:
        logger.exception("reading failed...")
        sys.exit(1)


def fill_hosts(host_type, config):
    host_group = config[HOST_GROUP][host_type]
    service_names = host_group.keys()
    for service_name in service_names:
        _populate_hosts(host_type, host_group, service_name)
        _populate_fields(host_type, host_group, service_name)
        _populate_list_fields(host_type, host_group, service_name)


def write_host_spec(host_spec):
    try:
        with open(spec_path, 'w') as f:
            dump(host_spec, f)
    except Exception:
        logger.exception("writing configuration failed...")
        sys.exit(1)


def _populate_hosts(host_type, host_group, service_name):
    if HOST_VAR_NAME in host_group[service_name].keys():
        host_group[service_name][HOST_VALUE] = _get_hosts(
            host_type, host_group[service_name][HOST_VAR_NAME])


def _populate_fields(host_type, host_group, service_name):
    for field in host_group[service_name].get(FIELD_NAME, []):
        host_group[service_name][field] = _get_field(host_type, field)


def _populate_list_fields(host_type, host_group, service_name):
    for field in host_group[service_name].get(LIST_FIELD_NAME, []):
        host_group[service_name][field] = _get_list_field(
            host_type, field)


def _get_hosts(host_type, name):
    return _get_list_field(host_type, name)


def _get_field(host_type, name):
    return _get_list_field(host_type, name)[0]


def _get_list_field(host_type, name):
    if host_type == APP_SUB_GROUP:
        return _get_application_list_field(name)
    else:
        return _get_platform_list_field(name)


def _get_platform_list_field(name):
    global cemod_hive_hosts, PLATFORM_DEF_PATH
    namenode = cemod_hive_hosts.split(' ')[0]
    field_to_search = "{" + name + "[@]}"
    cmd = "ssh {} 'source {}; echo ${}' ".format(
        namenode, PLATFORM_DEF_PATH, field_to_search)
    return subprocess.getoutput(cmd).split()


def _get_application_list_field(name):
    statement = "out = {}.split()".format(name)
    exec(statement, globals(), locals())
    return locals()['out']


if __name__ == '__main__':
    main()

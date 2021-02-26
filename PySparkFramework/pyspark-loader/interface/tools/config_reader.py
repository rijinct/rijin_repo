#  """
#  @author: pranchak
#  """

import glob
import os
import re
from pathlib import Path

import tools_util as util

QUERY_HINTS = "QueryHints"
QUERY_SETTINGS = "QuerySettings"
GLOBAL_SETTINGS = "GlobalSettings"
LOCAL_SETTINGS = "LocalSettings"
CB_INSTALLED = "COUCHBASE_LOOKUP"
JOB = "Job"
DEFAULT_HINTS = "DEFAULT"
SPARK_EVENT_LOG = "PROJECT_SPARK_EVENTLOG_DIRECTORY"
SPARK_HISTORY_SERVER = "PROJECT_HISTORYSERVER_ADDRESS"
APPLICATION_DEF_PATH = "/opt/nsn/ngdb/ifw/lib/application/utils" \
                       "/application_definition.sh"  # noqa:501


def get_global_settings(args):
    global_dict = {}
    global_dict[GLOBAL_SETTINGS] = {}
    config = util.read_yaml(args.hints_file)
    for key, value in config[QUERY_SETTINGS][GLOBAL_SETTINGS].items():
        if (not isinstance(value, dict)):
            global_dict[GLOBAL_SETTINGS][key] = config[QUERY_SETTINGS][
                GLOBAL_SETTINGS][key]
    return global_dict


def get_hints(args, job_name):
    job_hint_dict = dict()
    config = util.read_yaml(args.hints_file)
    job_hint_dict.update(
        _read_config(config[QUERY_SETTINGS][GLOBAL_SETTINGS][JOB], job_name))
    job_hint_dict.update(
        _read_config(config[QUERY_SETTINGS][LOCAL_SETTINGS][JOB], job_name))
    job_hint_dict.update(_update_custom_hints(args, job_name))
    job_hint_dict.update(_update_schema_event_log_and_history())
    config_str = _create_config_string(job_hint_dict)
    jars = ",".join(
        [jar for jar in _get_jars_list(args.lib_path) if Path(jar).exists()])
    return f"--jars {jars} {config_str}"


def get_job_name(target_path):
    path = Path(target_path)
    path_split = path.parts
    if "us" in path_split:
        spec_type = "Usage"
        agg_type = "LoadJob"
    elif "es" in path_split:
        spec_type = "Entity"
        agg_type = "CorrelationJob"
    else:
        spec_type = "Perf"
        agg_type = "AggregateJob"
    return "{}_{}_{}".format(spec_type, path.name, agg_type)


def is_couchbase_installed(args):
    config = util.read_yaml(args.hints_file)
    if CB_INSTALLED in config[QUERY_SETTINGS][GLOBAL_SETTINGS] and not config[
            QUERY_SETTINGS][GLOBAL_SETTINGS][CB_INSTALLED]:
        return False
    else:
        return True


def _get_jars_list(path):
    return glob.glob(os.path.join(path, '*.jar'))


def _create_config_string(hint):
    config_string = ""
    for key, value in hint.items():
        config_string += "--conf {key}={value} ".format(key=key, value=value)
    return config_string


def _extract_query_hints(config_pattern):
    job_hint_dict = {}
    for hint in config_pattern[QUERY_HINTS]:
        for name, value in hint.items():
            job_hint_dict[name] = str(value)
    return job_hint_dict


def _read_config(config, job_name):
    job_hint_dict = {}
    if config:
        for pattern in config.keys():
            if pattern == DEFAULT_HINTS or re.fullmatch(pattern, job_name):
                job_hint_dict.update(_extract_query_hints(config[pattern]))
    return job_hint_dict


def _update_custom_hints(args, job_name):
    job_hint_dict = {}
    custom_file = util.get_replaced_file_name(args.hints_file, "custom")
    if os.path.exists(custom_file):
        config = util.read_yaml(custom_file)
        job_hint_dict.update(
            _read_config(config[QUERY_SETTINGS][GLOBAL_SETTINGS][JOB],
                         job_name))
        job_hint_dict.update(
            _read_config(config[QUERY_SETTINGS][LOCAL_SETTINGS][JOB],
                         job_name))
    return job_hint_dict


def _update_schema_event_log_and_history():
    if os.environ.get('IS_K8S'):
        return {
            'spark.eventLog.enabled':
            'true',
            'spark.eventLog.dir':
            os.environ.get(SPARK_EVENT_LOG),
            'spark.yarn.historyServer.address':
            os.environ.get(SPARK_HISTORY_SERVER),
            'spark.yarn.appMasterEnv.HIVESCHEMA':
            os.environ.get('DB_SCHEMA')
        }
    else:
        file = open(APPLICATION_DEF_PATH)
        exec(file.read().replace("(", "(\"").replace(")", "\")"), globals())
        file.close()
        return {
            'spark.yarn.appMasterEnv.HIVESCHEMA': project_hive_schema
        }  # noqa:821

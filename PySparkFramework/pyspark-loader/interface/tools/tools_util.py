#  """
#  @author: pranchak
#  """
import re
import sys
from pathlib import Path

import yaml

is_double_quote_required = False


def get_replaced_file_name(file_name, prefix):
    conf_file = Path(file_name)
    custom_filename = "{}_{}".format(prefix, conf_file.name)
    return str(Path(conf_file.parent).joinpath(custom_filename))


def get_new_lines_replaced_query(query):
    query = re.sub('\n', ' ', query)
    query = re.sub('\t', ' ', query)
    return query


def read_sql_file(sql_path):
    if sql_path:
        with open(sql_path, 'r') as f:
            return f.read()


def read_yaml(file):
    try:
        with open(file, 'r') as file:
            return yaml.safe_load(file)
    #    except yaml.YAMLError as exc:
    #        print(exc)
    #        sys.exit(1)
    except Exception:
        pass
    return {}


def mk_double_quote(dumper, data):
    global is_double_quote_required
    dict_keys = ['query']
    if data in dict_keys:
        is_double_quote_required = True
        return dumper.represent_scalar('tag:yaml.org,2002:str', data,
                                       style='')
    if is_double_quote_required:
        is_double_quote_required = False
        return dumper.represent_scalar('tag:yaml.org,2002:str', data,
                                       style='"')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='')


def write_output_yaml(data, file_name):
    try:
        with open(file_name, 'w') as f:
            yaml.add_representer(str, mk_double_quote)
            yaml.dump(data, f, default_flow_style=False)
    except Exception:
        sys.exit(1)

import argparse
import glob

import tools_util as util
from constants import ExtensionConstants as EC
from constants import FileConstants as FC
from hash_parser import hash_replace_query,hash_replace_udf
from config_reader import get_hints, get_job_name
from yaml_generator import YamlGenerator

DIM_EXTN_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/conf" \
                r"/dimension_extensions/"  # noqa:501
YAML_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/yaml/"


def add_process_sub_parser(sub_parser):
    conf_parser = sub_parser.add_parser("process")
    conf_parser.add_argument("-action",
                             "--action",
                             required=True,
                             help="action to be performed")
    conf_parser.add_argument("-t",
                             "--target_path",
                             required=False,
                             help="hdfs path of the target table")
    conf_parser.add_argument("-s",
                             "--source_path",
                             required=False,
                             help="hdfs path of the target table")
    conf_parser.add_argument("-tz",
                             "--timezone",
                             default="Default",
                             help="timezone")
    conf_parser.add_argument("-l",
                             "--lower_bound",
                             required=False,
                             help="lower bound")
    conf_parser.add_argument("-u",
                             "--upper_bound",
                             required=False,
                             help="upper bound")
    conf_parser.add_argument("-c",
                             "--conf_files",
                             required=False,
                             help="comma separated yaml files")
    conf_parser.add_argument("-q",
                             "--query_file",
                             required=False,
                             help="File containing aggregation query")
    conf_parser.add_argument("-p",
                             "--lib_path",
                             help="path containing custom udf jar",
                             default="/etc/hive/lib")
    conf_parser.add_argument("-f",
                             "--hints_file",
                             required=False,
                             help="Query Hints YaML file")
    conf_parser.add_argument("-n",
                             "--job_name",
                             required=False,
                             help="Name of the job")


def get_input_args(args, yml_path):
    args.conf_files = yml_path
    return " -t {} -s {} -tz {} -l {} -u {} -c {}".format(
            args.target_path, args.source_path, args.timezone,
            args.lower_bound
            , args.upper_bound, args.conf_files)


def process_args():
    ap = argparse.ArgumentParser(description="Usage: config_generator.py")
    sub_parser = ap.add_subparsers(dest="cmd")
    add_process_sub_parser(sub_parser)
    return ap.parse_known_args()[0]


def get_yaml_files(conf_files, extension):
    out = []
    for file in conf_files.split(','):
        out.append(file)
    if extension:
        out.append(extension)
    return ",".join(out)


def merger():
    product_dimension = get_product_dimension_extension()
    custom_dimension = get_custom_dimension_extension()
    merged_dimension = {}
    if not custom_dimension:
        merged_dimension = product_dimension
    else:
        for key in product_dimension:
            if key not in custom_dimension:
                merged_dimension.update({key: product_dimension[key]})
            elif key in custom_dimension:
                product_dimension[key]['target_tables'].update(
                        custom_dimension[key]['target_tables'])
                merged_dimension.update({key: product_dimension[key]})

        for key in custom_dimension:
            if key not in product_dimension:
                merged_dimension.update({key: custom_dimension[key]})
    path_name = "{}merged_dimension_extension.yaml".format(DIM_EXTN_PATH)
    util.write_output_yaml(merged_dimension, path_name)
    return path_name


def get_product_dimension_extension():
    return load_and_merge_yaml(
            glob.glob("{}*.yaml".format(DIM_EXTN_PATH)) +
            glob.glob("{}*.yml".format(DIM_EXTN_PATH)))


def get_custom_dimension_extension():
    custom_dimension_pathname = r"{}\custom\\".format(DIM_EXTN_PATH)
    return load_and_merge_yaml(
            glob.glob("{}*.yaml".format(custom_dimension_pathname)) +
            glob.glob("{}*.yml".format(custom_dimension_pathname)))


def load_and_merge_yaml(yaml_files):
    merged_yaml = {}
    for yaml_file in yaml_files:
        try:
            merged_yaml.update(util.read_yaml(yaml_file))
        except Exception:
            continue
    return merged_yaml


def call_yaml_generator(yaml_file, job_name, query_file):
    if not yaml_file:
        if query_file:
            yg = YamlGenerator(query_file, None)
            return yg.generate_yaml(job_name, query_file)
        else:
            raise ValueError(
                    "Missing arguments!! -q query_file or -c yaml_file "
                    "must "
                    "be passed")
    else:
        yg = YamlGenerator(query_file, yaml_file)
        return yg.get_updated_yaml_with_mandatory_fields(
                job_name,
                query_file)


def process_actions(args):
    yaml_file = None
    job_name = get_job_name(args.target_path)
    merger()
    yaml_file = call_yaml_generator(yaml_file, job_name,
                                    args.query_file)
    if args.conf_files:
        yaml_file = util.read_yaml(args.conf_files)
        yaml_file = hash_replace_query(yaml_file, args.lower_bound,
                                       args.upper_bound, job_name)
    yaml_file=hash_replace_udf(yaml_file,job_name)
    path_name = "{}replaced_{}{}".format(FC.YAML_PATH.value, job_name,
                                         EC.YAML_EXT.value)
    util.write_output_yaml(yaml_file, path_name)
    return get_input_args(args, path_name)


def main():
    args = process_args()
    action = args.action.lower()
    if action == "conf":
        print(get_hints(args, args.job_name))
    elif action == "job_name":
        print(get_job_name(args.target_path))
    elif action == "process_all":
        print(process_actions(args))


if __name__ == "__main__":
    main()

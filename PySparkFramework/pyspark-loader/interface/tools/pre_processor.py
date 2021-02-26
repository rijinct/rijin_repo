import argparse
import glob
from pathlib import Path

import tools_util as util
from config_reader import get_global_settings, is_couchbase_installed
from config_reader import get_hints, get_job_name
from constants import ExtensionConstants as EC
from constants import FileConstants as FC
from hash_parser import hash_replace_query, hash_replace_udf
from yaml_generator import YamlGenerator

DIM_EXTN_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/conf" \
                r"/dimension_extensions/"  
YAML_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/yaml/"
CONF_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/conf/"
DIM_TBL_REL_FILE = r"table_mapping.yaml"
DIM_COL_MAP_FILE = r"dimension_column_mapping.yaml"

def main():
    args = process_args()
    action = args.action.lower()
    if action == "conf":
        print(get_hints(args, args.job_name))
    elif action == "job_name":
        print(get_job_name(args.target_path))
    elif action == "process_all":
        print(pre_process(args))


def process_args():
    ap = argparse.ArgumentParser(description="Usage: config_generator.py")
    ap.add_argument("-action",
                    "--action",
                    required=True,
                    help="action to be performed")
    ap.add_argument("-t",
                    "--target_path",
                    required=False,
                    help="hdfs path of the target table")
    ap.add_argument("-s",
                    "--source_path",
                    required=False,
                    help="hdfs path of the target table")
    ap.add_argument("-tz", "--timezone", default="Default", help="timezone")
    ap.add_argument("-l", "--lower_bound", required=False, help="lower bound")
    ap.add_argument("-u", "--upper_bound", required=False, help="upper bound")
    ap.add_argument("-c",
                    "--agg_input_files",
                    required=False,
                    help="comma separated aggregation input file")
    ap.add_argument("-q",
                    "--query_file",
                    required=False,
                    help="aggregation query file")
    ap.add_argument("-p",
                    "--lib_path",
                    help="path containing custom udf jar",
                    default="/etc/hive/lib")
    ap.add_argument("-f", "--hints_file", required=False, help="hints file")
    ap.add_argument("-n", "--job_name", required=False, help="name of the job")
    return ap.parse_args()


def pre_process(args):
    job_name = get_job_name(args.target_path)
    global_dict = get_global_settings(args)
    default_dict = get_default_dict(job_name, args)
    additional_dict = get_additional_dict(args, default_dict)
    merged_dict = merge_dict(default_dict, additional_dict)
    merged_dict = hash_replace_udf(merged_dict, job_name)
    if args.agg_input_files:
        merged_dict = hash_replace_query(merged_dict, args.lower_bound,
                                         args.upper_bound, job_name)
    path_name = "{}replaced_{}{}".format(FC.YAML_PATH.value, job_name,
                                         EC.YAML_EXT.value)
    merged_dict.update(global_dict)
    merged_dict = remove_empty_fields(merged_dict)
    util.write_output_yaml(merged_dict, path_name)
    return get_input_args(args, path_name)


def get_default_dict(job_name, args):
    if args.agg_input_files:
        yaml_file = util.read_yaml(args.agg_input_files.split(',')[0])
        yg = YamlGenerator(args, yaml_file)
        return yg.get_updated_yaml_with_mandatory_fields(
            job_name, args.query_file)
    elif args.query_file:
        yg = YamlGenerator(args, None)
        return yg.generate_yaml(job_name, args.query_file)
    else:
        raise ValueError(
            "Missing arguments!! -q query_file or -c agg_input_files "
            "must "
            "be passed")


def get_additional_dict(args, conf_file_dict):
    additional_dict = {}
    if not is_couchbase_installed(args):
        tbl_map = merge_product_custom(CONF_PATH, DIM_TBL_REL_FILE)
        additional_dict['dimension_column_mapping'] = merge_product_custom(
            DIM_EXTN_PATH, DIM_COL_MAP_FILE)
        additional_dict = merge_dict(tbl_map, additional_dict)
        table_list, source_target_tables_list = get_source_target_table_names(
            additional_dict, args,
            conf_file_dict)
        remove_not_needed_table_from_yaml(additional_dict,
                                          source_target_tables_list,
                                          table_list)
    else:
        additional_dict['dimension_column_mapping'] = merge_dim_extension()
    return additional_dict


def remove_not_needed_table_from_yaml(additional_dict,
                                      source_target_tables_list, table_list):
    remove_not_needed_table(additional_dict, table_list,
                            'dimension_column_mapping')
    remove_not_needed_table(additional_dict, source_target_tables_list,
                            'table_relationship')
    remove_not_needed_table(additional_dict, source_target_tables_list,
                            'table_udf_mapping')


def remove_not_needed_table(additional_dict, table_list, key):
    if additional_dict.get(key):
        for table, value in list(additional_dict[key].items()):
            if table not in table_list:
                del additional_dict[key][
                    table]


def get_source_target_table_names(additional_dict, args, conf_file_dict):
    table_list = []
    source_target_tables_list = []
    for source_path in args.source_path.split(","):
        table_name = get_table_name(source_path)
        source_target_tables_list.append(table_name)
        update_table_list(additional_dict, table_list, table_name)
    target_table_name = conf_file_dict['target_table_name'] if \
        'target_table_name' in conf_file_dict.keys() and conf_file_dict[
            'target_table_name'] else get_table_name(
        args.target_path)
    update_table_list(additional_dict, table_list, target_table_name)
    source_target_tables_list.append(target_table_name)
    return table_list, source_target_tables_list


def update_table_list(additional_dict, table_list, target_table_name):
    if additional_dict.get('table_relationship'):
        if target_table_name in additional_dict['table_relationship'].keys():
            table_list.extend(
                additional_dict['table_relationship'][target_table_name])


def get_table_name(source_path):
    return "{0}_{1}".format(
        source_path.rsplit("/ngdb/")[1].split('/')[0],
        Path(source_path).name.lower())


def merge_product_custom(path, file_name):
    pr_tbl_rel = util.read_yaml("{0}/{1}".format(path, file_name))
    cstm_tbl_rel = util.read_yaml("{0}/custom/{1}".format(path, file_name))
    return merge_dict(pr_tbl_rel, cstm_tbl_rel)


def merge_dict(target_dict, source_dict):
    for key in source_dict.keys():
        if key in target_dict.keys() and isinstance(target_dict[key], dict):
            merge_dict(target_dict[key], source_dict[key])
        else:
            target_dict[key] = source_dict[key]
    return target_dict


def merge_dim_extension():
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
    return merged_dimension


def get_product_dimension_extension():
    return load_and_merge_yaml(
        glob.glob("{}dimension_extension*.yaml".format(DIM_EXTN_PATH)) +
        glob.glob("{}dimension_extension*.yml".format(DIM_EXTN_PATH)))


def get_custom_dimension_extension():
    custom_dimension_pathname = r"{}\custom\\".format(DIM_EXTN_PATH)
    return load_and_merge_yaml(
        glob.glob("{}dimension_extension*.yaml".format(
            custom_dimension_pathname)) +
        glob.glob("{}dimension_extension*.yml".format(
            custom_dimension_pathname)))


def load_and_merge_yaml(yaml_files):
    merged_yaml = {}
    for yaml_file in yaml_files:
        try:
            merged_yaml.update(util.read_yaml(yaml_file))
        except Exception:
            continue
    return merged_yaml


def remove_empty_fields(output):
    yaml_dict = {k: v for k, v in output.items() if v}
    for key, value in yaml_dict.items():
        if isinstance(value, dict):
            final_dict = {inner_k: inner_v for inner_k, inner_v in
                value.items() if inner_v}
            yaml_dict[key] = final_dict
    return yaml_dict


def get_input_args(args, yml_path):
    return " -t {} -s {} -tz {} -l {} -u {} -c {}".format(
        args.target_path, args.source_path, args.timezone, args.lower_bound,
        args.upper_bound, yml_path)


if __name__ == "__main__":
    main()

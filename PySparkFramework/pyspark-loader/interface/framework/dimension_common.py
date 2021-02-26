import argparse as ap
from pathlib import Path
from specification import SourceSpecification
from specification import TargetSpecification


def process_args():
    p = ap.ArgumentParser(description="Usage: dyanmic_dimension_loader.py")
    p.add_argument("-s",
                   "--source_paths",
                   required=True,
                   help="source location(s)")
    p.add_argument("-t",
                   "--target_path",
                   required=True,
                   help="target location")
    args = p.parse_args()
    return args


def get_sources_and_target(inargs):
    hive_schema = "project"
    source_specs = []
    source_paths = inargs.source_paths.split(',')
    for source_path in source_paths:
        source_specs.append(
            SourceSpecification(source_path.replace('work/', ''), hive_schema))
    target_spec = TargetSpecification(inargs.target_path, hive_schema)
    return source_specs, target_spec


def get_hdfs_corrupt_filename(hdfs_path):
    last_folder_name = Path(hdfs_path).name
    return "/tmp/duplicate/{0}/".format(last_folder_name)

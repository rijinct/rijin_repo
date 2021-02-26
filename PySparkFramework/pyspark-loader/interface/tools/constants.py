from enum import Enum


class YamlConstants(Enum):
    COMMON = "common"
    DESCRIPTION = "description"
    TARGET_PATH = "target_path"
    SUBPARTITION_COLUMNS = "subpartition_columns"
    SUBPARTITION = "partition"
    COLUMNS_WITH_VALUES = "columns_with_values"
    COLUMNS = "columns"
    SQL = "sql"
    QUERY = "query"
    NUMBER_OF_OUTPUT_FILES = "number_of_output_files"
    TARGET_TABLE_NAME = "target_table_name"


class FileConstants(Enum):
    TEMPLATE_YAML = r"/mnt/ContentAndAdaptation/pyspark-loader/yaml" \
                    r"/sample_template.yaml"
    YAML_PATH = r"/mnt/ContentAndAdaptation/pyspark-loader/yaml/"


class ExtensionConstants(Enum):
    YAML_EXT = ".yaml"

SOURCE_CEM = "CEM"
SOURCE_PRODUCT = "PRODUCT"
SOURCE_CUSTOM = "CUSTOM"

CONSIDER_ALL_COULUMN = False
TABLE_NAMES_TO_CONSIDER = []
#Provide table names in upper case separated by comma. Example : ["US_SMS_1","US_BB_APPLICATIONS_1"]

DATA_RETENTION_DAYS = 14
MAX_START_DATE_RANGE = 7

STREAMING_CONSOLE = False

LOG_LEVEL = 'INFO'

DEFAULT_VALUES_FOR_FIELDS_DEFINITION_DF = {"completeness_rule_name": "NULL_IN_SOURCE",
          "conformity_rule_name": "NULL_IN_SOURCE",
          "consistency_rule_name":"NULL_IN_SOURCE",
          "integrity_rule_name": "NULL_IN_SOURCE",
          "range_rule_name":"NULL_IN_SOURCE",
          "uniqueness_rule_name":"NULL_IN_SOURCE",
          "completeness_threshold":5,
          "conformity_threshold": 5,
          "consistency_threshold":5,
          "integrity_threshold": 5,
          "range_threshold":5,
          "uniqueness_threshold":5,
          "completeness_weight":1,
          "conformity_weight": 1,
          "consistency_weight":1,
          "integrity_weight": 1,
          "range_weight":1,
          "uniqueness_weight":1
          }

DEFAULT_VALUES_FOR_PRE_COMPUTED_SCORE_DF = {"completeness_rule_name": "NULL_IN_SOURCE",
          "conformity_rule_name": "NULL_IN_SOURCE",
          "consistency_rule_name":"NULL_IN_SOURCE",
          "integrity_rule_name": "NULL_IN_SOURCE",
          "range_rule_name":"NULL_IN_SOURCE",
          "uniqueness_rule_name":"NULL_IN_SOURCE",
          "completeness_threshold":5,
          "conformity_threshold": 5,
          "consistency_threshold":5,
          "integrity_threshold": 5,
          "range_threshold":5,
          "uniqueness_threshold":5,
          "completeness_weight":1,
          "conformity_weight": 1,
          "consistency_weight":1,
          "integrity_weight": 1,
          "range_weight":1,
          "uniqueness_weight":1
          }

DIMENSION_COMPLETENESS = "COMPLETENESS"
DIMENSION_CONFORMITY = "CONFORMITY"
DIMENSION_UNIQUENESS = "UNIQUENESS"
DIMENSION_RANGE = "RANGE"
DIMENSION_INTEGRITY = "INTEGRITY"
DIMENSION_CONSISTENCY = "CONSISTENCY"

DEFAULT_RULES = {
DIMENSION_COMPLETENESS:["1", "is_null_check", "NONE", 5, 1],
DIMENSION_CONFORMITY:["10", "is_pattern_check", "/^[A-Za-z]+$/", 5, 1],
DIMENSION_UNIQUENESS:["4", "is_unique_check", "NONE", 5, 1],
DIMENSION_RANGE:["9", "is_between_values", "0-1000000", 5, 1],
DIMENSION_INTEGRITY:["1", "is_null_check", "NONE", 5, 1],
DIMENSION_CONSISTENCY:["1", "is_null_check", "NONE", 5, 1]
}


PRE_COMPUTED_SCORES_FILE = 'PRE_COMPUTED_SCORES.xlsx'

PRE_COMPUTED_SCORES_COLUMNS = ["field_name",
                               "field_table_name",
                               "completeness_rule_name",
                               "completeness_rule_parameter",
                               "completeness_threshold",
                               "completeness_weight",
                               "completeness_bad_record_count",
                               "completeness_score",
                               "completeness_check_pass"]

TEST_ENABLE = True
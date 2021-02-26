import os
CONSIDER_ALL_COULUMN = False

TABLE_NAMES_TO_CONSIDER = []
#Provide table names in upper case separated by comma. Example : ["US_SMS_1","US_BB_APPLICATIONS_1"]

ENABLE_LOCAL_EXECUTION = bool(os.getenv('LOCAL_EXECUTION_ENABLE', False))

if ENABLE_LOCAL_EXECUTION is True or os.getenv('IS_K8S') != "true":
    DATA_RETENTION_DAYS = 14
    MAX_START_DATE_RANGE = 7
else:
    DATA_RETENTION_DAYS = int(os.environ['DATA_RETENTION_IN_DAYS'])
    MAX_START_DATE_RANGE = int(os.environ['MAX_START_DATE_RANGE'])

STREAMING_CONSOLE = True

LOG_LEVEL = 'INFO'

DEFAULT_VALUES_FOR_FIELDS_DEFINITION_DF = {"completeness_rule_name": "NULL_IN_SOURCE",
          "conformity_rule_name": "NULL_IN_SOURCE",
          "consistency_rule_name":"NULL_IN_SOURCE",
          "integrity_rule_name": "NULL_IN_SOURCE",
          "range_rule_name":"NULL_IN_SOURCE",
          "uniqueness_rule_name":"NULL_IN_SOURCE",
          
          "completeness_rule_parameter": "NONE",
          "conformity_rule_parameter": "NONE",
          "consistency_rule_parameter":"NONE",
          "integrity_rule_parameter": "NONE",
          "range_rule_parameter":"NONE",
          "uniqueness_rule_parameter":"NONE",
          
          "completeness_custom_expression" : "NONE",
          "conformity_custom_expression" : "NONE",
          "consistency_custom_expression" : "NONE",
          "integrity_custom_expression" : "NONE",
          "range_custom_expression" : "NONE",
          "uniqueness_custom_expression" : "NONE",
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
DIMENSION_UNIQUENESS:["4", "is_unique_check", "NONE", 5, 1],
}


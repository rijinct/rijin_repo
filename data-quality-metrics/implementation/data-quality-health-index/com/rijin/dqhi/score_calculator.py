from numpy import double
from pyspark.sql.utils import ParseException, AnalysisException

import common_constants
import common_utils
import constants
import date_utils
import rules
from connection_wrapper import ConnectionWrapper
from query_executor import DBExecutor


LOGGER = common_utils.get_logger()


class ScoreCalculator:
    
    def __init__(self,
                 df,
                 table_name,
                 kpi_cde_df,
                 rule_name_id_dict,execution="Baremetal"):
        self.df = df
        self.table_name = table_name
        if execution == "Baremetal":
            self.postgres_connection_obj = DBExecutor(ConnectionWrapper.get_postgres_connection_instance())
        self.kpi_cde_df = kpi_cde_df
        self.rule_name_id_dict = rule_name_id_dict
    
    def trigger_function(self, dimension, column, column_data_type):
        rtv = self.handle_for_product_and_customization(dimension, column)
        try:
            if rtv is not None:
                start_time = date_utils.get_current_date_time()
                rule_parameter = rtv["rule_parameter"]
                rule_class = rtv["rule_class"]
                custom_expression = rtv["custom_expression"]
                default_excludes = rtv.get("default_excludes","")
                if rule_parameter == 'NONE':
                    result_fn = getattr(rules, rule_class.lower())
                    bad_record_count = result_fn(self.df, column, custom_expression,default_excludes)
                else:
                    rule_class = rule_class.lower()
                    
                    if rule_class == "is_equal_to_int_check":
                        if column_data_type == "double" or "decimal" in column_data_type:
                            rule_parameter = double(rule_parameter)
                        elif column_data_type == "int" or column_data_type == "bigint":
                            rule_parameter = int(rule_parameter)
                    result_fn = getattr(rules, rule_class.lower())
                    bad_record_count = result_fn(self.df, column, rule_parameter, custom_expression,default_excludes)
                
                rtv["start_time"] = start_time
                rtv["end_time"] = date_utils.get_current_date_time()
                rtv["total_count"] = bad_record_count.get("total_count",None)
                rtv["rule_condition_count"] = bad_record_count.get("rule_condition_count")
                rtv["bad_record_count"] = bad_record_count.get("bad_record_count")
                rtv["custom_expression"] = custom_expression
                return rtv
            else:
                return None
        except ParseException as error:
            raise Exception("Error occurred while processing '{}' for field '{}' with custom expression '{}'".format(dimension,column,custom_expression))
        except AnalysisException as error:
            raise Exception(error)
            

    def calculate_consistency_bad_records(self, dimension, column, bad_records_of_all_dimensions):
        start_time = date_utils.get_current_date_time()
        rtv = self.handle_for_product_and_customization(dimension, column)
        if rtv is not None:
            rule_name = rtv[0]
            rule_properties = self.rule_name_id_dict[rule_name].split(",")
            rule_id = rule_properties[0]
            rule_parameter = rtv[1]
            rule_threshold = rtv[2]
            rule_weight = rtv[3]
        bad_record_count = sum(bad_records_of_all_dimensions)
        return {"rule_id":int(rule_id),
                "rule_parameter":rule_parameter,
                "record_count":bad_record_count,
                "weight":rule_weight,
                "threshold":rule_threshold,
                "start_time":start_time,
                "end_time":date_utils.get_current_date_time()}
        
    def get_score(self,total_count, number_of_bad_records):
        LOGGER.info("Calculating Score with values total_usage_count: {},number_of_bad_records:{} ".format(total_count, number_of_bad_records))
        return round((float(total_count - number_of_bad_records) / total_count) * 100)

    def handle_for_product_and_customization(self, dimension, column):
        rtv = None
        column_information = common_utils.get_column_informtion(self.kpi_cde_df, dimension, self.table_name, column)
        if column_information is not None:
            LOGGER.info("Column information found in {}".format(constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE))
            rule_name = column_information[0]
            rule_properties = self.rule_name_id_dict[rule_name].split(",")
            rule_id = rule_properties[0]
            rule_class = rule_properties[1]
            default_excludes = rule_properties[2]
            
            rule_parameter = column_information[1]
            
            rule_threshold = column_information[2]
            rule_weight = column_information[3]
            custom_expression = column_information[4]
            rtv = {"rule_id":int(rule_id),
                    "rule_class":rule_class,
                    "rule_parameter":rule_parameter,
                    "default_excludes":default_excludes,
                    "weight":rule_weight,
                    "threshold":rule_threshold,
                    "source":constants.SOURCE_CEM,
                    "custom_expression":custom_expression}
            LOGGER.info("Properties fetched for dimension: {}, column: {} are {} ".format(dimension, column, rtv))
        elif common_constants.CONSIDER_ALL_COULUMN :
            rule_properties = common_constants.DEFAULT_RULES[dimension]
            if len(rule_properties) != 0: 
                rtv = {"rule_id":rule_properties[0],
                        "rule_class":rule_properties[1],
                        "rule_parameter":rule_properties[2],
                        "default_excludes":"Unknown,NULL IN SOURCE,-1,-10",
                        "threshold":rule_properties[4],
                        "weight":rule_properties[3],
                        "source":constants.SOURCE_PRODUCT,
                        "custom_expression":"NONE"}
        else:
            rtv = None
            
        return rtv


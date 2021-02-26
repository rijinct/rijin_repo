import common_constants, hdfs_file_utils
import common_utils
import constants
from connection_wrapper import ConnectionWrapper
from query_executor import DBExecutor
from score_calculator import ScoreCalculator

LOGGER = common_utils.get_logger()


class DQHIScoreHandler:

    def __init__(self, table_name,
                 total_hive_row_count,
                 hive_tabl_as_df,
                 kpi_cde_df,
                 computation_date,
                 table_column_ids,
                 rule_name_id_dict, spark=None, counter=None, execution="Baremetal"):
        self.df = hive_tabl_as_df
        self.list_of_columns = hive_tabl_as_df.columns
        self.kpi_cde_df = kpi_cde_df
        if execution == "Baremetal":
            self.sai_db_query_executor = DBExecutor(ConnectionWrapper.get_postgres_connection_instance())        
        self.table_name = table_name
        self.table_column_ids = table_column_ids
        self.table_id = table_column_ids[table_name][table_name]
        self.column_name = None
        self.column_precision = None
        self.column_datatype = None
        self.computed_date = computation_date
        self.total_row_count = total_hive_row_count
        self.auto_generated_id = counter
        self.dq_fields_definition = []
        self.fields_score_list = []
        self.execution = execution
        self.spark = spark
        self.scoreCalculator = ScoreCalculator(self.df,
                                               self.table_name,
                                               self.kpi_cde_df,
                                               rule_name_id_dict, 'k8s')

    def score_handler(self):
        for column in self.list_of_columns:
            is_entry_configured = common_utils.check_for_index(self.kpi_cde_df, self.table_name.lower(), column.lower())
            if is_entry_configured == True or common_constants.CONSIDER_ALL_COULUMN:
                self.column_name = column
                self.column_id = self.table_column_ids[self.table_name][self.column_name]
                self.column_precision = self.table_column_ids[self.table_name][self.column_name + "_precision"]
                self.column_datatype = self.table_column_ids[self.table_name][self.column_name + "_datatype"]
                auto_generated_id = self.fetch_auto_generated_id()
                data_type = self.get_dtype(column)
                LOGGER.info("Calculating score for column: {} whose datatype: {} ".format(self.column_name, data_type))
                if ("array" not in data_type):
                    try:
                        # "COMPLETENESS"
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_COMPLETENESS, column, data_type)
                        LOGGER.info('Return value {} for dimension {} for column : {}'.format(rtv, constants.DIMENSION_COMPLETENESS, column))
                        if rtv is not None:
                            total_records = rtv.get("total_count")
                            rule_condition_count = rtv["rule_condition_count"]
                            score = 0
                            if total_records != 0:
                                score = self.scoreCalculator.get_score(total_records, rule_condition_count)
                            
                            check_pass = common_utils.is_check_pass(rtv["threshold"], total_records, rule_condition_count)
                            
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_COMPLETENESS, rtv["source"], score, rtv, check_pass, total_records, rule_condition_count)
                        else :
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_COMPLETENESS, column, self.table_name))
                    except Exception as error:
                        LOGGER.debug(error.args[0])
                    
                    # Conformity
                    try:
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_CONFORMITY, column, data_type)
                        if rtv is not None:
                            total_records = rtv.get("total_count")
                            rule_condition_count = total_records - rtv["rule_condition_count"]
                            score = 0
                            if total_records != 0:
                                score = self.scoreCalculator.get_score(total_records, rule_condition_count)
                            check_pass = common_utils.is_check_pass(rtv["threshold"], total_records, rule_condition_count)
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_CONFORMITY, rtv["source"], score, rtv, check_pass, total_records, rule_condition_count)
                        else :
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_CONFORMITY, column, self.table_name))
                    except Exception as error:
                        LOGGER.debug(error.args[0])
                     
                    # Uniqueness
                    try:
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_UNIQUENESS, column, data_type)
                        if rtv is not None:
                            rule_condition_count = rtv["rule_condition_count"]
                            total_records = rtv.get("total_count")
                            score = 0
                            if total_records != 0:
                                score = round((float(rule_condition_count * 100) / total_records))
                            bad_record = total_records - rule_condition_count
                            check_pass = common_utils.is_check_pass(rtv["threshold"], total_records, bad_record)
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_UNIQUENESS, rtv["source"], score, rtv, check_pass, total_records, bad_record)
                        else :
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_UNIQUENESS, column, self.table_name))
                    except Exception as error:
                        LOGGER.debug(error.args[0])
                         
                    # Range
                    try:
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_RANGE, column, data_type)
                        if rtv is not None:
                            total_records = rtv.get("total_count")
                            rule_condition_count = total_records - rtv["rule_condition_count"]
                            score = 0
                            if total_records != 0:
                                score = self.scoreCalculator.get_score(total_records, rule_condition_count)
                            check_pass = common_utils.is_check_pass(rtv["threshold"], self.total_row_count, rule_condition_count)
                             
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_RANGE, rtv["source"], score, rtv, check_pass, total_records, rule_condition_count)
                        else :
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_RANGE, column, self.table_name))
                    except Exception as error:
                        LOGGER.debug(error.args[0])
                         
                    # Integrity
                    try:
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_INTEGRITY, column, data_type)
                        if rtv is not None:
                            total_records = rtv.get("total_count")
                            rule_condition_count = rtv["rule_condition_count"]
                            bad_record_count = rtv["bad_record_count"]
                            score = 0
                            if total_records != 0:
                                score = self.scoreCalculator.get_score(total_records, rule_condition_count)    
                            check_pass = common_utils.is_check_pass(rtv["threshold"], total_records, rule_condition_count)
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_INTEGRITY, rtv["source"], score, rtv, check_pass, total_records, bad_record_count)
                        else :
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_INTEGRITY, column, self.table_name)) 
                    except Exception as error:
                        LOGGER.debug(error.args[0])
                         
                    # Consistency
                    try:
                        rtv = self.scoreCalculator.trigger_function(constants.DIMENSION_CONSISTENCY, column, data_type)
                        if rtv is not None:
                            total_records = rtv.get("total_count")
                            rule_condition_count = rtv["rule_condition_count"]
                            score = 0
                            if total_records != 0:
                                score = self.scoreCalculator.get_score(total_records, rule_condition_count)
                            check_pass = common_utils.is_check_pass(rtv["threshold"], self.total_row_count, rule_condition_count)
                            self.insert_dq_fields_score(auto_generated_id, constants.DIMENSION_CONSISTENCY, rtv["source"], score, rtv, check_pass, total_records, rule_condition_count)
                        else:
                            LOGGER.info("Skipping calculation: {} for column: {} in table: {}".format(constants.DIMENSION_CONSISTENCY, column, self.table_name)) 
                    except Exception as error:
                        LOGGER.debug(error.args[0])
        if constants.ENABLE_LOCAL_EXECUTION:
            pass
        else:
            dq_fields_definition_df = self.spark.createDataFrame(self.dq_fields_definition)
            dq_fields_definition_df.repartition(1).write.mode("overwrite").csv("dqhi/{}/{}".format("dq_fields_definition", self.table_name))
            fields_score_list_df = self.spark.createDataFrame(self.fields_score_list)
            fields_score_list_df.repartition(1).write.mode("overwrite").csv("dqhi/{}/{}".format("dq_fields_score", self.table_name))
            LOGGER.info("DONE Writing to DataFrame dq_fields_definition_df ")
    
    def insert_dq_fields_score(self, auto_generated_id, dimension, source, score, rtv, check_pass, total_records, rule_condition_count):
        LOGGER.info('insert_dq_fields_score')
        if self.execution == "Baremetal":
            dq_fields_score_query = self.__construct_dq_fields_score_query(auto_generated_id, dimension, source, score, rtv, check_pass, total_records, rule_condition_count)
            self.sai_db_query_executor.execute_query(dq_fields_score_query)
        else :
            dq_fields_score_query = self.__construct_dq_fields_score_query(auto_generated_id, dimension, source, score, rtv, check_pass, total_records, rule_condition_count)
            self.fields_score_list.append(dq_fields_score_query)
            LOGGER.info(self.fields_score_list)
            
    def __construct_dq_fields_score_query(self, auto_generated_id, dimension, source, score, rtv, check_pass, total_records, rule_condition_count):
        custom_expression = rtv.get("custom_expression")
        if custom_expression == "NONE" :
            custom_expression = ""
            
        if self.execution == "Baremetal":
            dq_fields_score_query = constants.DQ_FIELDS_SCORE_QUERY.format(field_def_id=auto_generated_id,
                                                   dimension=dimension,
                                                   ruleid=rtv["rule_id"],
                                                   rule_parameter=rtv["rule_parameter"],
                                                   bad_record_count=rule_condition_count,
                                                   source=source,
                                                   score=score,
                                                   weight=rtv["weight"],
                                                   threshold=rtv["threshold"],
                                                   check_pass=check_pass,
                                                   start_time=rtv["start_time"],
                                                   end_time=rtv["end_time"],
                                                   rule_condition=custom_expression,
                                                   total_row_count=total_records
                                                   )
            LOGGER.debug("Query to insert into sairepo.dq_fields_score table : {} ".format(dq_fields_score_query))
        else:
            dq_fields_score_query = (auto_generated_id,
                                     dimension,
                                     rtv["rule_id"],
                                     rtv["rule_parameter"],
                                     rule_condition_count,
                                     source,
                                     score,
                                     float(rtv["weight"]),
                                     float(rtv["threshold"]),
                                     check_pass,
                                     rtv["start_time"],
                                     rtv["end_time"],
                                     custom_expression,
                                     total_records)
        return dq_fields_score_query
    
    def fetch_auto_generated_id(self):
        if self.execution == "Baremetal":
            dq_fields_definition_query = constants.DQ_FIELDS_DEFINITION_INSERT_QUERY.format(table_name=self.table_name, \
                                                                                      table_id=self.table_id, \
                                                                                      column_name=self.column_name, \
                                                                                      column_id=self.column_id, \
                                                                                      computed_date=self.computed_date, \
                                                                                      column_precision=self.column_precision, \
                                                                                      column_datatype=self.column_datatype)
            LOGGER.debug("Query to insert for dq_fields_definition table : {} ".format(dq_fields_definition_query))
            
            self.sai_db_query_executor.execute_query(dq_fields_definition_query)
            
            auto_generated_id = self.sai_db_query_executor.fetch_dqhi_columns_details(constants.DQ_FIELDS_DEFINITION_MAX_INSERTED_QUERY)[0]

        else:
            dq_fields_definition_values = (self.table_name, self.table_id, self.column_name, self.column_id, self.computed_date, self.column_precision, self.column_datatype)
            self.dq_fields_definition.append(dq_fields_definition_values)
            self.auto_generated_id = self.auto_generated_id + 1
            auto_generated_id = self.auto_generated_id
            
        return auto_generated_id
    
    def get_dtype(self, colname):
        return [dtype for name, dtype in self.df.dtypes if name == colname][0]

    def get_counter(self):
        return self.auto_generated_id

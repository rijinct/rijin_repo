from com.rijin.dqhi import constants


def construct_fields_insert_query(query_params):
    return constants.DQ_FIELDS_DEFINITION_INSERT_QUERY.format(
                                                                table_name=query_params["table_name"], \
                                                                table_id=query_params["table_id"], \
                                                                column_name=query_params["column_name"], \
                                                                column_id=query_params["column_id"], \
                                                                computed_date=query_params["computed_date"], \
                                                                total_row_count=query_params["total_row_count"], \
                                                                column_precision=query_params["column_precision"], \
                                                                column_datatype=query_params["column_datatype"]
                                                            )

    
def construct_dq_fields_score_query(query_params):
    return constants.DQ_FIELDS_SCORE_QUERY.format(
                                                field_def_id=query_params["auto_generated_id"],
                                                dimension=query_params["dimension"],
                                                ruleid=query_params["rule_id"],
                                                rule_parameter=query_params["rule_parameter"],
                                                bad_record_count=query_params["bad_record_count"],
                                                source=query_params["source"],
                                                score=query_params["score"],
                                                weight=query_params["weight"],
                                                threshold=query_params["threshold"],
                                                check_pass=query_params["check_pass"],
                                                start_time=query_params["start_time"],
                                                end_time=query_params["end_time"]
                                               )

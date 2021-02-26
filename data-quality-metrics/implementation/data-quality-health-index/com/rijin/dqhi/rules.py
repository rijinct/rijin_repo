from pyspark.sql.functions import col


def filter_default_excludes(df, column, default_excludes=""):
    exclude_fields = list(default_excludes.split("|"))
    good_records_df = df.filter(~df[column].isin(exclude_fields))
    return good_records_df;


def get_custom_expression(df, custom_expression):
    custom_expression = custom_expression + " as expr"
    exprDF = df.selectExpr("*", custom_expression).filter(col("expr") == True)
    return exprDF


def is_null_check(df, column, custom_expression, default_excludes=""):
    computed_counts = {}
    
    exclude_fields = list(default_excludes.split("|"))
    good_records_df = df
    total_count = 0
    rule_condition_count = 0
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        if(good_records_df.count != 0) :
            total_count = good_records_df.count()
        
    else:
        total_count = good_records_df.count()
    
    if(good_records_df.count != 0) :    
        rule_condition_count = total_count - good_records_df.filter(~good_records_df[column].isin(exclude_fields)).count()
    
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_equal_to_string_check(df, column, param_value, custom_expression):
    computed_counts = {}
    if custom_expression != "NONE":
        exprDF = get_custom_expression(df, custom_expression)
        expression_count = exprDF.count()
        rule_condition_count = exprDF.where(df[column] == param_value).count()
        computed_counts["rule_condition_count"] = rule_condition_count
        computed_counts["expression_count"] = expression_count      
    else:
        rule_condition_count = df.where(df[column] == param_value).count()
         
        computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_equal_to_int_check(df, column, param_value, custom_expression):
    computed_counts = {}
    if custom_expression != "NONE":
        exprDF = get_custom_expression(df, custom_expression)
        expression_count = exprDF.count()
        rule_condition_count = exprDF.where(df[column] == param_value).count()
        computed_counts["rule_condition_count"] = rule_condition_count
        computed_counts["expression_count"] = expression_count
    else:
        rule_condition_count = df.where(df[column] == param_value).count()
        
        computed_counts["rule_condition_count"] = rule_condition_count           
    return computed_counts


def is_unique_check(df, column, custom_expression, default_excludes=""):
    computed_counts = {}
    good_records_df = filter_default_excludes(df, column, default_excludes)
    
    total_count = 0
    rule_condition_count = 0
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(df, custom_expression)
        total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
    
    rule_condition_count = good_records_df.select(column).distinct().count()
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_greater_than_check(df, column, param_value, custom_expression, default_excludes=""):
    computed_counts = {}
    good_records_df = df
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
        
    rule_condition_count = good_records_df.where(good_records_df[column] > param_value).count()
        
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_less_than_check(df, column, param_value, custom_expression, default_excludes=""):
    computed_counts = {}
    good_records_df = df
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
        
    rule_condition_count = good_records_df.where(good_records_df[column] < param_value).count()
        
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_between_values(df, column, param_value, custom_expression, default_excludes=""):
    fields = param_value.replace("(", "").replace(")", "").strip().split("to")
    computed_counts = {}
    good_records_df = df
    total_count = 0
    rule_condition_count = 0    
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        if(good_records_df.count != 0) :
            total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
    
    if(good_records_df.count != 0) :
        rule_condition_count = good_records_df.filter(good_records_df[column].between(fields[0].strip(), fields[1].strip())).count()
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_pattern_check(df, column, param_value, custom_expression, default_excludes=""):
    computed_counts = {}
    good_records_df = df
    total_count = 0
    rule_condition_count = 0    
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        if(good_records_df.count != 0) :
            total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
    
    if(good_records_df.count != 0) :
        rule_condition_count = good_records_df.filter(good_records_df[column].rlike(param_value)).count()    
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_value_aligned(df, column, param_value, custom_expression, default_excludes=""):
    column_names = list(param_value.split(","))
    exclude_fields = list(default_excludes.split("|"))
#     good_records_df = df.filter(~df[column].isin(exclude_fields))
#     
    good_records_df = filter_default_excludes(df, column, default_excludes)
    computed_counts = {}
    total_count = 0
    rule_condition_count = 0    
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        if(good_records_df.count != 0) :
            total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
        
    for column in column_names:
        good_records_df = good_records_df.filter(~df[column].isin(exclude_fields))
    
    if(good_records_df.count != 0) :
        rule_condition_count = total_count - good_records_df.count();
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = rule_condition_count
    return computed_counts


def is_value_present_in_other_columns(df, column, param_value, custom_expression, default_excludes=""):
    computed_counts = {}
    total_count = 0
    column_names = list(param_value.split(","))
    exclude_fields = list(default_excludes.split("|"))
    good_records_df = df.filter(~df[column].isin(exclude_fields))
    if custom_expression != "NONE":
        good_records_df = get_custom_expression(good_records_df, custom_expression)
        if(good_records_df.count != 0) :
            total_count = good_records_df.count()
    else:
        total_count = good_records_df.count()
    
    null_average_count = 0
    for column in column_names:
        null_average_count += total_count - good_records_df.filter(~df[column].isin(exclude_fields)).count()
    
    result = good_records_df
    for column in column_names:
        result = result.filter(~df[column].isin(exclude_fields))
    
    total_bad_records = total_count - result.count();
    computed_counts["total_count"] = total_count
    computed_counts["rule_condition_count"] = null_average_count / len(column_names)
    computed_counts["bad_record_count"] = total_bad_records
    return computed_counts

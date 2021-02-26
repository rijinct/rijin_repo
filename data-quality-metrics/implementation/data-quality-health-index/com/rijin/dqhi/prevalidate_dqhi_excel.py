import sys
import re
import os
from com.rijin.dqhi.df_excel_util import dfExcelUtil
from com.rijin.dqhi import constants
from com.rijin.dqhi import common_utils

if os.getenv('IS_K8S') == 'true':
    LOGGER = common_utils.get_logger_nova()
else:
    LOGGER = common_utils.get_logger()

validation_result_list = []
kpi_cde_field_list = []
dq_def_field_list = []
duplicate_entries_list = []


def check_rule_param_dtypes(dimension, field_name, field_def_rule_class, field_def_rule_param, pattern, dtypes, row_number):
    if any(x in field_def_rule_class for x in pattern):
        if not isinstance(field_def_rule_param, dtypes):
            validation_result_list.append('field:{field_name}, rule:{field_def_rule_class}, dimension:{dimension}, row_number:{line}'.format(dimension=dimension, field_def_rule_class=field_def_rule_class, field_name=field_name, line=row_number))

            
def check_regex_pattern(dimension, field_name, field_def_rule_class, field_def_rule_param, row_number):
    try:
        re.compile(field_def_rule_param)
        is_valid = True
    except re.error:
        is_valid = False
    if not (is_valid):
        validation_result_list.append('field:{field_name}, rule:{field_def_rule_class}, dimension:{dimension}, row_number:{line}'.format(dimension=dimension, field_def_rule_class=field_def_rule_class, field_name=field_name, line=row_number))


def check_range_and_pattern(dimension, field_name, field_def_rule_class, field_def_rule_param, row_number):
    if field_def_rule_class == 'CHECK_FOR_PATTERN':
        check_regex_pattern(dimension, field_name, field_def_rule_class, field_def_rule_param, row_number)
    if (field_def_rule_class == 'CHECK_FOR_RANGE') and ('to' not in str(field_def_rule_param)):
        validation_result_list.append('field:{field_name}, rule:{field_def_rule_class, dimension:{dimension}, row_number:{line}}'.format(dimension=dimension, field_def_rule_class=field_def_rule_class, field_name=field_name, line=row_number))
        

def validate_dimension(dq_field_def_df, dimension, index, row_number):
    field_name = dq_field_def_df['field_name'][index]
    field_def_rule_class = dq_field_def_df[dimension + '_' + 'rule_name'][index]
    field_def_rule_param = dq_field_def_df[dimension + '_' + 'rule_parameter'][index]
    # check for NaN
    if field_def_rule_class == 'NULL_IN_SOURCE':
        return
    elif field_def_rule_param != field_def_rule_param:
        validation_result_list.append('field:{field_name}, rule:{field_def_rule_class}, dimension:{dimension}, row_number:{line}'.format(dimension=dimension, field_def_rule_class=field_def_rule_class, field_name=field_name, line=row_number))   
    else:    
        check_rule_param_dtypes(dimension, field_name, field_def_rule_class, field_def_rule_param, constants.NUMERIC_PATTERN, constants.NUMERIC_DTYPES, row_number)
        check_rule_param_dtypes(dimension, field_name, field_def_rule_class, field_def_rule_param, constants.STRING_PATTERN, constants.STRING_DTYPE, row_number)
        if 'RANGE' or 'PATTERN' in field_def_rule_class: check_range_and_pattern(dimension, field_name, field_def_rule_class, field_def_rule_param, row_number)


def validate_fields(df, table_col_list, index, file, row_number):
    index_list = [df['field_table_name'][index], df['field_name'][index]]
    if index_list not in table_col_list:
            field_list = 'field:{f}, field_table_name:{f_tbl}, row_number:{line}'.format(f=df['field_name'][index], f_tbl=df['field_table_name'][index], line=row_number)
            if 'KPI' in file:
                kpi_cde_field_list.append(field_list)
            else:
                dq_def_field_list.append(field_list)

            
def iterate_rows(dq_field_def_df, table_col_list):
    dq_field_def_df.reset_index(inplace=True)
    for index in dq_field_def_df.index:
        row_number = index + 6
        validate_fields(dq_field_def_df, table_col_list, index, constants.DQ_RULES_AND_FIELD_DEFINITIONS_FILE, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_COMPLETENESS.lower(), index, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_CONFORMITY.lower(), index, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_UNIQUENESS.lower(), index, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_INTEGRITY.lower(), index, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_RANGE.lower(), index, row_number)
        validate_dimension(dq_field_def_df, constants.DIMENSION_CONSISTENCY.lower(), index, row_number)


def iterate_kpi_cde_rows(dq_field_def_df, kpi_cde_mapp_df):
    if kpi_cde_mapp_df.empty:
        return
    dq_field_def_df_index_list = list(dq_field_def_df.index)
    dq_field_def_df_index_list_conv = list(map(list, dq_field_def_df_index_list))
    for index in kpi_cde_mapp_df.index:
        row_number = index + 5
        validate_fields(kpi_cde_mapp_df, dq_field_def_df_index_list_conv, index, constants.KPI_CDE_FILE_NAME, row_number)            


def log_validation_result():
    if len(validation_result_list) == 0 and len(kpi_cde_field_list) == 0 and len(dq_def_field_list) == 0 and len(duplicate_entries_list) == 0:
        LOGGER.info("Pre-validation of excel columns completed successfully")
    else:
        if (validation_result_list):
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Excel pre-validation failed for DQ_RULES_AND_FIELD_DEFINITIONS.xlsx')
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Mandatory rule parameter is Missing/Incorrect for the following field, rule and dimension in DQ_RULES_AND_FIELD_DEFINITIONS.xlsx. Please correct it and rerun!')
            LOGGER.error(validation_result_list)
        if (kpi_cde_field_list):
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Excel pre-validation failed for KPI_CDE_MAPPING.xlsx')
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Entered fields {} are not present in the source file:DQ_RULES_AND_FIELD_DEFINITIONS.xlsx. Please correct it and re-run!'.format(kpi_cde_field_list))
        if (dq_def_field_list):
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Excel pre-validation failed for DQ_RULES_AND_FIELD_DEFINITIONS.xlsx')
            LOGGER.error('-----------------------------------------------------------------------------------')
            LOGGER.error('Below entered fields are not present in the DB. Please correct it and re-run!')
            LOGGER.error(dq_def_field_list)
        if (duplicate_entries_list):
            for duplicate_entry in duplicate_entries_list:
                LOGGER.error(duplicate_entry)
        sys.exit(1)

def check_for_duplicate_entries(dq_field_def_df):
    temp_dict = {}
    for index in dq_field_def_df.index:
        key = index[0]+"-"+index[1]
        value = index[1]
        is_key_present = temp_dict.get(key,False)
        if is_key_present:
            duplicate_entries_list.append("Duplicate entry found for table: {} with column: {}".format(key.split("-")[0],value))
        else :
            temp_dict[key] =  value
    
              
def execute_prevalidation (table_column_list,dq_field_def_df):
    if constants.ENABLE_LOCAL_EXECUTION is True:
        kpi_cde_mapp_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/test-data/excel/KPI_CDE_MAPPING.xlsx"))
    else:
        kpi_cde_mapp_file = common_utils.get_custom_default_filename(constants.DQHI_CUSTOM_CONF + '/' + constants.KPI_CDE_FILE_NAME, constants.DQHI_CONF + '/' + constants.KPI_CDE_FILE_NAME)
    
    df_excel_util_kpi_cde_mapp = dfExcelUtil(kpi_cde_mapp_file, 3, constants.KPI_CDE_SHEET_NAME)
    df_excel_util_kpi_cde_mapp.lowercase_df_column(constants.KPI_CDE_LOWERCASE_FIELD_NAMES)
    kpi_cde_mapp_df = df_excel_util_kpi_cde_mapp.get_df_excel()
    check_for_duplicate_entries(dq_field_def_df)
    iterate_kpi_cde_rows(dq_field_def_df, kpi_cde_mapp_df)
    iterate_rows(dq_field_def_df, table_column_list)
    log_validation_result()

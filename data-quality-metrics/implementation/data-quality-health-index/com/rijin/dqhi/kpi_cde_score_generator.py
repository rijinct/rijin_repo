from datetime import datetime
import os
import sys
import traceback
sys.path.insert(0,'/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi')
from com.rijin.dqhi import common_utils
from com.rijin.dqhi import constants
from com.rijin.dqhi import date_utils
from com.rijin.dqhi.df_db_util import dfDbUtil
from com.rijin.dqhi.df_excel_util import dfExcelUtil
import numpy as np
import pandas as pd
import constants

if os.getenv('IS_K8S') == 'true':
    LOGGER = common_utils.get_logger_nova()
else:
    LOGGER = common_utils.get_logger()
currentDate = datetime.now()


def construct_kpi_cde_df():
    global cde_list, df_kpi_cde_map,df_kpi_cde_final
    df_kpi_cde_map = df_excel_util_obj.get_df_excel()
    if df_kpi_cde_map.empty:
        LOGGER.error("KPI_CDE_MAPPING file is empty. Hence, exiting")
        exit(0)
    cde_list = list(df_excel_util_obj.get_unique_df_col('field_name'))
    df_kpi_def_db = db_util_obj.get_sql_result_df(constants.DQ_KPI_DEF_QUERY)
    df_kpi_cde_map['kpi_kqi_universe_name']=df_kpi_cde_map[constants.DQ_KPI_DEF_GRP_BY_FIELDS].astype(str).apply(lambda x: '_'.join(x), axis = 1)
    df_kpi_cde_map_kpiName=pd.DataFrame(df_kpi_cde_map.kpi_kqi_universe_name.drop_duplicates())
    df_kpi_def_db_kpiName=pd.DataFrame(df_kpi_def_db['kpi_kqi_universe_name'])
    df_kpi_cde_delta=df_kpi_cde_map_kpiName[~df_kpi_cde_map_kpiName.apply(tuple,1).isin(df_kpi_def_db_kpiName.apply(tuple,1))]
    df_kpi_cde_delta.columns=['kpi_kqi_name']
    if df_kpi_cde_delta.empty:
        LOGGER.info('kpi_kqi_names are already present in DQ_KPI_DEFINITION table.')
    else:
        if os.getenv('IS_K8S') == 'true':
            db_util_obj.insert_db_df(df_kpi_cde_delta, 'dq_kpi_definition', None, 'append', False)
        else:
            db_util_obj.insert_db_df(df_kpi_cde_delta, 'dq_kpi_definition', 'sairepo', 'append', False)
    df_kpi_def_db = db_util_obj.get_sql_result_df(constants.DQ_KPI_DEF_QUERY)
    df_kpi_cde_with_id = common_utils.get_df_merged_result(df_kpi_cde_map, df_kpi_def_db, 'inner', 'kpi_kqi_universe_name')
    df_kpi_cde_final = df_kpi_cde_with_id[constants.DQ_KPI_CDE_EXCEL_FIELD_NAMES]
    LOGGER.info("Successfully read the KPI_CDE mapping file")


def construct_df_db():
    global df_db
    finalQuery = constants.DQHI_SCORES_QUERY.format(column=cde_list, compute_date=compute_date).replace(']', '').replace('[', '')
    try:
        df_db = db_util_obj.get_sql_result_df(finalQuery)
        LOGGER.debug("sql query is: {query}".format(query=finalQuery))
    except:
        LOGGER.error("Error executing query")
        LOGGER.error(traceback.format_exc())  
    
    if (df_db.empty):
        LOGGER.info("No data in the database. Hence, exiting!")
        sys.exit(1)    

    
def kpi_cde_score_generator():
    global df_kpi_cde_score
    join_columns = ['field_name', 'field_table_name']
    df_kpi_cde = common_utils.get_df_merged_result(df_kpi_cde_final, df_db, 'inner', join_columns)
    df_kpi_cde['overall_score'] = ((df_kpi_cde[constants.DQ_DIM_SCORES_WEIGHTED_COL_NAMES].replace(-1, np.nan).sum(axis=1)) / (df_kpi_cde[constants.DQ_DIM_WEIGHT_COL_NAMES].replace(-1, np.nan).sum(axis=1))).astype(int).round()
    df_kpi_cde['object_type'] = 'field'
    df_cde_score = df_kpi_cde[constants.CDE_SCORE_COL_NAMES]
    df_cde_score.rename(columns={"field_name":"kpi_or_cde_name"}, inplace=True)
    df_cde_score.rename(columns={"field_description":"kpi_or_cde_description"}, inplace=True)
    df_cde_score_final = df_cde_score[constants.KPI_CDE_COL_NAMES]
    df_kpi_description = df_kpi_cde[constants.DQ_KPI_DESC_DF_COL].groupby(constants.DQ_KPI_CDE_DESC_JOIN_COL)[constants.DQ_KPI_DESC_DF_COL[2]].first().reset_index()
    df_kpi_score_avg = df_kpi_cde.groupby(constants.DQ_KPI_CDE_DESC_JOIN_COL).mean().astype(int).reset_index().round()
    df_kpi_score = common_utils.get_df_merged_result(df_kpi_score_avg, df_kpi_description, 'inner', constants.DQ_KPI_CDE_DESC_JOIN_COL)
    df_kpi_score['object_type'] = 'KPI'
    df_kpi_score.rename(columns={'kpi_kqi_name':'kpi_or_cde_name'}, inplace=True)
    df_kpi_score.rename(columns={'kpi_kqi_description':'kpi_or_cde_description'}, inplace=True)
    df_kpi_score_final = df_kpi_score[constants.KPI_SCORE_COL_NAMES]
    df_kpi_score_final.rename(columns={'parent_object_id':'object_id'}, inplace=True)
    df_kpi_score_final.loc[:,constants.DQ_DIM_SCORES_COL_NAMES] = -1
    df_kpi_cde_score = pd.concat([df_kpi_score_final, df_cde_score_final], sort=False)
    df_kpi_cde_score = df_kpi_cde_score.fillna(-1)
    df_kpi_cde_score[constants.DQ_DIM_SCORES_COL_NAMES] = df_kpi_cde_score[constants.DQ_DIM_SCORES_COL_NAMES].astype(int)
    df_kpi_cde_score['computed_date'] = compute_date
    LOGGER.info("Successfully computed KPI and CDE score")
    
    
def populate_kpi_cde_score():
    df_kpi_cde_values = df_kpi_cde_score[constants.KPI_CDE_COL_FINAL]
#     df_kpi_cde_values.assign(load_date = currentDate)
    df_kpi_cde_values['load_date'] = currentDate
    try:
        if os.getenv('IS_K8S') == 'true':
            db_util_obj.insert_db_df(df_kpi_cde_values, 'dq_kpi_cde_scores', None, 'append', False)
        else:
            db_util_obj.insert_db_df(df_kpi_cde_values, 'dq_kpi_cde_scores', 'sairepo', 'append', False)
        LOGGER.info("Successfully inserted the scores in the DB")
    except:
        LOGGER.error("Error in inserting the data to DB")
        LOGGER.error(traceback.format_exc())   


def validateArguments(args):
    global compute_date
    if len(args) == 1:
        LOGGER.info("No Date argument provided. Hence, considering previous day as computed date!")
        compute_date = date_utils.get_previous_date()
    else:
        compute_date = sys.argv[1]


def main(argv=sys.argv):
    validateArguments(argv)
    if constants.ENABLE_LOCAL_EXECUTION is True:
        kpi_cde_fileName = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/test-data/excel/KPI_CDE_MAPPING.xlsx"))
    else:
        kpi_cde_fileName = common_utils.get_custom_default_filename(constants.DQHI_CUSTOM_CONF + '/' + constants.KPI_CDE_FILE_NAME, constants.DQHI_CONF + '/' + constants.KPI_CDE_FILE_NAME)
    global df_excel_util_obj, db_util_obj
    df_excel_util_obj = dfExcelUtil(kpi_cde_fileName, 3, constants.KPI_CDE_SHEET_NAME)
    df_excel_util_obj.lowercase_df_column(constants.KPI_CDE_LOWERCASE_FIELD_NAMES)
    db_util_obj = dfDbUtil()
    construct_kpi_cde_df()
    construct_df_db()
    kpi_cde_score_generator()
    populate_kpi_cde_score()


if __name__ == "__main__":
    main()

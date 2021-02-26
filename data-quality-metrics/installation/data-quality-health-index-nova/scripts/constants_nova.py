'''
Created on 21-Nov-2020

@author: rithomas
'''

HADOOP_CONF = '/etc/hadoop/conf/'

DQ_FIELDS_DEFINITION_COL = ['table_name', 'table_id', 'column_name', 'column_id', 'computed_date', 'column_precision', 'column_datatype']

DQ_FIELDS_SCORE_COL = ['field_def_id', 'dimension', 'ruleid', 'rule_parameter', 'bad_record_count', 'source', 'score', 'weight', 'threshold', 'check_pass', 'start_time', 'end_time', 'rule_condition','total_row_count']

TRUNCATE_DQ_DEFINITION_QUERY = 'delete from dq_fields_definition'

TRUNCATE_DQ_SCORES_QUERY = 'delete from dq_fields_score'

TRUNCATE_KPI_CDE_QUERY = 'delete from dq_kpi_cde_scores'

DELETE_SQLITE_SEQ_QUERY = '''delete from sqlite_sequence where name='dq_fields_definition'
'''

DQHI_HOME = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index'

DQ_HDFS_FILES_LOCAL = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/hdfs_files'

DQ_HDFS_FILES_CDLK = 'hdfs://namenodeHA/user/ngdb/dqhi'

DQ_FIELDS_SCORE_TBL = 'dq_fields_score'

DQ_FIELDS_DEF_TBL = 'dq_fields_definition'

DQ_SCHEMA_LOC = '{}/sql/schema'.format(DQHI_HOME)

COMPUTED_TBL_QUERY = '''
select table_name from dq_fields_definition where strftime('%Y-%m-%d', computed_date)='{}'
'''
import os

DIMENSION_COMPLETENESS = "COMPLETENESS"
DIMENSION_CONFORMITY = "CONFORMITY"
DIMENSION_UNIQUENESS = "UNIQUENESS"
DIMENSION_RANGE = "RANGE"
DIMENSION_INTEGRITY = "INTEGRITY"
DIMENSION_CONSISTENCY = "CONSISTENCY"

SOURCE_CEM = "CEM"
SOURCE_PRODUCT = "PRODUCT"
SOURCE_CUSTOM = "CUSTOM"

ENABLE_LOCAL_EXECUTION = bool(os.getenv('LOCAL_EXECUTION_ENABLE', False))

PROCESS_MONITOR_HDFS_PATH = "/ngdb/common/process_monitor/"

HIVE_TABLE_WITH_LOCATION_QUERY = None 
if ENABLE_LOCAL_EXECUTION is True:
    HIVE_TABLE_WITH_LOCATION_QUERY = "SELECT table_name, table_location from hive_meta_data"
else:
    HIVE_TABLE_WITH_LOCATION_QUERY = "SELECT TBL_NAME,LOCATION FROM SDS INNER JOIN TBLS ON TBLS.SD_ID = SDS.SD_ID where (TBLS.TBL_NAME like 'us_%' or TBLS.TBL_NAME like 'es_%') and TBLS.TBL_NAME not like '%_arc%';"
    
LOG_FILE_NAME = "score_generator_{}"

if os.getenv('IS_K8S') == "true":
    DQHI_CONF = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf'
    DQHI_CUSTOM_CONF = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/conf/custom'
else:
    DQHI_CONF = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/conf'
    DQHI_CUSTOM_CONF = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/conf/custom'

if ENABLE_LOCAL_EXECUTION is True:
    DQHI_SCORES_QUERY = """
    select column_id as object_id, column_name as field_name, table_name as field_table_name,max(completeness_score) as completeness_score, max(completeness_weighted_score) as completeness_weighted_score ,max(completeness_weight) as completeness_weight, max(uniqueness_weighted_score) as uniqueness_weighted_score,max(uniqueness_score) as uniqueness_score, max(uniqueness_weight) as uniqueness_weight, max(conformance_weighted_score) as conformance_weighted_score, max(conformance_score) as conformance_score, max(conformance_weight) as conformance_weight, max(integrity_weighted_score) as integrity_weighted_score, max(integrity_score) as integrity_score, max(integrity_weight) as integrity_weight, max(range_weighted_score) as range_weighted_score, max(range_score) as range_score, max(range_weight) as range_weight, max(consistence_weighted_score) as consistence_weighted_score, max(consistence_score) as consistence_score, max(consistence_weight) as consistence_weight from (select source,column_id,column_name,table_name,dimension,computed_date,((case when (dimension = 'UNIQUENESS' and weight != 0) then score * weight else -1 end)) as uniqueness_weighted_score, ((case when (dimension = 'UNIQUENESS' and weight != 0) then score else -1 end)) as uniqueness_score, ((case when dimension = 'UNIQUENESS' then weight else -1 end)) as uniqueness_weight, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score * weight else -1 end))  as consistence_weighted_score, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score else -1 end)) as consistence_score, ((case when dimension = 'CONSISTENCY' then weight else -1 end)) as consistence_weight, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score * weight else -1 end)) as completeness_weighted_score, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score else -1 end)) as completeness_score, ((case when dimension = 'COMPLETENESS' then weight else -1 end)) as completeness_weight, ((case when (dimension = 'CONFORMITY' and weight != 0) then score * weight else -1 end)) as conformance_weighted_score, ((case when (dimension = 'CONFORMITY' and weight != 0) then score else -1 end)) as conformance_score, ((case when dimension = 'CONFORMITY' then weight else -1 end)) as conformance_weight, ((case when (dimension = 'INTEGRITY' and weight != 0) then score * weight else -1 end)) as integrity_weighted_score, ((case when (dimension = 'INTEGRITY' and weight != 0) then score else -1 end)) as integrity_score, ((case when dimension = 'INTEGRITY' then weight else -1 end)) as integrity_weight, ((case when (dimension = 'RANGE' and weight != 0) then score * weight else -1 end)) as range_weighted_score, ((case when (dimension = 'RANGE' and weight != 0) then score else -1 end)) as range_score, ((case when dimension = 'RANGE' then weight else -1 end)) as range_weight from dq_fields_score inner join dq_fields_definition on dq_fields_score.field_def_id=dq_fields_definition.id and id in (select max(id) from dq_fields_definition group by table_name,table_id,column_name,column_id)) q1 where column_name in  ('equip_id', 'client_device', 'client_device_type', 'domain', 'technology_id') and computed_date='2017-02-05' and source != 'PRODUCT' group by object_id,field_name,field_table_name order by object_id
     """
elif os.getenv('IS_K8S') == "true":
    DQHI_SCORES_QUERY = """
     select column_id as object_id, column_name as field_name, table_name as field_table_name,max(completeness_score) as completeness_score, max(completeness_weighted_score) as completeness_weighted_score ,max(completeness_weight) as completeness_weight, max(uniqueness_weighted_score) as uniqueness_weighted_score,max(uniqueness_score) as uniqueness_score, max(uniqueness_weight) as uniqueness_weight, max(conformance_weighted_score) as conformance_weighted_score, max(conformance_score) as conformance_score, max(conformance_weight) as conformance_weight, max(integrity_weighted_score) as integrity_weighted_score, max(integrity_score) as integrity_score, max(integrity_weight) as integrity_weight, max(range_weighted_score) as range_weighted_score, max(range_score) as range_score, max(range_weight) as range_weight, max(consistence_weighted_score) as consistence_weighted_score, max(consistence_score) as consistence_score, max(consistence_weight) as consistence_weight from (select source,column_id,column_name,table_name,dimension,computed_date,((case when (dimension = 'UNIQUENESS' and weight != 0) then score * weight else -1 end)) as uniqueness_weighted_score, ((case when (dimension = 'UNIQUENESS' and weight != 0) then score else -1 end)) as uniqueness_score, ((case when dimension = 'UNIQUENESS' then weight else -1 end)) as uniqueness_weight, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score * weight else -1 end))  as consistence_weighted_score, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score else -1 end)) as consistence_score, ((case when dimension = 'CONSISTENCY' then weight else -1 end)) as consistence_weight, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score * weight else -1 end)) as completeness_weighted_score, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score else -1 end)) as completeness_score, ((case when dimension = 'COMPLETENESS' then weight else -1 end)) as completeness_weight, ((case when (dimension = 'CONFORMITY' and weight != 0) then score * weight else -1 end)) as conformance_weighted_score, ((case when (dimension = 'CONFORMITY' and weight != 0) then score else -1 end)) as conformance_score, ((case when dimension = 'CONFORMITY' then weight else -1 end)) as conformance_weight, ((case when (dimension = 'INTEGRITY' and weight != 0) then score * weight else -1 end)) as integrity_weighted_score, ((case when (dimension = 'INTEGRITY' and weight != 0) then score else -1 end)) as integrity_score, ((case when dimension = 'INTEGRITY' then weight else -1 end)) as integrity_weight, ((case when (dimension = 'RANGE' and weight != 0) then score * weight else -1 end)) as range_weighted_score, ((case when (dimension = 'RANGE' and weight != 0) then score else -1 end)) as range_score, ((case when dimension = 'RANGE' then weight else -1 end)) as range_weight from dq_fields_score inner join dq_fields_definition on dq_fields_score.field_def_id=dq_fields_definition.id and id in (select max(id) from dq_fields_definition group by table_name,table_id,column_name,column_id)) q1 where column_name in  ({column}) and strftime('%Y-%m-%d',computed_date)=\'{compute_date}\' and source <> 'PRODUCT' group by object_id,field_name,field_table_name order by object_id 
     """
else:
    DQHI_SCORES_QUERY = """
     select column_id as object_id, column_name as field_name, table_name as field_table_name,max(completeness_score) as completeness_score, max(completeness_weighted_score) as completeness_weighted_score ,max(completeness_weight) as completeness_weight, max(uniqueness_weighted_score) as uniqueness_weighted_score,max(uniqueness_score) as uniqueness_score, max(uniqueness_weight) as uniqueness_weight, max(conformance_weighted_score) as conformance_weighted_score, max(conformance_score) as conformance_score, max(conformance_weight) as conformance_weight, max(integrity_weighted_score) as integrity_weighted_score, max(integrity_score) as integrity_score, max(integrity_weight) as integrity_weight, max(range_weighted_score) as range_weighted_score, max(range_score) as range_score, max(range_weight) as range_weight, max(consistence_weighted_score) as consistence_weighted_score, max(consistence_score) as consistence_score, max(consistence_weight) as consistence_weight from (select source,column_id,column_name,table_name,dimension,computed_date,((case when (dimension = 'UNIQUENESS' and weight != 0) then score * weight else -1 end)) as uniqueness_weighted_score, ((case when (dimension = 'UNIQUENESS' and weight != 0) then score else -1 end)) as uniqueness_score, ((case when dimension = 'UNIQUENESS' then weight else -1 end)) as uniqueness_weight, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score * weight else -1 end))  as consistence_weighted_score, ((case when (dimension = 'CONSISTENCY' and weight != 0) then score else -1 end)) as consistence_score, ((case when dimension = 'CONSISTENCY' then weight else -1 end)) as consistence_weight, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score * weight else -1 end)) as completeness_weighted_score, ((case when (dimension = 'COMPLETENESS' and weight != 0) then score else -1 end)) as completeness_score, ((case when dimension = 'COMPLETENESS' then weight else -1 end)) as completeness_weight, ((case when (dimension = 'CONFORMITY' and weight != 0) then score * weight else -1 end)) as conformance_weighted_score, ((case when (dimension = 'CONFORMITY' and weight != 0) then score else -1 end)) as conformance_score, ((case when dimension = 'CONFORMITY' then weight else -1 end)) as conformance_weight, ((case when (dimension = 'INTEGRITY' and weight != 0) then score * weight else -1 end)) as integrity_weighted_score, ((case when (dimension = 'INTEGRITY' and weight != 0) then score else -1 end)) as integrity_score, ((case when dimension = 'INTEGRITY' then weight else -1 end)) as integrity_weight, ((case when (dimension = 'RANGE' and weight != 0) then score * weight else -1 end)) as range_weighted_score, ((case when (dimension = 'RANGE' and weight != 0) then score else -1 end)) as range_score, ((case when dimension = 'RANGE' then weight else -1 end)) as range_weight from sairepo.dq_fields_score inner join sairepo.dq_fields_definition on dq_fields_score.field_def_id=dq_fields_definition.id and id in (select max(id) from sairepo.dq_fields_definition group by table_name,table_id,column_name,column_id)) q1 where column_name in  ({column}) and computed_date=\'{compute_date}\' and source <> 'PRODUCT' group by object_id,field_name,field_table_name order by object_id 
     """

KPI_CDE_FILE_NAME = 'KPI_CDE_MAPPING.xlsx'

DQ_RULES_AND_FIELD_DEFINITIONS_FILE = 'DQ_RULES_AND_FIELD_DEFINITIONS.xlsx'

if ENABLE_LOCAL_EXECUTION is True:
    PRE_COMPUTED_SCORE_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test/pre_computed_score/PreComputedScore.xml"))
else:
    PRE_COMPUTED_SCORE_FILE_NAME = DQHI_CONF + '/' + "PreComputedScore.xml"
    
DQ_RULES_DEFINITION_SHEET = 'dq_rules_definition'

if (ENABLE_LOCAL_EXECUTION is True or os.getenv('IS_K8S') == "true"):
    RULES_TABLE_QUERY = "select rule_id, rule_class,default_value from dqhi_rules where dimension='{}' and column_datatype='{}' and is_denorm_column='{}';"
else:
    RULES_TABLE_QUERY = "select rule_id, rule_class,default_value from {schema}dqhi_rules where dimension='{}' and column_datatype='{}' and is_denorm_column='{}';"

ADAPTATION_TOPOLOGY_MAPPING_QUERY = "select concat('US_',us.specid,'_1') , concat('',us1.specid,'_1')  from sairepo.usage_spec_rel rel_table join sairepo.usage_spec us on rel_table.virtualspec=us.id join sairepo.usage_spec us1 on rel_table.transientspec=us1.id;"

DQ_FIELDS_DEFINITION_INSERT_QUERY = None

if ENABLE_LOCAL_EXECUTION is True:
    DQ_FIELDS_DEFINITION_INSERT_QUERY = """insert into dq_fields_definition(table_name,table_id,column_name,column_id,computed_date,column_precision,column_datatype) values(
    '{table_name}',{table_id},'{column_name}',{column_id},'{computed_date}','{column_precision}','{column_datatype}'
    )"""
else:
    DQ_FIELDS_DEFINITION_INSERT_QUERY = """insert into sairepo.dq_fields_definition(table_name,table_id,column_name,column_id,computed_date,column_precision,column_datatype) values(
    '{table_name}',{table_id},'{column_name}',{column_id},'{computed_date}','{column_precision}','{column_datatype}'
    )"""

DQ_FIELDS_DEFINITION_MAX_INSERTED_QUERY = None

if ENABLE_LOCAL_EXECUTION is True:
    DQ_FIELDS_DEFINITION_MAX_INSERTED_QUERY = "select max(id) from dq_fields_definition"
else :
    DQ_FIELDS_DEFINITION_MAX_INSERTED_QUERY = "select max(id) from sairepo.dq_fields_definition"


DQ_FIELDS_SCORE_QUERY = None

if ENABLE_LOCAL_EXECUTION is True:
    DQ_FIELDS_SCORE_QUERY = """insert into dq_fields_score values(
    {field_def_id},'{dimension}',{ruleid},'{rule_parameter}',{bad_record_count},'{source}',{score},{weight},{threshold},'{check_pass}',
    '{start_time}','{end_time}',"{rule_condition}",{total_row_count}
    )"""
else:
    DQ_FIELDS_SCORE_QUERY = """insert into sairepo.dq_fields_score values(
    {field_def_id},'{dimension}',{ruleid},'{rule_parameter}',{bad_record_count},'{source}',{score},{weight},{threshold},'{check_pass}',
    '{start_time}','{end_time}','{rule_condition}',{total_row_count}
    )"""
    
QUERY_USAGE_TABLEID_COLUMNID = """
select lower(TableName),table_id,lower(ColumnName),concat(table_id,'',column_id) as column_id,datatype,precision from (select row_number() over(partition by
 concat(tablename,'_',ColumnName)) as
 rank,TableName,table_id,ColumnName,column_id,datatype,precision  from (select 'US_' || n6.specid|| '_1' as
 TableName,200000+n5.usagespecid as table_id, n5.ColumnName as ColumnName, sequence as column_id,valuetype as datatype,
 valuetypeprecision as precision from  ((select specid,id from sairepo.usage_spec)n6 join 
(select case when n4.usagespecid is NULL then min(n4.usagespecid) over(partition by n3.usagespecid) else n4.usagespecid
 end as usagespecid,n3.sequence as sequence,n3.name as ColumnName, n3.valuetype, n3.valuetypeprecision from (select
 n1.usagespecid as usagespecid,n1.sequence as sequence,n1.name as
 name,n1.valuetype,valuetypeprecision,n2.specid,concat(n2.specid,'_',n1.name) as specidColumnName from (select
 usage_spec_char_use.usagespecid as usagespecid,usage_spec_char_use.sequence,char_spec.id as id,char_spec.name,
 char_spec.valuetype, char_spec.valuetypeprecision from sairepo.char_spec join sairepo.usage_spec_char_use on
 char_spec.id=usage_spec_char_use.usagespeccharid)n1 join (select specid,max(id) as id from sairepo.usage_spec where
 (abstract='DERIVED' or abstract is null) group by specid)n2 on n1.usagespecid=n2.id)n3 left join (select
 n1.usagespecid,n1.sequence,n1.name,n1.valuetype,valuetypeprecision,n2.specid,concat(n2.specid,'_',n1.name) as
 specidColumnName from (select usage_spec_char_use.usagespecid as
 usagespecid,usage_spec_char_use.sequence,char_spec.id,char_spec.name, char_spec.valuetype, char_spec.valuetypeprecision
 from sairepo.char_spec join sairepo.usage_spec_char_use on char_spec.id=usage_spec_char_use.usagespeccharid)n1 join
 (select specid,min(id) as id from sairepo.usage_spec where (abstract='DERIVED' or abstract is null) group by specid)n2 on
 n1.usagespecid=n2.id)n4 on n3.specidColumnName=n4.specidColumnName)n5 on n5.usagespecid=n6.id) order by
 n5.usagespecid,sequence)n7 order by table_id,column_id)n8 where rank=1
"""

QUERY_ENTITY_TABLEID_COLUMNID = """
select lower(TableName),table_id,lower(ColumnName),concat(table_id,'',column_id) as column_id,datatype,precision from (select row_number() over(partition by
 concat(tablename,'_',ColumnName)) as
 rank,TableName,table_id,ColumnName,column_id,datatype,precision  from (select 'ES_' || n6.specid|| '_1' as
 TableName,100000+n5.entityspecid as table_id, n5.ColumnName as ColumnName, sequence as column_id,valuetype as datatype,
 valuetypeprecision as precision from  ((select specid,id from sairepo.entity_spec)n6 join 
(select case when n4.entityspecid is NULL then min(n4.entityspecid) over(partition by n3.entityspecid) else
 n4.entityspecid end as entityspecid,n3.sequence as sequence,n3.name as ColumnName, n3.valuetype, n3.valuetypeprecision
 from (select n1.entityspecid as entityspecid,n1.sequence as sequence,n1.name as
 name,n1.valuetype,valuetypeprecision,n2.specid,concat(n2.specid,'_',n1.name) as specidColumnName from (select
 entity_spec_char_use.entityspecid as entityspecid,entity_spec_char_use.sequence,char_spec.id as id,char_spec.name,
 char_spec.valuetype, char_spec.valuetypeprecision from sairepo.char_spec join sairepo.entity_spec_char_use on
 char_spec.id=entity_spec_char_use.entityspeccharid)n1 join (select specid,max(id) as id from sairepo.entity_spec where
 (abstract='DERIVED' or abstract is null) group by specid)n2 on n1.entityspecid=n2.id)n3 left join (select
 n1.entityspecid,n1.sequence,n1.name,n1.valuetype,valuetypeprecision,n2.specid,concat(n2.specid,'_',n1.name) as
 specidColumnName from (select entity_spec_char_use.entityspecid as
 entityspecid,entity_spec_char_use.sequence,char_spec.id,char_spec.name, char_spec.valuetype, char_spec.valuetypeprecision
 from sairepo.char_spec join sairepo.entity_spec_char_use on char_spec.id=entity_spec_char_use.entityspeccharid)n1 join
 (select specid,min(id) as id from sairepo.entity_spec where (abstract='DERIVED' or abstract is null) group by specid)n2
 on n1.entityspecid=n2.id)n4 on n3.specidColumnName=n4.specidColumnName)n5 on n5.entityspecid=n6.id) order by
 n5.entityspecid,sequence)n7 order by table_id,column_id)n8 where rank=1
"""

QUERY_RULE_NAME_RULE_ID = "select rule_presentation_name , rule_id,rule_class from sairepo.DQ_RULES_DEFINITION where rule_class !=''"

if ENABLE_LOCAL_EXECUTION is True or os.getenv('IS_K8S') == 'true':
    QUERY_RULE_NAME_RULE_ID = "select rule_presentation_name , rule_id,rule_class,default_excludes from DQ_RULES_DEFINITION where rule_class !=''"
else:
    QUERY_RULE_NAME_RULE_ID = "select rule_presentation_name , rule_id,rule_class,default_excludes from sairepo.DQ_RULES_DEFINITION where rule_class !=''"

RAW_COLUMN_SUPPORTED_TYPE = ["timestamp", "bigint", "string", "decimal(15,12)", "double"]

DENORM_COLUMN_SUPPORTED_TYPE = ["bigint", "string"]

if os.getenv('IS_K8S') == 'true':
    PYTHON3_HOME = 'python3'
else: 
    PYTHON3_HOME = '/usr/local/bin/python3.7'

PYTHON3_HOME_NOVA = 'python3'

KPI_CDE_SCORE_CALCULATOR = 'com.rijin.dqhi.kpi_cde_score_generator'

if os.getenv('IS_K8S') == 'true':
    SCORE_CALCULATOR = 'com.rijin.dqhi.dqhi_score_generator_client_nova'
else:
    SCORE_CALCULATOR = 'com.rijin.dqhi.dqhi_score_generator_client'

SCORE_CALCULATOR_NOVA = 'com.rijin.dqhi.dqhi_score_generator_client_nova'

DQ_DATA_EXPORTER = 'com.rijin.dqhi.dq_data_exporter'

DQ_DATA_LOADER = 'com.rijin.dqhi.dq_data_loader'

DQ_DB_DATA_DELETER = 'com.rijin.dqhi.dq_db_data_deleter'

DQ_SCORE_CALCULATOR_WRAPPER = 'com.rijin.dqhi.score_calculator_wrapper'

SPARK_HOME = '/opt/cloudera/parcels/SPARK2_WITH_TS'

PYTHONPATH = '/opt/cloudera/parcels/SPARK2_WITH_TS/python'

HADOOP_CONF_DIR = '/etc/hadoop/conf'

CLASSPATH = '/opt/nsn/ngdb/tomcat/webapps/sai-ws/WEB-INF/lib/*:/etc/hadoop/conf'

DQHI_SCRIPTS = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/'

RULE_DEFINITIONS_COLUMN_LIST = ['rule_id', 'dimension', 'rule_presentation_name', 'rule_class', 'default_value','default_excludes','rule_description']
TRUNCATE_RULES_QUERY = 'truncate dq_rules_definition'

TRUNCATE_RULES_QUERY_NOVA = 'delete from dq_rules_definition'

DQ_FIELD_DEF_COL_NAMES = [
    "field_name",
    "field_description",
    "field_table_name",
    "completeness_rule_name",
    "completeness_rule_parameter",
    "completeness_custom_expression",
    "completeness_threshold",
    "completeness_weight",
    "conformity_rule_name",
    "conformity_rule_parameter",
    "conformity_custom_expression",
    "conformity_threshold",
    "conformity_weight",
    "consistency_rule_name",
    "consistency_rule_parameter",
    "consistency_custom_expression",
    "consistency_threshold",
    "consistency_weight",
    "integrity_rule_name",
    "integrity_rule_parameter",
    "integrity_custom_expression", 
    "integrity_threshold", 
    "integrity_weight", 
    "range_rule_name", 
    "range_rule_parameter", 
    "range_custom_expression", 
    "range_threshold", 
    "range_weight", 
    "uniqueness_rule_name", 
    "uniqueness_rule_parameter", 
    "uniqueness_custom_expression", 
    "uniqueness_threshold", 
    "uniqueness_weight"]

DELETE_QUERY = "DELETE FROM sairepo.dq_fields_definition WHERE computed_date < 'COMP_DATE';DELETE FROM sairepo.dq_kpi_cde_scores WHERE computed_date < 'COMP_DATE'"

DQ_LOWERCASE_FIELD_NAMES = ['field_name', 'field_table_name']

KPI_CDE_LOWERCASE_FIELD_NAMES = ['kpi_kqi_name', 'kpi_kqi_description', 'field_name', 'field_description', 'field_table_name']

KPI_CDE_COL_NAMES = ['object_id', 'parent_object_id', 'object_type', 'kpi_or_cde_name', 'kpi_or_cde_description', 'completeness_score', 'uniqueness_score', 'conformance_score', 'integrity_score', 'range_score', 'consistence_score', 'overall_score']

KPI_CDE_COL_FINAL = ['object_id', 'parent_object_id', 'object_type', 'kpi_or_cde_name', 'kpi_or_cde_description', 'overall_score', 'completeness_score', 'uniqueness_score', 'conformance_score', 'integrity_score', 'range_score', 'consistence_score', 'computed_date']

KPI_SCORE_COL_NAMES = ['parent_object_id', 'object_type', 'kpi_or_cde_name', 'kpi_or_cde_description', 'completeness_score', 'uniqueness_score', 'conformance_score', 'integrity_score', 'range_score', 'consistence_score', 'overall_score']

DQ_DIM_SCORES_WEIGHTED_COL_NAMES = ['completeness_weighted_score', 'uniqueness_weighted_score', 'conformance_weighted_score', 'integrity_weighted_score', 'range_weighted_score', 'consistence_weighted_score']

DQ_DIM_SCORES_COL_NAMES = ['completeness_score', 'uniqueness_score', 'conformance_score', 'integrity_score', 'range_score', 'consistence_score']

DQ_DIM_WEIGHT_COL_NAMES = ['completeness_weight','uniqueness_weight','conformance_weight','integrity_weight','range_weight','consistence_weight']

CDE_SCORE_COL_NAMES = ['object_id', 'parent_object_id', 'object_type', 'field_name', 'field_description', 'completeness_score', 'completeness_weight', 'uniqueness_score', 'uniqueness_weight', 'conformance_score', 'conformance_weight', 'integrity_score', 'integrity_weight', 'range_score', 'range_weight', 'consistence_score', 'consistence_weight', 'overall_score']

STATISTICS_QUERY = "select concat(table_name,' : ',Count_or_Stats) as table_stat from (select table_name, CAST(count(table_name) as VARCHAR) as Count_or_Stats from sairepo.dq_fields_definition where computed_date ='DATA_DATE' and table_name in (select distinct table_name from sairepo.dq_fields_definition where computed_date ='DATA_DATE')group by table_name UNION ALL select 'TOTAL TABLES' table_name,CAST(count(distinct table_name) as VARCHAR) from sairepo.dq_fields_definition where computed_date ='DATA_DATE' UNION ALL select 'TOTAL COLUMNS' table_name, CAST(count(table_name) as VARCHAR) from sairepo.dq_fields_definition where computed_date ='DATA_DATE' UNION ALL select 'TOTAL RULES TO BE COMPUTED' table_name, CAST(count(table_name)*6 as VARCHAR) from sairepo.dq_fields_definition where computed_date ='DATA_DATE' UNION ALL select 'TOTAL RULES COMPUTED' table_name, CAST(count(field_def_id) as VARCHAR) from sairepo.dq_fields_score where start_time >'DATA_DATE 23:59:00' UNION ALL select 'TOTAL RULES FAILED' table_name, CAST(count(a.table_name)-count(b.field_def_id) as VARCHAR) from sairepo.dq_fields_definition a, sairepo.dq_fields_score b where a.computed_date ='DATA_DATE' and b.start_time >'TODAY_DATE' UNION ALL select 'START TIME' table_name,CAST(min(start_time) as VARCHAR) from sairepo.dq_fields_score where start_time>'TODAY_DATE' and start_time<'TODAY_DATE 23:59:00' UNION ALL select 'END TIME' table_name, CAST(max(end_time) as VARCHAR) from sairepo.dq_fields_score where start_time>'TODAY_DATE 00:00:00' and start_time<'TODAY_DATE 23:59:00' UNION ALL select 'TOTAL TIME TAKEN' table_name,CAST(max(end_time)-min(start_time) as VARCHAR) from sairepo.dq_fields_score where start_time>'TODAY_DATE' and start_time<'TODAY_DATE 23:59:00') q1"

STATISTICS_QUERY_NOVA = """
select table_name || ' : ' || Count_or_Stats as table_stat from (select table_name, CAST(count(table_name) as VARCHAR) as Count_or_Stats from dq_fields_definition where strftime('%Y-%m-%d',computed_date) ='DATA_DATE' and table_name in (select distinct table_name from dq_fields_definition where strftime('%Y-%m-%d',computed_date) ='DATA_DATE')group by table_name 
UNION ALL 
select 'TOTAL TABLES' table_name,CAST(count(distinct table_name) as VARCHAR) from dq_fields_definition where strftime('%Y-%m-%d',computed_date) ='DATA_DATE' 
UNION ALL 
select 'TOTAL COLUMNS' table_name, CAST(count(table_name) as VARCHAR) from dq_fields_definition where strftime('%Y-%m-%d',computed_date) ='DATA_DATE' 
UNION ALL 
select 'TOTAL RULES TO BE COMPUTED' table_name, CAST(count(table_name)*6 as VARCHAR) from dq_fields_definition where strftime('%Y-%m-%d',computed_date) ='DATA_DATE' 
UNION ALL 
select 'TOTAL RULES COMPUTED' table_name, CAST(count(field_def_id) as VARCHAR) from dq_fields_score where start_time >'DATA_DATE 23:59:00' 
UNION ALL 
select 'TOTAL RULES FAILED' table_name, CAST(count(a.table_name)-count(b.field_def_id) as VARCHAR) from dq_fields_definition a join dq_fields_score b where strftime('%Y-%m-%d',a.computed_date) ='DATA_DATE' and b.start_time >'TODAY_DATE'
UNION ALL 
select 'START TIME' table_name,CAST(min(start_time) as VARCHAR) from dq_fields_score where start_time>'TODAY_DATE' and start_time<'TODAY_DATE 23:59:00' 
UNION ALL 
select 'END TIME' table_name, CAST(max(end_time) as VARCHAR) from dq_fields_score where start_time>'TODAY_DATE 00:00:00' and start_time<'TODAY_DATE 23:59:00' 
UNION ALL 
select 'TOTAL TIME TAKEN' table_name,CAST(max(end_time)-min(start_time) as VARCHAR) from dq_fields_score where start_time>'TODAY_DATE' and start_time<'TODAY_DATE 23:59:00') q1
"""

KPI_CDE_STATS_QUERY = "select kpi_or_cde_name as KPI from sairepo.dq_kpi_cde_scores where object_type='KPI' and computed_date='{}'"

KPI_CDE_STATS_QUERY_NOVA = "select kpi_or_cde_name as KPI from dq_kpi_cde_scores where object_type='KPI' and computed_date='{}'"

if ENABLE_LOCAL_EXECUTION is True:
    USAGE_ENTITY_QUERY = ["select * from USAGE_TABLEID_COLUMNID;", "select * from ENTITY_TABLEID_COLUMNID;"]
else:
    USAGE_ENTITY_QUERY = [QUERY_USAGE_TABLEID_COLUMNID , QUERY_ENTITY_TABLEID_COLUMNID]
    

NUMERIC_PATTERN = ['GREATER_THAN', 'NUMERIC', 'LESS_THAN', 'INT']
        
STRING_PATTERN = ['STRING']

NUMERIC_DTYPES = (float, int)

STRING_DTYPE = (str)

if ENABLE_LOCAL_EXECUTION is True  or os.getenv('IS_K8S') == "true":
    DQ_KPI_DEF_QUERY = "select id as parent_object_id,kpi_kqi_name as kpi_kqi_universe_name from dq_kpi_definition"
else:
    DQ_KPI_DEF_QUERY = "select id as parent_object_id,kpi_kqi_name as kpi_kqi_universe_name from sairepo.dq_kpi_definition"

DQ_KPI_DEF_FIELD_NAMES = ['kpi_kqi_name','field_name', 'field_table_name']

DQ_KPI_CDE_EXCEL_FIELD_NAMES = ['parent_object_id','kpi_kqi_name', 'kpi_kqi_description', 'field_name','field_description','field_table_name']

KPI_CDE_SHEET_NAME = 'KPI_CDE_Mapping'

DQ_KPI_DEF_GRP_BY_FIELDS = ['universe_name','kpi_kqi_name']

DQ_KPI_CDE_DESC_JOIN_COL = ['parent_object_id', 'kpi_kqi_name']

DQ_KPI_DESC_DF_COL = ['parent_object_id', 'kpi_kqi_name','kpi_kqi_description']

DQHI_SQLITE_DB = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/dqhi.db'

CREATE_SQL_FILE = 'create_dqhi_tables_nova.sql'

DELETE_SQL_FILE = 'delete_dqhi_tables_nova.sql'

TABLE_COL_DICT = 'table_column_ids_dict.properties'
    
TABLE_COL_LIST = 'table_column_list.properties'
    
RULE_NAME_ID_DICT = 'rule_name_id_dict.properties'

HIVE_TABLE_LIST = 'hive_tables.properties'

SPARK_SUBMIT_SCRIPT = 'execute_spark_submit.sh'

if os.getenv('IS_K8S') == "true":
    DQHI_OUTPUT = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/output'
else:
    DQHI_OUTPUT = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/output'

DATA_QUALITY_STATISTICS_QUERY_NOVA = """
select object_type,((case when object_type = 'KPI' then ''  else CAST(parent_object_id AS varchar) end)) as "parentobjectid",'PROJECT' as "schemaname", 'BIGDATA' as "databasetype",object_id, kpi_or_cde_name, kpi_or_cde_description, ((case when overall_score = '-1' then 'NA'  else CAST(overall_score AS varchar) end)) as overall_score, ((case when completeness_score = '-1' then 'NA'  else CAST(completeness_score AS varchar) end)) as "completeness_score",((case when conformance_score = '-1' then 'NA'  else CAST(conformance_score AS varchar) end)) as "conformity_score",((case when consistence_score = '-1' then 'NA'  else CAST(consistence_score AS varchar) end)) as "consistency_score",((case when integrity_score = '-1' then 'NA'  else CAST(integrity_score AS varchar) end)) as "integrity_score",((case when range_score = '-1' then 'NA'  else CAST(range_score AS varchar) end)) as "range_score",((case when uniqueness_score = '-1' then 'NA'  else CAST(uniqueness_score AS varchar) end)) as "uniqueness_score" from dq_kpi_cde_scores where strftime('%Y-%m-%d',computed_date)='{}' and load_date=(select max(load_date) from dq_kpi_cde_scores)
"""
DATA_QUALITY_RESULT_QUERY_NOVA = """
select 'PROJECT' as "schemaname",'BIGDATA' as databasetype, c.table_id, c.table_name, c.column_id,c.column_name,b.ruleid,a.rule_presentation_name,case when b.rule_parameter='NONE' then a.rule_description else a.rule_description || ' -' || '[' || b.rule_parameter || ']' end as rule_description,b.dimension,'field' as "objecttype", c.column_datatype,b.threshold, b.total_row_count, b.bad_record_count, b.score, b.check_pass,strftime('%Y%m%d',c.computed_date) as "datatime", strftime('%Y/%m/%d %H:%M:%S',b.start_time) as "starttime",strftime('%Y/%m/%d %H:%M:%S',b.end_time) as "endtime" from dq_rules_definition a JOIN dq_fields_score b ON a.rule_id=b.ruleid AND b.source in ('CEM') JOIN (select * from dq_fields_definition where id in (select max(id) from dq_fields_definition where strftime('%Y-%m-%d',computed_date)='{}' group by table_name,column_name)) c ON b.field_def_id=c.id
    """
DATA_QUALITY_RULE_QUERY_NOVA = """
select b.ruleid,a.rule_class ,a.rule_presentation_name,replace(b.rule_condition,'"','''') as rule_condition, case when b.rule_parameter='NONE' then a.rule_description else a.rule_description || ' -' || '[' || b.rule_parameter || ']' end as rule_description,b.dimension,'field' as "objecttype",'PROJECT' as "schemaname",'BIGDATA' as "databasetype",c.table_id as "tableid", c.table_name as "tablename", c.column_id,c.column_name, c.column_datatype,cast(b.threshold as int) as "threshold",'1' as "action",b.weight as "weight",'' as "createoperator",'' as "createtime",'' as "lastmodifyoperator",((case when b.dimension = 'RANGE' then b.rule_parameter  else '' end)) as "valuerange",((case when b.dimension = 'CONFORMITY' then b.rule_parameter  else '' end)) as "enumeration", '' as "auditexpression", ((case when b.dimension = 'CONFORMITY' then CAST(c.column_precision AS varchar)  else '' end)) as "fieldlength" from dq_rules_definition a JOIN dq_fields_score b ON a.rule_id=b.ruleid AND b.source in ('CEM') JOIN (select * from dq_fields_definition where id in (select max(id) from dq_fields_definition where strftime('%Y-%m-%d',computed_date)='{}' group by table_name,column_name)) c ON b.field_def_id=c.id
"""

DQHI_LOG_PATH_NOVA = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt/log'

DQHI_TRIGGER_INSERT_QUERY = '''insert into dq_score_calculator_trigger values('{s}','{d}')'''

DQHI_TRIGGER_FETCH_QUERY = 'select max(trigger_date) from dq_score_calculator_trigger'

DQHI_TRIGGER_DELETE_QUERY = 'delete from dq_score_calculator_trigger '

DQHI_EXPORT_SQL = '/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/sql/export'

MONITORING_DIR = '/var/local/monitoring/log'
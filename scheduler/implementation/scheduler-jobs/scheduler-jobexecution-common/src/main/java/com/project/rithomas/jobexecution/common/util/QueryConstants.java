
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class QueryConstants {

	public static final String ETL_STATUS_INSERT = "INSERT INTO rithomas.ETL_STATUS (PROC_ID, JOB_NAME, TYPE, LOAD_TIME, START_TIME, END_TIME, STATUS, DESCRIPTION) values($PROC_ID, $JOBNAME, $JOBTYPE, LOCALTIMESTAMP(0), LOCALTIMESTAMP(0) , null , $STATUS , null)";

	public static final String ETL_STATUS_INSERT_FOR_WAITING = "INSERT INTO rithomas.ETL_STATUS (PROC_ID, JOB_NAME, TYPE, LOAD_TIME, START_TIME, END_TIME, STATUS, DESCRIPTION) values($PROC_ID, $JOBNAME, $JOBTYPE, LOCALTIMESTAMP(0), LOCALTIMESTAMP(0) , LOCALTIMESTAMP(0) , $STATUS , $DESCRIPTION)";

	public static final String ETL_STATUS_UPDATE = "UPDATE rithomas.ETL_STATUS SET END_TIME = LOCALTIMESTAMP(0), STATUS = $STATUS, DESCRIPTION = $DESCRIPTION ,ERROR_DESCRIPTION =nvl(ERROR_DESCRIPTION || ';','') || $ERROR_DESCRIPTION WHERE PROC_ID = $PROC_ID and (case when $STATUS = 'S' then 'S' else status end) !='E'";

	public static final String ETL_STATUS_UPDATE_FOR_WAITING = "UPDATE rithomas.ETL_STATUS SET END_TIME = LOCALTIMESTAMP(0), STATUS = $STATUS, DESCRIPTION = $DESCRIPTION ,ERROR_DESCRIPTION = $ERROR_DESCRIPTION WHERE PROC_ID = $PROC_ID AND STATUS != 'E'";

	public static final String CHECK_JOB_STATUS = "select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name=$JOBNAME and status != $STATUS)";

	public static final String USAGE_IMPORT_DIR = "select tnes_import_path from rithomas.tnes_types where tnes_type = $TNES_TYPE";

	public static final String ETL_STATUS_UPDATE_FOR_RUNNING_IDLE_TO_E = "UPDATE rithomas.ETL_STATUS SET END_TIME = LOCALTIMESTAMP(0), STATUS = 'E',ERROR_DESCRIPTION = 'scheduler was restarted.' WHERE STATUS IN ('R','I','C')";

	public static final String ETL_STATUS_UPDATE_FOR_RUNNING_IDLE_TO_E_ON_ABORT = "UPDATE rithomas.ETL_STATUS SET END_TIME = LOCALTIMESTAMP(0), STATUS = 'E',ERROR_DESCRIPTION = 'Job was aborted by the user.' WHERE JOB_NAME=? AND STATUS IN ('R','I')";

	public static final String REAGG_LIST_UPDATE = "UPDATE rithomas.reagg_list SET status = '$STATUS' WHERE provider_jobname = '$PROVIDER_JOB' AND requestor_jobname IN ($SOURCE_JOB_NAMES) AND report_time = '$REPORT_TIME' AND status IN ($OLD_STATUS)";

	public static final String REAGG_LIST_UPDATE_TZ = "UPDATE rithomas.reagg_list SET status = '$STATUS' WHERE provider_jobname = '$PROVIDER_JOB' AND requestor_jobname IN ($SOURCE_JOB_NAMES) AND report_time = '$REPORT_TIME' AND status IN ($OLD_STATUS) AND region_id ='$REGION_ID'";

	public static final String REAGG_LIST_UPDATE_PREV_STATUS = "UPDATE rithomas.reagg_list SET status = '$STATUS' WHERE provider_jobname = '$PROVIDER_JOB' AND requestor_jobname IN ($SOURCE_JOB_NAMES) AND status IN ($OLD_STATUS)";

	public static final String REAGG_LIST_UPDATE_PREV_STATUS_TZ = "UPDATE rithomas.reagg_list SET status = '$STATUS' WHERE provider_jobname = '$PROVIDER_JOB' AND requestor_jobname IN ($SOURCE_JOB_NAMES) AND status IN ($OLD_STATUS) AND region_id='$REGION_ID'";

	public static final String REAGG_CONFIG_QUERY = "select param_name, param_value from saidata.es_reagg_config_1";

	public static final String RUNTIME_PROP_QUERY = "select paramvalue from runtime_prop where paramname='QUERY_TIMEOUT'";

	public static final String CHECK_REAGG_LIST = "SELECT provider_jobname FROM rithomas.reagg_list r WHERE r.requestor_jobname = '$REQUESTOR_JOB'"
			+ " AND r.provider_jobname = '$PROVIDER_JOB' AND r.report_time = '$REPORT_TIME' AND r.status <> 'Completed' and r.status <> 'Completed_Part'";

	public static final String GET_MIN_LOAD_TIME = "select min(load_time) from rithomas.reagg_list r where r.status = 'Initialized' and r.provider_jobname = '$PROVIDER_JOB'";

	public static final String CHECK_REAGG_LIST_WITH_REGION = "SELECT provider_jobname FROM rithomas.reagg_list r WHERE r.requestor_jobname = '$REQUESTOR_JOB'"
			+ " AND r.provider_jobname = '$PROVIDER_JOB' AND r.report_time = '$REPORT_TIME' AND r.region_id = '$REGION_ID' AND r.status <> 'Completed' and r.status <> 'Completed_Part'";

	public static final String GET_DISTINCT_REAGG_LIST = "SELECT DISTINCT requestor_jobname, provider_jobname, report_time, region_id FROM rithomas.reagg_list WHERE requestor_jobname in ($SOURCE_JOB_NAMES) AND provider_jobname = '$PROVIDER_JOB' AND status = 'Completed_Part' AND load_time >= '$LOAD_TIME'";

	public static final String GET_PENDING_REAGG_LIST = "SELECT provider_jobname, report_time from rithomas.reagg_list where provider_jobname='$JOB_ID' and status in ('Pending', 'Initialized') and report_time>='$LOWER_BOUND' and report_time<'$UPPER_BOUND'";
	
	public static final String GET_SOURCE_TABLE_NAME_ADAP = "select table_name, jd.adaptationid, jd.adaptationversion from rithomas.job_list jl, rithomas.boundary b, rithomas.job_dictionary jd, rithomas.job_dict_dependency jdd "
			+ " where b.jobid='$JOB_ID' and b.id=jdd.bound_jobid and jdd.source_jobid=jl.id and jl.jobid=jd.jobid";

	public static final String GET_SOURCE_TABLE_NAME_ADAP_TZ = "select table_name, jd.adaptationid, jd.adaptationversion from rithomas.job_list jl, rithomas.boundary b, rithomas.job_dictionary jd, rithomas.job_dict_dependency jdd "
			+ " where b.jobid='$JOB_ID' and b.region_id='$REGION_ID' and b.id=jdd.bound_jobid and jdd.source_jobid=jl.id and jl.jobid=jd.jobid";

	public static final String INSERT_INTO_REAGG_LIST = "insert into rithomas.reagg_list (id, load_time, report_time, requestor_jobname, provider_jobname, alevel, status) "
			+ "values(nextval('rithomas.reagg_list_s'),'$LOAD_TIME','$REPORT_TIME','$REQUESTOR_JOB','$PROVIDER_JOB','$ALEVEL','$STATUS')";

	public static final String INSERT_INTO_REAGG_LIST_WITH_REGION = "insert into rithomas.reagg_list (id, load_time, report_time, requestor_jobname, provider_jobname, alevel,region_id, status) "
			+ "values(nextval('rithomas.reagg_list_s'),'$LOAD_TIME','$REPORT_TIME','$REQUESTOR_JOB','$PROVIDER_JOB','$ALEVEL','$REGION_ID','$STATUS')";

	public static final String INSERT_INTO_USAGE_AGG_STATUS = "insert into rithomas.usage_agg_status(usage_job_id,report_time,number_of_records,perf_job_id,status) values('$USAGE_JOB_ID','$REPORT_TIME','$NUMBER_OF_RECORDS','$AGG_JOB_ID','false')";

	public static final String INSERT_INTO_USAGE_AGG_STATUS_WITH_REGION = "insert into rithomas.usage_agg_status(usage_job_id,report_time,number_of_records,perf_job_id,region_id,status) values('$USAGE_JOB_ID','$REPORT_TIME','$NUMBER_OF_RECORDS','$AGG_JOB_ID','$REGION_ID','false')";

	public static final String USAGE_AGG_STATUS_LAST_INSERT_COUNT = "select sum(number_of_records) as inserted_records from usage_agg_status where usage_job_id='$USAGE_JOB_ID' and report_time='$REPORT_TIME' group by perf_job_id";

	public static final String USAGE_AGG_STATUS_LAST_INSERT_COUNT_WITH_REGION = "select sum(number_of_records) as inserted_records from usage_agg_status where usage_job_id='$USAGE_JOB_ID' and report_time='$REPORT_TIME'  and region_id='$REGION_ID' group by perf_job_id";

	public static final String GET_AGGREGATED_RECORD_COUNT = "select sum(number_of_records) from rithomas.usage_agg_status where usage_job_id='$USAGE_JOB_ID' and perf_job_id='$AGG_JOB_ID' and status='$STATUS' and report_time>='$REPORT_TIME' and report_time<'$UPPER_BOUND'";

	public static final String GET_AGGREGATED_RECORD_COUNT_TZ = "select sum(number_of_records) from rithomas.usage_agg_status where usage_job_id='$USAGE_JOB_ID' and perf_job_id='$AGG_JOB_ID' and status='$STATUS' and report_time>='$REPORT_TIME' and report_time<'$UPPER_BOUND' and region_id='$REGION_ID'";

	public static final String UPDATE_USAGE_AGG_STATUS = "update rithomas.usage_agg_status set status='true' where  perf_job_id='$AGG_JOB_ID' and report_time>='$REPORT_TIME' and report_time<'$UPPER_BOUND'";

	public static final String UPDATE_USAGE_AGG_STATUS_TZ = "update rithomas.usage_agg_status set status='true' where  perf_job_id='$AGG_JOB_ID' and report_time>='$REPORT_TIME' and report_time<'$UPPER_BOUND' and region_id='$REGION_ID'";

	public static final String GET_REPORT_TIMES = "select distinct case when region_id is null then concat(report_time,'') else concat(report_time,' (Region: ',region_id,')') end rt from rithomas.reagg_list where status ='Completed_Part' and provider_jobname ='$PROVIDER_JOB' and requestor_jobname in ($SOURCE_JOB_NAMES) order by rt";

	public static final String ALEVEL = "$ALEVEL";

	public static final String STATUS = "$STATUS";

	public static final String PROC_ID = "$PROC_ID";

	public static final String DESCRIPTION = "$DESCRIPTION";

	public static final String ERROR_DESCRIPTION = "$ERROR_DESCRIPTION";

	public static final String REPORT_TIME = "$REPORT_TIME";

	public static final String PROVIDER_JOB = "$PROVIDER_JOB";

	public static final String PROVIDER_AGG_JOB = "$PROVIDER_AGG_JOB";

	public static final String JOB_ID = "$JOB_ID";

	public static final String PARAM_VAL = "$PARAM_VAL";

	public static final String LOAD_TIME = "$LOAD_TIME";

	public static final String REQUESTOR_JOB = "$REQUESTOR_JOB";

	public static final String SOURCE_JOB_NAMES = "$SOURCE_JOB_NAMES";

	public static final String OLD_STATUS = "$OLD_STATUS";

	public static final String USAGE_JOB_ID = "$USAGE_JOB_ID";

	public static final String PERF_JOB_ID = "$AGG_JOB_ID";

	public static final String UPPER_BOUND = "$UPPER_BOUND";

	public static final String NUMBER_OF_RECORDS = "$NUMBER_OF_RECORDS";

	public static final String TABLE_NAME = "$TABLE_NAME";

	public static final String UNIQUE_COLS = "$UNIQUE_COLS";

	public static final String TABLE_COLS = "$TABLE_COLS";

	// rakekris
	public static final String DUPLICATE_RECORDS_QUERY = "select $TABLE_COLS from(select *,row_number() over (partition by $UNIQUE_COLS)rowpartition from $TABLE_NAME)row_table where row_table.rowpartition>1";

	public static final String DEPENDENT_JOBS_QUERY = "SELECT jd.jobid AS provider_jobid, jp.paramvalue AS alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.boundary bi"
			+ " WHERE  j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND  j.paramvalue = bi.jobid AND  bi.maxvalue IS NOT NULL) as maxvalue, "
			+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
			+ " FROM rithomas.job_dictionary jd, rithomas.job_prop jp, rithomas.boundary b WHERE  b.sourcejobid = '$JOB_ID' AND b.jobid = jd.jobid"
			+ " AND  jd.id = jp.jobid AND  jp.paramname = 'ALEVEL' AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')"
			+ " UNION "
			+ "SELECT b.jobid as provider_jobid, jp.paramvalue as alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd, rithomas.boundary bi"
			+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND j.paramvalue = bi.jobid AND bi.maxvalue IS NOT NULL) as maxvalue,"
			+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
			+ " FROM rithomas.job_dict_dependency c, rithomas.job_list j, rithomas.boundary b, rithomas.job_prop jp, rithomas.job_dictionary jd"
			+ "  WHERE j.jobid = '$JOB_ID' AND c.source_jobid = j.id AND b.id = c.bound_jobid AND b.jobid = jd.jobid AND jp.jobid = jd.id  AND jp.paramname = 'ALEVEL'"
			+ " AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')";

	public static final String DEPENDENT_JOBS_QUERY_TZ = "SELECT jd.jobid AS provider_jobid, jp.paramvalue AS alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.boundary bi"
			+ " WHERE  j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND  j.paramvalue = bi.jobid AND  bi.maxvalue IS NOT NULL AND ((nvl(bi.region_id, '$REGION_ID') = '$REGION_ID') "
			+ "OR (jp.paramvalue in ('WEEK','MONTH') AND nvl(bi.region_id, '$DEFAULT_PART') = '$DEFAULT_PART'))) as maxvalue, "
			+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
			+ " FROM rithomas.job_dictionary jd, rithomas.job_prop jp, rithomas.boundary b WHERE  b.sourcejobid = '$JOB_ID' AND b.jobid = jd.jobid"
			+ " AND  jd.id = jp.jobid AND (nvl(b.region_id, '$REGION_ID') = '$REGION_ID' or nvl(b.region_id, '$DEFAULT_PART') = '$DEFAULT_PART') AND jp.paramname = 'ALEVEL' AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')"
			+ " UNION "
			+ "SELECT b.jobid as provider_jobid, jp.paramvalue as alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd, rithomas.boundary bi"
			+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND j.paramvalue = bi.jobid AND bi.maxvalue IS NOT NULL AND ((nvl(bi.region_id, '$REGION_ID') = '$REGION_ID') "
			+ "OR (jp.paramvalue in ('WEEK','MONTH') AND nvl(bi.region_id, '$DEFAULT_PART') = '$DEFAULT_PART'))) as maxvalue,"
			+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
			+ " FROM rithomas.job_dict_dependency c, rithomas.job_list j, rithomas.boundary b, rithomas.job_prop jp, rithomas.job_dictionary jd"
			+ "  WHERE j.jobid = '$JOB_ID' AND c.source_jobid = j.id AND b.id = c.bound_jobid AND (nvl(b.region_id, '$REGION_ID') = '$REGION_ID' or nvl(b.region_id, '$DEFAULT_PART') = '$DEFAULT_PART') AND b.jobid = jd.jobid AND jp.jobid = jd.id  AND jp.paramname = 'ALEVEL'"
			+ " AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')";

	public static final String MULTIPLE_DEPENDENT_JOBS_QUERY = "SELECT b.jobid as provider_jobid, jp.paramvalue as alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd, rithomas.boundary bi"
			+ " WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND j.paramvalue = bi.jobid AND bi.maxvalue IS NOT NULL) as maxvalue, "
			+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name, "
			+ "  b.region_id FROM rithomas.job_dict_dependency c, rithomas.job_list j, rithomas.boundary b, rithomas.job_prop jp, rithomas.job_dictionary jd"
			+ "  WHERE j.jobid = '$JOB_ID' AND c.source_jobid = j.id AND b.id = c.bound_jobid AND b.jobid = jd.jobid AND jp.jobid = jd.id  AND jp.paramname = 'ALEVEL'"
			+ " AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')";

	public static final String AGG_DEPENDENT_JOBS_QUERY = "SELECT DISTINCT job_prop_target.jobid, job_prop_target.maxvalue, job_prop_target.alevel "
			+ "FROM job_prop jp, " + "(  SELECT DISTINCT perf_boundary.jobid, "
			+ "( CASE WHEN job_prop.paramvalue = '15MIN' THEN perf_boundary.maxvalue + interval '15 minutes' "
			+ "WHEN job_prop.paramvalue = 'HOUR' THEN perf_boundary.maxvalue + interval '1 hour' "
			+ "WHEN job_prop.paramvalue = 'DAY' THEN perf_boundary.maxvalue + interval '1 day' "
			+ "WHEN job_prop.paramvalue = 'WEEK' THEN perf_boundary.maxvalue + interval '1 week' "
			+ "WHEN job_prop.paramvalue = 'MONTH' THEN perf_boundary.maxvalue + interval '1 month' "
			+ "WHEN job_prop.paramvalue = 'YEAR' THEN perf_boundary.maxvalue + interval '1 year' "
			+ "ELSE perf_boundary.maxvalue END ) as maxvalue, job_prop.paramvalue AS alevel , job_prop.jobid as id "
			+ "FROM rithomas.boundary perf_boundary, rithomas.job_dictionary, ( select jobid, paramname, paramvalue from rithomas.job_prop ) job_prop"
			+ " WHERE perf_boundary.sourcejobid = '$JOB_ID' AND perf_boundary.jobid = job_dictionary.jobid AND"
			+ " job_dictionary.id = job_prop.jobid AND ( job_prop.paramname = 'ALEVEL' OR job_prop.paramname = 'INTERVAL' ) AND"
			+ " job_dictionary.typeid in ( "
			+ "select id from rithomas.job_type_dictionary where jobtypeid = 'Export' or jobtypeid = 'Aggregation' or "
			+ "jobtypeid = 'TNPAggregation' or jobtypeid = 'Threshold' or jobtypeid = 'Profiling' )  "
			+ "UNION  SELECT DISTINCT perf_boundary.jobid, "
			+ "( CASE WHEN job_prop.paramvalue = '15MIN' THEN perf_boundary.maxvalue + interval '15 minutes' "
			+ "WHEN job_prop.paramvalue = 'HOUR' THEN perf_boundary.maxvalue + interval '1 hour' "
			+ "WHEN job_prop.paramvalue = 'DAY' THEN perf_boundary.maxvalue + interval '1 day' "
			+ "WHEN job_prop.paramvalue = 'WEEK' THEN perf_boundary.maxvalue + interval '1 week' "
			+ "WHEN job_prop.paramvalue = 'MONTH' THEN perf_boundary.maxvalue + interval '1 month' "
			+ "WHEN job_prop.paramvalue = 'YEAR' THEN perf_boundary.maxvalue + interval '1 year' "
			+ "ELSE perf_boundary.maxvalue END ) as maxvalue, job_prop.paramvalue AS alevel , job_prop.jobid "
			+ "FROM rithomas.boundary perf_boundary, rithomas.job_dictionary, rithomas.job_prop, rithomas.job_dict_dependency jd, rithomas.job_list jl"
			+ " WHERE jl.jobid = '$JOB_ID' AND jl.id = jd.source_jobid AND jd.bound_jobid = perf_boundary.id AND perf_boundary.sourcejobid IS NULL AND"
			+ " perf_boundary.jobid = job_dictionary.jobid AND job_dictionary.id = job_prop.jobid AND job_prop.paramname = 'ALEVEL'"
			+ " AND job_dictionary.typeid = ( SELECT id FROM rithomas.job_type_dictionary where jobtypeid = 'Aggregation' ) ) job_prop_target "
			+ "WHERE job_prop_target.id = jp.jobid  AND 'YES' =  (  SELECT case when exists ( SELECT  1  FROM  job_prop  WHERE  paramname = 'JOB_ENABLED' "
			+ "AND jobid = jp.jobid ) THEN ( SELECT paramvalue FROM job_prop WHERE paramname = 'JOB_ENABLED'  AND jobid = jp.jobid  AND paramvalue = 'YES') "
			+ "ELSE 'YES' END );";

	public static final String AGG_DEPENDENT_JOBS_QUERY_TIME_ZONE = "SELECT DISTINCT job_prop_target.jobid, job_prop_target.maxvalue, job_prop_target.alevel, job_prop_target.regions "
			+ "FROM job_prop jp, ( SELECT DISTINCT  perf_boundary.jobid,  "
			+ "( CASE  WHEN job_prop.paramvalue = '15MIN' THEN perf_boundary.maxvalue + interval '15 minutes' "
			+ "WHEN job_prop.paramvalue = 'HOUR' THEN perf_boundary.maxvalue + interval '1 hour' "
			+ "WHEN job_prop.paramvalue = 'DAY' THEN perf_boundary.maxvalue + interval '1 day' "
			+ "WHEN job_prop.paramvalue = 'WEEK' THEN perf_boundary.maxvalue + interval '1 week' "
			+ "WHEN job_prop.paramvalue = 'MONTH' THEN perf_boundary.maxvalue + interval '1 month'"
			+ " WHEN job_prop.paramvalue = 'YEAR' THEN perf_boundary.maxvalue + interval '1 year'"
			+ " ELSE perf_boundary.maxvalue  END  )  as maxvalue, job_prop.paramvalue AS alevel, perf_boundary.region_id as regions,  "
			+ "job_prop.jobid as id  FROM  rithomas.boundary perf_boundary, rithomas.job_dictionary, "
			+ "( SELECT  jobid,  paramname,  paramvalue  FROM  rithomas.job_prop )  job_prop  "
			+ "WHERE  perf_boundary.sourcejobid = '$JOB_ID' AND perf_boundary.jobid = job_dictionary.jobid AND job_dictionary.id = job_prop.jobid"
			+ " AND ( job_prop.paramname = 'ALEVEL'  OR job_prop.paramname = 'INTERVAL' )  AND job_dictionary.typeid in "
			+ "( SELECT  id  FROM  rithomas.job_type_dictionary  WHERE  jobtypeid = 'Export' or jobtypeid = 'Aggregation' ) "
			+ "UNION SELECT DISTINCT  perf_boundary.jobid,  "
			+ "( CASE  WHEN job_prop.paramvalue = '15MIN' THEN perf_boundary.maxvalue + interval '15 minutes' "
			+ "WHEN job_prop.paramvalue = 'HOUR' THEN perf_boundary.maxvalue + interval '1 hour'"
			+ " WHEN job_prop.paramvalue = 'DAY' THEN perf_boundary.maxvalue + interval '1 day' "
			+ "WHEN job_prop.paramvalue = 'WEEK' THEN perf_boundary.maxvalue + interval '1 week'"
			+ " WHEN job_prop.paramvalue = 'MONTH' THEN perf_boundary.maxvalue + interval '1 month'"
			+ " WHEN job_prop.paramvalue = 'YEAR' THEN perf_boundary.maxvalue + interval '1 year' ELSE perf_boundary.maxvalue  END  )  as maxvalue, "
			+ "job_prop.paramvalue AS alevel, region_id as regions ,  job_prop.jobid as id  "
			+ "FROM  rithomas.boundary perf_boundary, rithomas.job_dictionary, rithomas.job_prop, rithomas.job_dict_depENDency jd, rithomas.job_list jl  "
			+ "WHERE  jl.jobid = '$JOB_ID' AND jl.id = jd.source_jobid AND jd.bound_jobid = perf_boundary.id AND perf_boundary.sourcejobid IS NULL "
			+ "AND perf_boundary.jobid = job_dictionary.jobid AND job_dictionary.id = job_prop.jobid AND job_prop.paramname = 'ALEVEL' "
			+ "AND job_dictionary.typeid = ( SELECT  id  FROM  rithomas.job_type_dictionary  WHERE  jobtypeid = 'Aggregation' ) ) job_prop_target"
			+ " WHERE job_prop_target.id = jp.jobid  AND 'YES' =  ( SELECT  CASE WHEN  exists ( SELECT 1  FROM job_prop "
			+ " WHERE paramname = 'JOB_ENABLED'  AND jobid = jp.jobid  ) THEN(  SELECT paramvalue FROM job_prop WHERE paramname = 'JOB_ENABLED' "
			+ " AND jobid = jp.jobid  AND paramvalue = 'YES') ELSE 'YES' END );";

	public static final String REAGG_DEPENDENT_JOBS_QUERY = "SELECT DISTINCT provider_jobname AS jobid FROM rithomas.reagg_list, rithomas.job_dictionary, rithomas.job_prop"
			+ " WHERE reagg_list.requestor_jobname = '$JOB_ID' AND reagg_list.status NOT IN ('Pending', 'Completed', 'Completed_Part')"
			+ " AND job_dictionary.jobid = reagg_list.provider_jobname AND job_dictionary.id = job_prop.jobid"
			+ " AND job_prop.paramname = 'REAGG_JOB_ENABLED' AND job_prop.paramvalue = 'YES'";

	public static final String AVAILABLE_DATA_SGSN_QUERY = "select * from saidata.es_sgsn_available_data_1 where upper(type) in ('GB','IUPS') and upper(available)='YES' and source='SGSN'";

	public static final String CHECK_SOURCE_QUERY = "select type from saidata.es_sgsn_available_data_1 where source in ('SGSN','GNCP','LTE_4G') and upper(available)='YES'";

	public static final String HOME_COUNTRY_CODE_QUERY = "select  param_value from saidata.es_home_country_lookup_1 where param_name='HOME_COUNTRY_CODE'";

	public static final String HOME_COUNTRY_IMSI_CODE_QUERY = "select param_value from saidata.es_home_country_lookup_1 where param_name like '%HOME_COUNTRY_IMSI_CODE%'";

	public static final String HOME_OPERATOR_ID_QUERY = "select param_value from saidata.es_home_country_lookup_1 where param_name like '%HOME_OPERATOR_ID%'";

	public static final String AVAILABLE_DATA_HTTP_QUERY = "select AVAILABLE from saidata.ES_SGSN_AVAILABLE_DATA_1 where SOURCE='GNUP' and type='US_GNUP_1'";

	public static final String AVAILABLE_DATA_GN_QUERY = "select AVAILABLE from saidata.ES_SGSN_AVAILABLE_DATA_1 where SOURCE = 'GNCP' and type in ('GN','FNG') AND upper(AVAILABLE) = 'YES' ";

	public static final String AVAILABLE_DATA_HTTP_4G_QUERY = "select AVAILABLE from saidata.ES_SGSN_AVAILABLE_DATA_1 where SOURCE='S1U' and type='US_S1U_1'";

	public static final String AVAILABLE_DATA_GN_4G_QUERY = "select AVAILABLE from saidata.ES_SGSN_AVAILABLE_DATA_1 where SOURCE='LTE_4G' and type='4G'";

	public static final String GET_MAX_LOC_QUERY = "select max(loc_id) from ES_LOC_1 where loc_id not in (-1,-99,-10)";

	public static final String GET_MAX_REGION_QUERY = "select max(region_id) from ES_REGION_1 where region_id not in (-1,-99,-10)";

	public static final String GET_MAX_CITY_QUERY = "select max(city_id) from ES_CITY_1 where city_id not in (-1,-99,-10)";

	private QueryConstants() {
		// hide default constructor for utility class
	}

	public static final String END_TIME = "$END_TIME";

	public static final String ETL_STATUS_DELETE = "DELETE FROM rithomas.ETL_STATUS WHERE END_TIME <= '$END_TIME';";

	public static final String UNIQUE_ROWS_QUERY = "select count(*),$UNIQUE_COLS from $TABLE_NAME group by $UNIQUE_COLS having count(*) >1";

	public static final String MAX_COUNT_ASSOC_QUERY = "select nvl(max(association_id),0) from ES_ASSOC_TYPES_1";

	public static final String MAX_COUNT_PARENT_QUERY = "select nvl(max(parent_group_id),0) from ES_PARENT_GROUP_1";

	public static final String MAX_COUNT_SUBGROUP_QUERY = "select nvl(max(group_id),0) from ES_SUB_GROUP_1";

	public static final Map<String, String> UNIQUE_ROWS_QUERY_MAP = new HashMap<String, String>();

	public static final String UPDATE_SUBSCRIPTION_AGE = "insert overwrite table es_subscription_1 select es_subscription_1.subscription_id,es_subscription_1.contract_id_ref,es_subscription_1.imsi,es_subscription_1.msisdn,es_subscription_1.subs_start_time,es_subscription_1.subs_end_time,es_subscription_1.subs_activation_time,es_subscription_1.device,es_subscription_1.subs_lt_revenue,es_subscription_1.subs_category,es_subscription_1.subs_subcategory,es_subscription_1.subs_rate,es_subscription_1.subs_state,es_subscription_1.cust_first_nm,es_subscription_1.cust_last_nm,es_subscription_1.cust_dob,es_subscription_1.cust_gender,es_subscription_1.cust_lang,es_subscription_1.cust_credit_rating,es_subscription_1.cust_region,es_subscription_1.cust_sub_region,es_subscription_1.cust_category,es_subscription_1.cust_subcategory,es_subscription_1.cust_role,es_subscription_1.cust_status,es_subscription_1.cust_annual_income_lvl,es_subscription_1.cust_lt_val,es_subscription_1.cust_nationality,case when month(cust_dob)= month(sysdate()) and day(cust_dob)=day(sysdate()) then floor(datediff(sysdate(),CUST_DOB)/365) else cust_age end from es_subscription_1";
	static {
		UNIQUE_ROWS_QUERY_MAP.put(JobExecutionContext.ES_LOCATION_JOBID,
				"select count(*),LAC_ID,CELL_SAC_ID from EXT_LOCATION_1 group by LAC_ID,CELL_SAC_ID having count(*) >1");
		UNIQUE_ROWS_QUERY_MAP.put(JobExecutionContext.ES_OPERATOR_JOBID,

	public static final String SKIPPED_SERVICE_TZ_QUERY = "select tmz_region,TIME_SKIPPED_SERVICES from saidata.es_rgn_tz_info_1";

	public static final String MSISDN_MAX_LEN_SUPPORTED_QUERY = "select  param_value from saidata.es_home_country_lookup_1 where param_name='MSISDN_MAX_LEN_SUPPORTED'";

	public static final String GET_HIVE_RETRY_EXCEPTIONS = "select paramvalue from rithomas.runtime_prop where paramname like '%HIVE_RETRY_EXCEPTION%'";

	public static final String RETRY_ALL_HIVE_EXCEPTIONS = "select paramvalue from rithomas.runtime_prop where paramname like '%RETRY_ALL_HIVE_EXCEPTIONS%'";



	public static final String RUNTIME_PROP_QUERY_ALL = "select paramname,paramvalue,version from runtime_prop";

	public static final String AVAILABLE_DATA_ON_TYPE_AVAIL = "select type from saidata.es_sgsn_available_data_1 where type='${TYPE}' and available='${AVAILABLE}'";

	public static final String AVAILABLE_DATA_COL_TYPE = "TYPE";

	public static final String AVAILABLE_DATA_COL_AVAILABLE = "AVAILABLE";

	public static final String REAGG_DEPENDENT_DISABLED_JOBS_QUERY = "SELECT paramvalue FROM job_prop  WHERE jobid IN "
			+ "(SELECT id FROM job_dictionary WHERE jobid IN (SELECT paramvalue FROM job_prop WHERE jobid IN "
			+ "(SELECT id FROM job_dictionary WHERE jobid = '$JOB_ID') and paramname='PERF_JOB_NAME'))"
			+ " AND  paramname='JOB_ENABLED' AND paramvalue='NO';";

	public static final String ARCHIVING_DEPENDENT_JOBS_QUERY = "select distinct perf_boundary.jobid,perf_boundary.maxvalue,jp.paramvalue "
			+ "from rithomas.boundary perf_boundary, rithomas.job_dictionary, rithomas.job_prop jp where "
			+ "perf_boundary.sourcejobid='$JOB_ID' and perf_boundary.jobid=job_dictionary.jobid and "
			+ "jp.jobid = job_dictionary.id and job_dictionary.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Archiving') "
			+ "and jp.paramname='ARCHIVING_DAYS' and jp.paramvalue != '0' and jp.paramvalue is not null and "
			+ "'YES' =  (  SELECT case when exists ( SELECT  1  FROM  job_prop  WHERE  paramname = 'JOB_ENABLED'"
			+ " AND jobid = jp.jobid ) THEN ( SELECT paramvalue FROM job_prop WHERE paramname = 'JOB_ENABLED'  AND jobid = jp.jobid  "
			+ "AND paramvalue = 'YES') ELSE 'YES' END );";

	public static final String SE_RESTART_UPDATE_QUERY = "UPDATE rithomas.ETL_STATUS SET END_TIME = LOCALTIMESTAMP(0), STATUS = 'E',ERROR_DESCRIPTION = 'schedulerEngine was restarted.' WHERE STATUS IN ('R','I','C')";
	
	public static final String SE_RESTART_UPDATE_REAGG_LIST = "UPDATE rithomas.REAGG_LIST SET STATUS='Initialized' WHERE STATUS='Running'";

	public static final String CLEANUP_USAGE_AGG_STATUS = "delete from rithomas.usage_agg_status where usage_job_id='%s' and report_time<'%s'";

	public static final String SERVICE_ID_FOR_USAGE_QUERY_AVAILABLE_SRC = "select type from saidata.es_sgsn_available_data_1 where source in (USAGE_ID_LIST) and upper(available) = 'YES'";

	public static final String SERVICE_ID_FOR_USAGE_QUERY = "select type from saidata.es_sgsn_available_data_1 where source in (USAGE_ID_LIST)";
	public static final String SGSN_AVAILABLE_DATA_TYPE_QUERY = "select type from saidata.es_sgsn_available_data_1 where source='%s' and upper(available) = 'YES'";
	public static final String GET_BOUNDARY_REGION_CHECK_QUERY = "select * from boundary where jobid='$JOB_ID' and region_id='$REGION_ID'";

	public static final String REGION_ZONEID_QUERY = "select tmz_region, tmz_name from $TABLE_NAME order by (case when tmz_name is not null and tmz_name!='null' then now() at time zone tmz_name else null end) desc";

	public static final String REAGG_LIST_STATUS_QUERY =  "select string_agg(requestor_jobname,','), reagg.provider_jobname, reagg.alevel, reagg.report_time, reagg.status, reagg.region_id from "
			+ "(SELECT requestor_jobname, provider_jobname, alevel, report_time, status, region_id FROM rithomas.reagg_list"
			+ " WHERE  provider_jobname = '$JOB_ID' AND status NOT IN ('Completed', 'Completed_Part', 'Pending')) reagg left outer join "
			+ "(select report_time, region_id from reagg_list where provider_jobname='$JOB_ID' and status ='Pending') pending_list"
			+ " on reagg.report_time=pending_list.report_time and nvl(reagg.region_id,'-1')=nvl(pending_list.region_id,'-1') where pending_list.report_time is null"
			+ " group by reagg.provider_jobname, reagg.alevel, reagg.report_time, reagg.status, reagg.region_id ORDER  BY reagg.region_id, reagg.report_time desc";
	
	public static final String MAX_END_TIME_DEPENDENT_REAGG = "select max(case when alevel='HOUR' then report_time + interval '01:00' hour to minute when alevel='15MIN' then report_time + interval '00:15' hour to minute "
			+ "when alevel='DAY' then report_time + interval '1' day when alevel ='WEEK' then report_time + interval '7' day when alevel ='MONTH' then report_time + interval '1' month else report_time end) as end_time "
			+ "from reagg_list where requestor_jobname='$JOB_ID' and status not in ('Completed', 'Completed_Part') and report_time<'$RETENTION_DATE'";

	public static final String ACTIVE_DEPENDENT_REAGG_JOBS = "select provider_jobname, report_time from reagg_list where requestor_jobname=replace(replace('%s','Aggregate','Reaggregate'),'Archiving','Reaggregate') and status not in ('Completed', 'Completed_Part') and report_time<'%s'";

	public static final String ENABLE_DISABLE_REAGG_JOB_QUERY =  "update rithomas.job_prop set paramvalue='$PARAM_VAL' where paramname='REAGG_JOB_ENABLED' and "
			+ "jobid in (select id from rithomas.job_dictionary where jobid in ($JOB_ID))";

	public static final String GET_JOB_ENABLED_QUERY = "select paramname,paramvalue from job_prop where paramname in('JOB_ENABLED','REAGG_JOB_ENABLED') and jobid in (select id from job_dictionary where jobid in ('$PROVIDER_JOB','$PROVIDER_AGG_JOB'))";

	public static final String GET_TO_EXECUTE_INSERT_UPDATE = "INSERT INTO rithomas.to_execute (job_id, to_execute) VALUES ('$JOB_ID','$AGG_QUERY') ON CONFLICT (job_id) DO UPDATE SET to_execute = '$AGG_QUERY'";

	public static final String AGG_QUERY = "$AGG_QUERY";

	public static final String INSERT_JOB_STATS = "insert into JOB_EXE_STATS VALUES('$LOAD_TIME','$JOB_ID',$REPORT_TIME,$TIMEZONE_PARTITION,$APP_IDS,'$STATS') ON CONFLICT (load_time, job_id, report_time, time_zone) DO NOTHING";
	
	public static final String DIM_VALS_QUERY = "select dim_name,dim_query from DIM_VAL_QUERY";
	
	public static final String USAGE_JOB_BOUNDARY_QUERY = "select jobid,maxvalue from boundary where jobid like 'Usage%LoadJob' and maxvalue is not null";

	public static final String USAGE_STATUS_QUERY = "select job_name,status,error_description,end_time from etl_status where proc_id in (select max(proc_id) from etl_status where job_name in (select jobid from boundary where jobid like 'Usage_%LoadJob') and status not in ('W','S') group by job_name)";

}

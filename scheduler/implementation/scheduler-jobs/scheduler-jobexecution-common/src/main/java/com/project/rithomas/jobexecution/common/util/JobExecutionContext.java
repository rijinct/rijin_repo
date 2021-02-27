
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowType;
import com.project.rithomas.sdk.workflow.deployment.util.DeployerConstants;

public class JobExecutionContext extends WorkFlowContext {

	public static final String JOB_NAME = "JOB_NAME";

	public static final String POSTGRESURL = "POSTGRESURL";

	public static final String POSTGRESDRIVER = "POSTGRESDRIVER";

	public static final String HIVEURL = "HIVEURL";

	public static final String HIVEDRIVER = "HIVEDRIVER";

	public static final String USERNAME = "USERNAME";

	public static final String PASSWORD = "PASSWORD";

	public static final String SOURCEJOBID = "SOURCEJOBID";

	public static final String SOURCE_JOB_NAME = "SOURCE_JOB_NAME";

	public static final String SOURCEJOBTYPE = "SOURCEJOBTYPE";

	public static final String SOURCEPARTCOLUMN = "SOURCEPARTCOLUMN";

	public static final String MAXVALUE = "MAXVALUE";

	public static final String MAXVALUE_UNIXTIME = "MAXVALUE_UNIXTIME";

	public static final String SOURCERETENTIONDAYS = "SOURCERETENTIONDAYS";

	public static final String SOURCEPLEVEL = "SOURCEPLEVEL";

	public static final String ID = "ID";

	public static final String LB = "LB";

	public static final String UB = "UB";

	public static final String NEXT_LB = "NEXT_LB";

	public static final String JOBTYPE = "JOBTYPE";

	public static final String PLEVEL = "PLEVEL";

	public static final String ALEVEL = "ALEVEL";

	public static final String RETENTIONDAYS = "RETENTIONDAYS";

	public static final String RETURN = "RETURN";

	public static final String SEQUENCE = "SEQUENCE";

	public static final String ENTITY_MANAGER = "ENTITY_MANAGER";

	public static final String SQL = "SQL";

	public static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";

	public static final String AGGDONEBOUND = "AGGDONEBOUND";

	public static final String QUERYDONEBOUND = "QUERYDONEBOUND";

	public static final String DEPLOY_RITHOMAS_TARGET = "DEPLOY_RITHOMAS_TARGET";

	public static final String HIVE_HOST = "HIVE_HOST";

	public static final String HIVE_PORT = "HIVE_PORT";

	public static final String HIVE_DBNAME = "HIVE_DBNAME";

	public static final String HIVE_USER = "HIVE_USER";

	public static final String HIVE_PASSWORD = "HIVE_PASSWORD";

	public static final String STATUS = "STATUS";

	public static final String AGGREGATIONDONEDATE = "AGGREGATIONDONEDATE";

	public static final String DESCRIPTION = "DESCRIPTION";

	public static final String TARGET = "TARGET";

	public static final String PARTITION_COLUMN = "PARTITION_COLUMN";

	public static final String HIVE_PARTITION_COLUMN = "HIVE_PARTITION_COLUMN";

	public static final String ETL_STATUS_SEQ = "ETL_STATUS_SEQ";

	public static final String TIMEZONEID = "GMT+5:30";

	public static final String DEPLOY_RITHOMAS_TARGET_VERSION = "DEPLOY_RITHOMAS_TARGET_VERSION";

	public static final String HADOOP_NAMENODE_IP = "HADOOP_NAMENODE_IP";

	public static final String HADOOP_NAMENODE_PORT = "HADOOP_NAMENODE_PORT";

	public static final String NAMENODE_IP = "NAMENODE_IP";

	public static final String NAMENODE_PORT = "NAMENODE_PORT";

	public static final String SOURCE_TABLE_NAME = "SOURCE";

	public static final String IMPORT_DIR_PATH = "IMPORT_DIR_PATH";

	public static final String ERROR_DESCRIPTION = "ERROR_DESCRIPTION";

	public static final String COMMIT_DONE = "COMMIT_DONE";

	public static final String PREV_STATUS = "PREV_STATUS";

	public static final String IMPORT_PATH = "IMPORT_PATH";

	public static final String TRUE = "TRUE";

	public static final String CONTINUOUS_LOADING = "CONTINUOUS_LOADING";

	public static final String STAGE_RETENTIONDAYS = "STAGE_RETENTIONDAYS";

	public static final String SOURCEJOBLIST = "SOURCEJOBLIST";

	public static final String PROCESS_MONITOR_TABLE_NAME = "process_monitor";





	public static final List<String> DATA_CP_NE_JOBID = new ArrayList<String>();
	{
		DATA_CP_NE_JOBID.add("Perf_DATA_CP_NE_SEGG_1_15MIN_AggregateJob");
		DATA_CP_NE_JOBID.add("Perf_DATA_CP_NE_SEGG_1_1_HOUR_AggregateJob");
	}

	public static final List<String> DATA_CP_JOBID = new ArrayList<String>();
	{
		DATA_CP_JOBID.add("Perf_DATA_CP_HVC_HS_SEGG_1_15MIN_AggregateJob");
		DATA_CP_JOBID.add("Perf_DATA_CP_HVC_HS_SEGG_1_1_HOUR_AggregateJob");
		DATA_CP_JOBID.add("Perf_DATA_CP_SEGG_1_HOUR_AggregateJob");
	}

	public static final List<String> CEI_O_JOBID = new ArrayList<String>();
	{
		CEI_O_JOBID.add("Perf_CEI_O_INDEX_1_HOUR_AggregateJob");
		CEI_O_JOBID.add("Perf_CEI_O_INDEX_1_DAY_AggregateJob");
		CEI_O_JOBID.add("Perf_CEI_O_INDEX_1_WEEK_AggregateJob");
		CEI_O_JOBID.add("Perf_CEI_O_INDEX_1_MONTH_AggregateJob");
	}

	public static final Map<String, String> CEI_O_SRC_JOB_ID = new HashMap<String, String>();
	{
		CEI_O_SRC_JOB_ID.put("Perf_CEI_O_INDEX_1_HOUR_AggregateJob",
				"Perf_CEI_INDEX_1_HOUR_AggregateJob");
		CEI_O_SRC_JOB_ID.put("Perf_CEI_O_INDEX_1_DAY_AggregateJob",
				"Perf_CEI_INDEX_1_DAY_AggregateJob");
		CEI_O_SRC_JOB_ID.put("Perf_CEI_O_INDEX_1_WEEK_AggregateJob",
				"Perf_CEI_INDEX_1_WEEK_AggregateJob");
		CEI_O_SRC_JOB_ID.put("Perf_CEI_O_INDEX_1_MONTH_AggregateJob",
				"Perf_CEI_INDEX_1_MONTH_AggregateJob");
	}

	public static final String RITHOMAS = "RITHOMAS";



	public static final String GN_LOAD_JOB = "Usage_GNCP_1_LoadJob";










	public static final String REAGG_JOB_AGGJOB_MAP = "REAGG_JOB_AGGJOB_MAP";

	public static final String DEPENDENT_AGG_JOBS = "DEPENDENT_AGG_JOBS";

	public static final String PROFILE_CALC_JOB_DETAILS = "PROFILE_CALCULATION_JOB_DETAILS";

	public static final String PROFILE = "Profile-";

	public static final String SOURCE_INDEX = "SOURCE_INDEX";

	public static final String SQOOP_QUERY = "SQOOP_QUERY";

	public static final String PROFILE_INTERVALS = "PROFILE_INTERVALS";

	public static final String JOB_CLEANUP_ETL_STATUS = "JOB_CLEANUP_ETL_STATUS";

	public static final String ETL_STATUS_DAYS_KEPT = "ETL_STATUS_DAYS_KEPT";

	public static final String MIN_15 = "15MIN";

	public static final String HOUR = "HOUR";

	public static final String RAW = "RAW";

	public static final String DAY = "DAY";

	public static final String WEEK = "WEEK";

	public static final String MONTH = "MONTH";

	public static final String YEAR = "YEAR";

	public static final String LB_INIT = "LB_INIT";

	public static final String COUNT_FILES = "COUNT_FILES";

	public static final String INPUT_COUNT_MAP = "INPUT_COUNT_MAP";

	public static final String PROCESS_MONITOR_TABLE_LOCATION = "PROCESS_MONITOR_TABLE_LOCATION";

	public static final String TRIGGERED_MAP_JOBS_LIST = null;

	public static final String IS_INTERRUPTED = "false";

	public static final String TABLE_COLUMNS = "TABLE_COLUMNS";

	public static final String UNIQUE_KEY = "UNIQUE_KEY";

	public static final String FLAG = "FLAG";

	public static final String COMMON_ADAP_ID = "COMMON";

	public static final List<String> NO_PKEY_TABLES = new ArrayList<String>();
	{
		NO_PKEY_TABLES.add("ES_CLEAR_CODE_1");
	}

	public static final String ES_LOCATION_JOBID = "Entity_LOCATION_1_CorrelationJob";

	public static final String ES_OPERATOR_JOBID = "Entity_OPERATOR_1_CorrelationJob";

	public static final String ES_CLEAR_CODE_GROUP_JOBID = "Entity_CLEAR_CODE_GROUP_1_CorrelationJob";

	public static final String ES_TERMINAL_GROUP_JOBID = "Entity_TERMINAL_GROUP_1_CorrelationJob";

	public static final String ES_SUBSCRIBER_GROUP_JOBID = "Entity_SUBSCRIBER_GROUP_1_CorrelationJob";

	public static final Map<String, List<String>> JOB_TABLE_MAP = new HashMap<String, List<String>>();
	static {
		JOB_TABLE_MAP.put(ES_LOCATION_JOBID,
				new ArrayList<String>(Arrays.asList("ES_REGION_1", "ES_LOC_1",
						"ES_LOCATION_1", "ES_CITY_1")));
		JOB_TABLE_MAP.put(ES_TERMINAL_GROUP_JOBID, new ArrayList<String>(
				Arrays.asList("ES_TERMINAL_1", "ES_TERMINAL_GROUP_1")));
		JOB_TABLE_MAP.put(ES_CLEAR_CODE_GROUP_JOBID, new ArrayList<String>(
				Arrays.asList("ES_CLEAR_CODE_1", "ES_CLEAR_CODE_GROUP_1")));
	}

	public static final List<String> FREQ_LEVELS = new ArrayList<String>();
	static {
		FREQ_LEVELS.add("HOUR");
		FREQ_LEVELS.add("DAY");
	}

	public static final String JOB_WS_CACHE_CLEANUP_DAY = "JOB_WS_CACHE_CLEANUP";

	public static final String JOB_WS_CACHE_CLEANUP_HOUR = "JOB_WS_CACHE_CLEANUP_HOUR";

	public static final String AGGREGATION = "AGGREGATION";

	@Override
	protected WorkFlowType getWorkFlowType() {
		return WorkFlowType.JOB_EXECUTION;
	}

	public static final DateTimeFormatter formatter = DateTimeFormat
			.forPattern(DATEFORMAT);

	public static final String DROP_PART = "DROP_PART";

	public static final String AGG_MANAGER = "AGG_MANAGER";

	public static final String CALLED_FROM = "CALLED_FROM";

	public static final String JOB_WS_SCHEDULED_CACHE_WEEK = "JOB_WS_SCHEDULED_CACHE_WEEK";

	public static final String JOB_WS_SCHEDULED_CACHE_MONTH = "JOB_WS_SCHEDULED_CACHE_MONTH";

	public static final String JOB_WS_SCHEDULED_CACHE_DAY = "JOB_WS_SCHEDULED_CACHE_DAY";

	public static final String THRESHOLDVALUE = "UB_THRESHOLD";


	public static final String TIMEZONE_INPUT_COUNT_MAP = "TIMEZONE_INPUT_COUNT_MAP";

	public static final String UB_DST_COUNT = "UB_DST_COUNT";

	public static final String VALID_FOR_REAGG = "VALID_FOR_REAGG";

	public static final String AUTO_TRIGGER_ENABLED = "AUTO_TRIGGER_ENABLED";

	public static final String QS_AUTO_TRIGGER_ENABLED = "QS_AUTO_TRIGGER_ENABLED";

	public static final String DUPLICATE_FILTER_ENABLED = "DUPLICATE_FILTER_ENABLED";

	public static final String DIM_VALS_JOB = "Entity_DIM_VALS_1_CorrelationJob";

	public static final String POSTGRES_LOADING_ENABLED = "POSTGRES_LOADING_ENABLED";

	public static final String DISTINCT_SUBSCRIBER_COUNT = "DISTINCT_SUBSCRIBER_COUNT";

	public static final String ADAPTATION_ID_VERSION = "$ADAP_ID_VER";

	public static final String STAGE_TYPE = "$STAGE_TYPE";

	public static final String TIMEZONE_REGION = "$REGION";

	public static final String KEY_SEPARATOR = ":";

	public static final String USAGE_SPECIFICATION_ID_VERSION = "$USAGE_ID_VER";

	public static final String DATA_AVBL_CACHE_KEY = ADAPTATION_ID_VERSION
			+ KEY_SEPARATOR + USAGE_SPECIFICATION_ID_VERSION + KEY_SEPARATOR
			+ LOWER_BOUND + KEY_SEPARATOR + STAGE_TYPE;

	public static final String DATA_AVBL_CACHE_KEY_TIMEZONE = ADAPTATION_ID_VERSION
			+ KEY_SEPARATOR + USAGE_SPECIFICATION_ID_VERSION + KEY_SEPARATOR
			+ LOWER_BOUND + KEY_SEPARATOR
			+ AddDBConstants.TIMEZONE_PARTITION_COLUMN + "=" + TIMEZONE_REGION
			+ KEY_SEPARATOR + STAGE_TYPE;

	public static final String INTERRUPTED = "INTERRUPTED";



	public static final String SMS_CLEAR_CODE_JOBNAME = "Entity_SMS_CLEAR_CODE_1_CorrelationJob";

	public static final String CLEAR_CODE_JOBNAME = "Entity_CLEAR_CODE_1_CorrelationJob";

	public static final String CLEAR_CODE_GROUP_JOBNAME = "Entity_CLEAR_CODE_GROUP_1_CorrelationJob";

	public static final String SMS_CLEAR_CODE_TABLENAME = "es_sms_clear_code_1";

	public static final String CLEAR_CODE_TABLENAME = "es_clear_code_1";

	public static final String SKIP_TZ_REGION = "SKIP_TZ_REGION";




	public static final String BASH_PROFILE_EXEC_COMMAND = ". ~/.bash_profile;";


	public static final String YES = "YES";

	public static final String JOB_DESCRIPTION = "JOB_DESCRIPTION";

	public static final String REGION_TIMEZONE_OFFSET_MAP = "REGION_TIMEZONE_OFFSET_MAP";


	public static final String TIME_ZONE_LB = "LB_";

	public static final String TIME_ZONE_UB = "UB_";

	public static final String MAP_FOR_DEPENDENT_JOBS = "MAP_FOR_DEPENDENT_JOBS";

	public static final String MAP_FROM_SOURCE_JOB = "MAP_FROM_SOURCE_JOB";

	public static final String IMPORT = "Import-";

	public static final String KPI = "KPI-";

	public static final String THRESHOLD = "Threshold-";


	public static final String DEFAULT_TIMEZONE = "Default";

	public static final String MAX_DT_PARTITION = "9999999999999";

	public static final String DEGREE_OF_PARALLELISM = "DEGREE_OF_PARALLELISM";

	public static final String WORK_DIR_PATH_LIST = "WORK_DIR_PATH_LIST";

	public static final String APPENDERS_PARALLEL_LOADING = "APPENDERS_PARALLEL_LOADING";



	public static final String ENTITY_SUBS_GROUP_OTHERS_1_CORRELATIONJOB = "Entity_SUBS_GROUP_OTHERS_1_CorrelationJob";

	public static final String QS_LOWER_BOUND = "QS_LB";

	public static final String QS_UPPER_BOUND = "QS_UB";

	public static final List<String> QS_JOB_INTERVALS = Arrays.asList(
			DeployerConstants.DAY_INTERVAL, DeployerConstants.WEEK_INTERVAL,
			DeployerConstants.MONTH_INTERVAL);

	public static final List<String> EXPORT_JOB_INTERVALS_FOR_FILEMERGE = Arrays
			.asList(RAW, MIN_15, HOUR);

	public static final String S1U_1_LOAD_JOB = "Usage_S1U_1_LoadJob";

	public static final String DQI_ENABLED = "DQI_ENABLED";

	public static final String TOTAL_COUNT = "TOTAL_COUNT";


	public static final String INTEGER = "INTEGER";

	public static final String STRING = "STRING";

	public static final String HIVE_DB = "HIVE";

	public static final String POSTGRES_DB = "POSTGRES";



	public static final String CACHE_LOADING_PARALLELISM = "CACHE_LOADING_PARALLELISM";

	public static final String ALERT_ENRICHMENT = "ALERT_ENRICHMENT";



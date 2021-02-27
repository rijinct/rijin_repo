
package com.project.rithomas.jobexecution.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DistinctSubscriberCountUtil;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.ext.adddb.workflow.generator.util.HiveGeneratorUtil;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.corelation.query.EntitySpecificationQuery;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.model.utils.ModelUtil;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class GetJobMetadata extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(GetJobMetadata.class);

	private List<String> sourceJobIdList = new ArrayList<>();
	
	private String jobName;

	private static final String DATA_SOURCE_REGION_QUERY = "select distinct region_id from saidata.es_data_src_rgn_info_1";

	private static final String CUTOFF_VOLUME_2G = "CUTOFF_VOLUME_2G";

	private static final String CUTOFF_VOLUME_3G = "CUTOFF_VOLUME_3G";

	private static final String CUTOFF_VOLUME_4G = "CUTOFF_VOLUME_4G";

	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		jobName = (String) context.getProperty(JobExecutionContext.JOB_NAME);
		boolean success = false;
		String sourceJobName = null;
		String sourceJobType = null;
		int regionCntInBoundary = 0;
		try {
			JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
			JobDictionary jobDictionary = jobDictionaryQuery.retrieve(jobName);
			String jobType = jobDictionary.getTypeid().getJobtypeid();
			BoundaryQuery boundaryQuery = new BoundaryQuery();
			ReConnectUtil reconnectUtil = new ReConnectUtil();
			context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT,
					reconnectUtil.reconnectCount());
			context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL,
					reconnectUtil.reconnectWaitTime());
			List<Boundary> boundaryList = boundaryQuery
					.retrieveByJobId(jobName);
			if (!boundaryList.isEmpty()) {
				Boundary boundary = boundaryList.get(0);
				sourceJobName = boundary.getSourceJobId();
				sourceJobType = boundary.getSourceJobType();
				context.setProperty(JobExecutionContext.SOURCEJOBID,
						sourceJobName);
				context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
						sourceJobType);
				context.setProperty(JobExecutionContext.SOURCEPARTCOLUMN,
						boundary.getSourcePartColumn());
				context.setProperty(JobExecutionContext.MAXVALUE,
						boundary.getMaxValue());
			} else if (!JobTypeDictionary.ENTITY_JOB_TYPE
					.equalsIgnoreCase(jobType)
					&& !JobTypeDictionary.PROFILE_JOB_TYPE
							.equalsIgnoreCase(jobType)
					&& !JobTypeDictionary.QS_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
				throw new WorkFlowExecutionException(
						"The boundary value for jobid " + jobName
								+ " is null as the job is of type : "
								+ jobType);
			}
			// Fetching job properties for job
			getDbUrlFromSettingsFile(context);
			context.setProperty(JobExecutionContext.ADAPTATION_ID,
					jobDictionary.getAdaptationId());
			context.setProperty(JobExecutionContext.ADAPTATION_VERSION,
					jobDictionary.getAdaptationVersion());
			context.setProperty(JobExecutionContext.JOB_DESCRIPTION,
					jobDictionary.getDescription());
			context.setProperty(JobExecutionContext.EXPORT_DIRECTORY,
					HiveTableQueryUtil.getHiveExportDirectory());
			context.setProperty(JobExecutionContext.JOB_START_DATE,
					DateFunctionTransformation.getInstance()
							.getFormattedDate(new Date()));
			List<JobProperty> jobPropList = getJobProp(jobName, context);
			LOGGER.info("Getting Job Properties For Job: {}", jobName);
			context.setProperty(JobExecutionContext.JOBTYPE, jobType);
			if (!jobPropList.isEmpty()) {
				for (JobProperty jobProp : jobPropList) {
					context.setProperty(jobProp.getJobPropPK().getParamname(),
							jobProp.getParamvalue());
					LOGGER.info(jobProp.getJobPropPK().getParamname() + " : {}",
							jobProp.getParamvalue());
				}
			} else {
				if (JobTypeDictionary.QS_JOB_TYPE.equals(jobType)) {
					LOGGER.info(
							"Job property list is empty. But ignored as the job type is : {}",
							jobType);
				} else {
					throw new WorkFlowExecutionException(
							"The job property list for jobid " + jobName
									+ " is null");
				}
			}
			// Fetching job properties for source jobs
			if (JobExecutionContext.DISTINCT_SUBSCRIBER_COUNT
					.equalsIgnoreCase(jobName)) {
				LOGGER.debug("when job is distinct subscriber count");
				List<String> sourceTables = DistinctSubscriberCountUtil
						.getSourceTableNames(context);
				if (sourceTables != null && !sourceTables.isEmpty()) {
					sourceJobIdList = DistinctSubscriberCountUtil
							.getSourceJobIds(sourceTables);
				}
			} else {
				sourceJobIdList = boundaryQuery.getSourceJobIds((String) context
						.getProperty(JobExecutionContext.JOB_NAME));
			}
			context.setProperty(JobExecutionContext.SOURCEJOBLIST,
					sourceJobIdList);
			LOGGER.debug("sourcejoblist in getmetadata:{}", sourceJobIdList);
			// getting deployement properties
			GetDBResource.getInstance()
					.retrievePostgresObjectProperties(context);
			Map<String, List<String>> sourceJobMap = new HashMap<>();
			for (String sourceJob : sourceJobIdList) {
				if (sourceJob != null && !sourceJob.isEmpty()) {
					jobPropList = getJobProp(sourceJob, context);
					if (!jobPropList.isEmpty()) {
						for (JobProperty jobProp : jobPropList) {
							context.setProperty(
									sourceJob + "_"
											+ jobProp.getJobPropPK()
													.getParamname(),
									jobProp.getParamvalue());
							LOGGER.debug("paramname : {}, param value : {}",
									sourceJob + "_"
											+ jobProp.getJobPropPK()
													.getParamname(),
									jobProp.getParamvalue());
						}
					} else {
						if (JobExecutionContext.DISTINCT_SUBSCRIBER_COUNT
								.equalsIgnoreCase(jobName)) {
							LOGGER.warn(
									"The job property list for source jobid  "
											+ sourceJob
											+ " is null hence avoiding this job while calculating distinct subs count");
						} else {
							throw new WorkFlowExecutionException(
									"The job property list for source jobid  "
											+ sourceJob + " is null");
						}
					}
					if (((String) context
							.getProperty(JobExecutionContext.JOBTYPE))
									.equalsIgnoreCase(
											JobTypeDictionary.PERF_JOB_TYPE)
							&& TimeZoneUtil.isTimeZoneEnabled(context)) {
						List<String> sourceIdList = new ArrayList<>();
						sourceJobMap.put(sourceJob,
								getSourceJobList(sourceJob, sourceIdList));
					}
				}
			}
			LOGGER.debug("Base source job map: {}", sourceJobMap);
			context.setProperty(JobExecutionContext.BASE_SOURCE_JOB_MAP,
					sourceJobMap);
			// getting the sql_to_exeute for the job
			getSqlToExecute(context);
			// getting run time property for query Time out
			getRunTimeProp(context);
			setArchivingDaysForAggJobs(context, jobName, jobType);
			Map<String, String> regionZoneIdMap = TimeZoneUtil
					.getRegionZoneIdMapping(context);
			context.setProperty(JobExecutionContext.REGION_ZONEID_MAPPING,
					regionZoneIdMap);
			// getting reagg config for the usage job also, to get the late data
			// delay and late data limit configurations
			if (JobTypeDictionary.USAGE_JOB_TYPE.equals(jobType)) {
				getReaggregationConfig(context);
			}
			if (JobTypeDictionary.PERF_REAGG_JOB_TYPE.equals(jobType)) {
				getReaggregationConfig(context);
				String perfJobName = (String) context
						.getProperty(JobExecutionContext.PERF_JOB_NAME);
				jobPropList = getJobProp(perfJobName, context);
				if (!jobPropList.isEmpty()) {
					for (JobProperty jobProp : jobPropList) {
						context.setProperty(
								perfJobName + "_"
										+ jobProp.getJobPropPK().getParamname(),
								jobProp.getParamvalue());
					}
				}
			}
			// get primary key columns and all columns for dimension jobs
			if (JobTypeDictionary.ENTITY_JOB_TYPE.equals(jobType)
					&& !JobExecutionContext.COMMON_ADAP_ID
							.equals(jobDictionary.getAdaptationId())) {
				getColumnsForTable(context);
			}
			// Time zone changes to insert record into boundary with region id
			if (TimeZoneUtil.isTimeZoneEnabled(context)) {
				QueryExecutor queryExecutor = new QueryExecutor();
				List<Object> resultSet = queryExecutor
						.executeMetadatasqlQueryMultiple(
						DATA_SOURCE_REGION_QUERY, context);
				List<String> regions = new ArrayList<>();
				List<String> regionsForAgg = new ArrayList<>();
				if (resultSet != null && !resultSet.isEmpty()) {
					for (Object result : resultSet) {
						if (!regions.contains(result.toString())) {
							regions.add(result.toString());
							regionsForAgg.add(result.toString());
						}
					}
				}
				context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
						regionsForAgg);
				// Adding default partition
				if (!regions.contains("Unknown")) {
					regions.add("Unknown");
				}
				LOGGER.debug(" data source regions list : {}", regions);
				context.setProperty(JobExecutionContext.TIME_ZONE_REGIONS,
						regions);
				regionCntInBoundary = getRegionCountFromBoundary(jobName,
						context);
				LOGGER.debug("count : {}", regionCntInBoundary);
				// when counts are not equal insert rows in boundary
				// table for the current job with the region which is
				// not inserted into
				// table
				if (JobTypeDictionary.USAGE_JOB_TYPE
						.equalsIgnoreCase(jobType)) {
					populateBoundaryData(jobName, regionCntInBoundary, regions,
							jobType, context);
				} else if (JobTypeDictionary.PERF_JOB_TYPE
						.equalsIgnoreCase(jobType)
						|| JobTypeDictionary.PERF_REAGG_JOB_TYPE
								.equalsIgnoreCase(jobType)
						|| JobTypeDictionary.TNP_IMPORT_JOB_TYPE
								.equalsIgnoreCase(jobType)
						|| JobTypeDictionary.ARCHIVING_JOB_TYPE
								.equalsIgnoreCase(jobType)) {
					if (TimeZoneUtil.isTimeZoneAgnostic(context)) {
						populateBoundaryDataForWeekAndMonthJob(jobName,
								context);
					} else {
						populateBoundaryData(jobName, regionCntInBoundary,
								regionsForAgg, jobType, context);
					}
				}
				TimeZoneUtil.setRegionTimeDiffOffset(context);
			}
			// TZ changes ends
			success = true;
			context.setProperty(JobExecutionContext.RETURN, 0);
		} catch (JobExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return success;
	}

	private List<String> getSourceJobList(String jobId, List<String> sourceIdList) {
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		List<String> tmpSourceList = boundaryQuery.getSourceJobIds(jobId);
		if (CollectionUtils.isEmpty(tmpSourceList)) {
			sourceIdList.add(jobId);
		} else {
			for (String source : tmpSourceList) {
				getSourceJobList(source, sourceIdList);
			}
		}
		return sourceIdList;
	}

	private void getDbUrlFromSettingsFile(WorkFlowContext context)
			throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL,
				HiveConfigurationProvider.getInstance()
						.shouldConnectToCustomDbUrl(
								(String) context.getProperty(
										JobExecutionContext.JOB_NAME),
								JobExecutionUtil.getExecutionEngine(context)));
	}

	private void populateBoundaryDataForWeekAndMonthJob(String jobName,
			WorkFlowContext context) throws JobExecutionException {
		QueryExecutor executor = new QueryExecutor();
		String region = JobExecutionContext.DEFAULT_TIMEZONE;
		String sqlToCheckBoundaryEntryForRegion = QueryConstants.GET_BOUNDARY_REGION_CHECK_QUERY
				.replace(QueryConstants.JOB_ID, jobName)
				.replace(QueryConstants.REGION_ID, region);
		Object[] boundaryRgnSelect = executor.executeMetadatasqlQuery(
				sqlToCheckBoundaryEntryForRegion, context);
		List<String> totalRgns = new ArrayList<>();
		totalRgns.add(region);
		if (boundaryRgnSelect == null) {
			populateBoundaryForOneRegion(jobName, totalRgns, context, executor);
		}
	}

	@SuppressWarnings("unused")
	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	// Added by rakekris to get all the table columns
	private void getColumnsForTable(WorkFlowContext context) {
		Map<String, String> tableColumnMap = new HashMap<>();
		Map<String, String> tableUniqueColumnMap = new HashMap<>();
		String tableNameFromContext = (String) context
				.getProperty(JobExecutionContext.SOURCE);
		LOGGER.debug("external source table:{}", tableNameFromContext);
		String[] sourceTables = tableNameFromContext.split(",");
		for (String tableName : sourceTables) {
			List<String> columns = new ArrayList<String>();
			List<String> uniqueColumns = new ArrayList<String>();
			LOGGER.debug("Retrieving column/s for the table:{}", tableName);
			List<String> esIDver = HiveGeneratorUtil
					.getSpecIDVerFromTabName(tableName);
			LOGGER.debug("esIDver details:{}", esIDver);
			EntitySpecificationQuery entitySpecificationQuery = new EntitySpecificationQuery();
			EntitySpecification entitySpecification = (EntitySpecification) entitySpecificationQuery
					.retrieveLatest(esIDver.get(0));
			if (entitySpecification != null) {
				Collection<EntitySpecificationCharacteristicUse> charSpecCollection = entitySpecification
						.getEntitySpecificationCharacteristicUse();
				List<EntitySpecificationCharacteristicUse> entitySpecificationCharacteristicUseList = new ArrayList<>(
						charSpecCollection);
				LOGGER.debug("entitySpecificationCharacteristicUseList:{}",
						entitySpecificationCharacteristicUseList);
				for (EntitySpecificationCharacteristicUse entitySpecCharUse : entitySpecificationCharacteristicUseList) {
					CharacteristicSpecification charSpec = entitySpecCharUse
							.getSpecChar();
					LOGGER.debug("charSpec for the src table:{}", charSpec);
					if ((charSpec.getAbstractType() == null
							|| (ModelUtil.SPEC_CHAR_TYPE_TRANSIENT)
									.equalsIgnoreCase(
											charSpec.getAbstractType()))
									&& !columns.contains(charSpec.getName())) {
							if ((ModelUtil.UNIQUE_CHAR
									.equalsIgnoreCase(charSpec.getUnik()))) {
								String uniqueColumn = charSpec
										.getDerivationFormula() != null
												? charSpec
														.getDerivationFormula()
												: charSpec.getName();
								uniqueColumns.add(uniqueColumn);
							}
							columns.add(charSpec.getName());
					}
				}
				LOGGER.debug("The columns of the external table:{} are {}",
						tableName, columns);
				LOGGER.debug("The primary keys of the external table:{} are {}",
						tableName, uniqueColumns);
			}
			tableColumnMap.put(tableName, getCommaSeparatedString(columns));
			tableUniqueColumnMap.put(tableName,
					getCommaSeparatedString(uniqueColumns));
			columns.clear();
			uniqueColumns.clear();
		}
		context.setProperty(JobExecutionContext.TABLE_COLUMNS, tableColumnMap);
		context.setProperty(JobExecutionContext.UNIQUE_KEY,
				tableUniqueColumnMap);
	}

	private String getCommaSeparatedString(List<String> columns) {
		StringBuilder sb = new StringBuilder();
		if (columns != null && !columns.isEmpty()) {
			for (int i = 1; i <= columns.size(); i++) {
				sb.append(columns.get(i - 1));
				if (i != columns.size()) {
					sb.append(",");
				}
			}
		}
		return sb.toString();
	}

	protected List<JobProperty> getJobProp(String jobName,
			WorkFlowContext context) {
		JobDictionaryQuery jobDictQuery = new JobDictionaryQuery();
		JobPropertyQuery jobPropQuery = new JobPropertyQuery();
		JobDictionary jobId = jobDictQuery.retrieve(jobName);
		List<JobProperty> jobPropList = new ArrayList<>();
		if (jobId != null) {
			if (this.jobName.equals(jobName)) {
				context.setProperty(JobExecutionContext.ADAPTATION_ID,
						jobId.getAdaptationId());
				context.setProperty(JobExecutionContext.ADAPTATION_VERSION,
						jobId.getAdaptationVersion());
			}
			context.setProperty(
					jobName + "_" + JobExecutionContext.ADAPTATION_ID,
					jobId.getAdaptationId());
			context.setProperty(
					jobName + "_" + JobExecutionContext.ADAPTATION_VERSION,
					jobId.getAdaptationVersion());
			jobPropList = jobPropQuery.retrieve(jobId.getId());
		}
		return jobPropList;
	}

	
	private void getSqlToExecute(WorkFlowContext context)
			throws JobExecutionException {
		GetDBResource.getInstance().retrievePostgresObjectProperties(context);
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobID = jobName;

		String sql = null;
		Object[] resultSet = fetchQueriesFromToExecute(jobID, context);
				
		if (ArrayUtils.isEmpty(resultSet)
				&& JobTypeDictionary.PERF_REAGG_JOB_TYPE.equals(jobType)) {
			String perfJobID = (String) context
					.getProperty(JobExecutionContext.PERF_JOB_NAME);
			LOGGER.info(
					"Job type is: {}, hence getting the query for the job: {}",
					jobType, perfJobID);
			resultSet = fetchQueriesFromToExecute(perfJobID, context);
		}
		
		if (resultSet != null) {
			if (resultSet[1] == null) {
				LOGGER.debug("Result set : {}", resultSet[0]);
				if (resultSet[0] instanceof String) {
					sql = (String) resultSet[0];
				} else {
					Clob clob = (Clob) resultSet[0];
					try {
						InputStream inputStream = clob.getAsciiStream();
						StringWriter stringWriter = new StringWriter();
						IOUtils.copy(inputStream, stringWriter);
						sql = stringWriter.toString();
					} catch (SQLException e) {
						throw new JobExecutionException(
								"Exception while getting to_execute. ", e);
					} catch (IOException e) {
						throw new JobExecutionException(
								"Exception while getting to_execute. ", e);
					}
				}
			} else {
				sql = (String) resultSet[1];
			}
		}
		context.setProperty(JobExecutionContext.SQL, sql);
		if (JobTypeDictionary.USAGE_JOB_TYPE.equals(jobType)) {
			String importDirSql = QueryConstants.USAGE_IMPORT_DIR
					.replace("$TNES_TYPE", "'" + jobName + "'");
			QueryExecutor queryExecutor = new QueryExecutor();
			resultSet = queryExecutor.executeMetadatasqlQuery(importDirSql,
					context);
			if (resultSet != null) {
				context.setProperty(JobExecutionContext.IMPORT_PATH,
						resultSet[0]);
			}
		}
	}

	protected Object[] fetchQueriesFromToExecute(String jobID,
			WorkFlowContext context) throws JobExecutionException {
		ST sqlTemplate = new ST(
				"select TO_EXECUTE,CUSTOM_EXECUTE from TO_EXECUTE where JOB_ID = \'<jobID>\'");
		sqlTemplate.add("jobID", jobID);
		LOGGER.debug("sqlToExecute: {}", sqlTemplate);
		QueryExecutor queryExecutor = new QueryExecutor();
		return queryExecutor.executeMetadatasqlQuery(sqlTemplate.render(),
				context);
	}

	private void getReaggregationConfig(WorkFlowContext context)
			throws JobExecutionException {
		StringBuilder offPeakHoursStrBuf = new StringBuilder("");
		QueryExecutor queryExecutor = new QueryExecutor();
		List<Object[]> resultSet = queryExecutor
				.executeMetadatasqlQueryMultiple(
				QueryConstants.REAGG_CONFIG_QUERY, context);
		if (resultSet != null && !resultSet.isEmpty()) {
			for (Object[] result : resultSet) {
				String paramName = result[0].toString();
				String paramValue = result[1].toString();
				LOGGER.debug(
						"Reaggregation config parameter name: {}, value: {}",
						paramName, paramValue);
				if (paramName != null && paramName
						.contains(JobExecutionContext.OFF_PEAK_HOUR)) {
					if (offPeakHoursStrBuf.length() == 0) {
						offPeakHoursStrBuf = offPeakHoursStrBuf
								.append(paramValue);
					} else {
						offPeakHoursStrBuf = offPeakHoursStrBuf
								.append("," + paramValue);
					}
				} else {
					context.setProperty(paramName, paramValue);
				}
			}
		}
		String offPeakHours = offPeakHoursStrBuf.toString();
		LOGGER.debug("Off peak hours: {}", offPeakHours);
		context.setProperty(JobExecutionContext.OFF_PEAK_HOURS, offPeakHours);
	}
	
	private void setArchivingDaysForAggJobs(WorkFlowContext context,
			String jobName, String jobType) {
		if (JobTypeDictionary.PERF_JOB_TYPE.equalsIgnoreCase(jobType)) {
			String archivingJob = jobName.replace("Aggregate", "Archiving");
			JobDictionaryQuery jdQuery = new JobDictionaryQuery();
			JobDictionary jd = jdQuery.retrieve(archivingJob);
			JobPropertyQuery jpQuery = new JobPropertyQuery();
			String archivingDays = jpQuery
					.retrieve(jd.getId(), JobExecutionContext.ARCHIVING_DAYS)
					.getParamvalue();
			context.setProperty(JobExecutionContext.ARCHIVING_DAYS,
					archivingDays);
		}
	}

	private void getRunTimeProp(WorkFlowContext context)
			throws JobExecutionException {
		String defaultMaxImsiApnExport = "4";
		Map<String, String> runtimeProp = RetrieveDimensionValues
				.getRuntimePropFromDB(context);
		String queryTimeOut = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.QUERY_TIMEOUT), "1800");
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT,
				Long.parseLong(queryTimeOut));
		LOGGER.debug("Query TimeOut Value: {}", queryTimeOut);
		String waitTime = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.WAIT_MINUTE), "5");
		context.setProperty(JobExecutionContext.WAIT_MINUTE,
				Long.parseLong(waitTime));
		LOGGER.debug("Wait minute for job: {}", waitTime);
		String threshold = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.THRESHOLDVALUE),
				"86400000");
		context.setProperty(JobExecutionContext.THRESHOLDVALUE,
				Long.parseLong(threshold) * 3600000);
		LOGGER.debug("Threshold value for UB calculation: {}", threshold);
		String maxApnExport = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.MAX_IMSI_APN_EXPORT),
				defaultMaxImsiApnExport);
		context.setProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT,
				Long.parseLong(maxApnExport));
		LOGGER.debug("Maximum imsi for APN Export: {}", maxApnExport);
		String cutOff2gVol = StringUtils
				.defaultIfEmpty(runtimeProp.get(CUTOFF_VOLUME_2G), "50");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_2G, cutOff2gVol);
		LOGGER.debug("Cut Off 2G Volume : {}", cutOff2gVol);
		String cutOff3gVol = StringUtils
				.defaultIfEmpty(runtimeProp.get(CUTOFF_VOLUME_3G), "200");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_3G, cutOff3gVol);
		LOGGER.debug("Cut Off 3G Volume : {}", cutOff3gVol);
		String cutOff4gVol = StringUtils
				.defaultIfEmpty(runtimeProp.get(CUTOFF_VOLUME_4G), "500");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_4G, cutOff4gVol);
		LOGGER.debug("Cut Off 4G Volume : {}", cutOff4gVol);
		String usageRetrigger = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.USAGE_RETRIGGER_DURATION),
				"24");
		context.setProperty(JobExecutionContext.USAGE_RETRIGGER_DURATION,
				usageRetrigger);
		LOGGER.debug("Usage Retrigger Duration in hours : {}", usageRetrigger);
		String weekStartDay = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.WEEK_START_DAY), "MONDAY");
		context.setProperty(JobExecutionContext.WEEK_START_DAY,
				weekStartDay.toUpperCase());
		LOGGER.debug("Week Start Day : {}", weekStartDay);
		String tempExpDir = StringUtils.defaultIfEmpty(
				runtimeProp.get(JobExecutionContext.TEMP_EXPORT_DIR), "/tmp/");
		context.setProperty(JobExecutionContext.TEMP_EXPORT_DIR, tempExpDir);
		String dqiThreshold = runtimeProp
				.get(JobExecutionContext.DQI_THRESHOLD);
		if (StringUtils.isNotEmpty(dqiThreshold)) {
			context.setProperty(JobExecutionContext.DQI_THRESHOLD,
					dqiThreshold);
		}
		context.setProperty(JobExecutionContext.ARCHIVING_REPLICATION_FACTOR,
				StringUtils.defaultIfEmpty(runtimeProp
								.get(JobExecutionContext.ARCHIVING_REPLICATION_FACTOR),
						"2"));
	}

	private int getRegionCountFromBoundary(String jobName,
			WorkFlowContext context) throws WorkFlowExecutionException {
		int cnt = 0;
		try {
			QueryExecutor executor = new QueryExecutor();
			String sql = "select count(region_id) from boundary where jobid = '"
					+ jobName + "'";
			Object[] resultSet = executor.executeMetadatasqlQuery(sql, context);
			if (resultSet != null) {
				if (resultSet[0] instanceof BigInteger) {
					cnt = ((BigInteger) resultSet[0]).intValue();
				} else if (resultSet[0] instanceof BigDecimal) {
					cnt = ((BigDecimal) resultSet[0]).intValue();
				}
			}
		} catch (JobExecutionException e) {
			LOGGER.error("Exception while executing query {}" + e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while checking job status ", e);
		}
		return cnt;
	}

	@SuppressWarnings("unchecked")
	private void populateBoundaryData(String jobName, int boundaryRgnCnt,
			List<String> totalRgns, String jobType, WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		int totalRgnCnt = totalRgns.size();
		List<String> jobDictSourceIds = new ArrayList<>();
		String boundarySourceId = null;
		String boundarySourceJobType = null;
		String boundarySourcePartColumn = null;
		try {
			QueryExecutor executor = new QueryExecutor();
			List<String> sourceJobList = (List<String>) context
					.getProperty(JobExecutionContext.SOURCEJOBLIST);
			LOGGER.debug("sourceJobList in populate boundary is: {}",
					sourceJobList);
			if (boundaryRgnCnt == 0) {
				// For the first time
				if (totalRgnCnt == 1) {
					success = populateBoundaryForOneRegion(jobName, totalRgns,
							context, executor);
				} else {
					// if more than one region, then delete the row and insert
					// row for each region in boundary and corresponding entry
					// in
					// job_dict_dependency table
					success = populateBoundaryForMoreThanOneRegion(jobName,
							totalRgns, jobType, context, jobDictSourceIds,
							boundarySourceId, boundarySourceJobType,
							boundarySourcePartColumn, executor);
				}
			}
			// when regions are added later
			if (!success) {
				populateBoundaryForPostRegionAddition(jobName, totalRgns,
						context, executor, sourceJobList);
			}
		} catch (JobExecutionException e) {
			LOGGER.error("Exception while executing query {}" + e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while checking job status ", e);
		}
	}

	private void populateBoundaryForPostRegionAddition(String jobName,
			List<String> totalRgns, WorkFlowContext context,
			QueryExecutor executor, List<String> sourceJobList)
			throws JobExecutionException {
		String boundarySourceId;
		String boundarySourceJobType;
		String boundarySourcePartColumn;
		Timestamp boundaryMaxValue;
		boundarySourceId = null;
		boundarySourceJobType = null;
		boundarySourcePartColumn = null;
		boundaryMaxValue = null;
		for (String region : totalRgns) {
			String sqlToCheckBoundaryEntryForRegion = QueryConstants.GET_BOUNDARY_REGION_CHECK_QUERY
					.replace(QueryConstants.JOB_ID, jobName)
					.replace(QueryConstants.REGION_ID, region);
			Object[] boundaryRgnSelect = executor.executeMetadatasqlQuery(
					sqlToCheckBoundaryEntryForRegion, context);
			String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='"
					+ jobName + "'";
			List<Object[]> boundarySelectResultList = executor
					.executeMetadatasqlQueryMultiple(sqlForBoundaryTable,
							context);
			if (!boundarySelectResultList.isEmpty()) {
				Object[] boundarySelectResultset = boundarySelectResultList
						.get(0);
				if (boundarySelectResultset != null) {
					LOGGER.debug(
							"boundarySelectResultset while adding region at runtime: {}",
							boundarySelectResultset);
					boundarySourceId = (String) boundarySelectResultset[0];
					boundarySourceJobType = (String) boundarySelectResultset[1];
					boundarySourcePartColumn = (String) boundarySelectResultset[2];
				}
			}
			if (boundaryRgnSelect == null) {
				// insert row into boundary with region and
				// add corresponding entry in job_dict_dependency when
				// multiple source
				String boundaryInsertQuery = QueryConstants.BOUNDARY_REGION_INSERT
						.replace(QueryConstants.JOB_ID, "'" + jobName + "'")
						.replace(QueryConstants.REGION_ID, "'" + region + "'");
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourceId,
						QueryConstants.SOURCE_JOB_NAMES);
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourceJobType,
						QueryConstants.SOURCE_JOB_TYPE);
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourcePartColumn,
						QueryConstants.SOURCE_PART_COLUMN);
				boundaryInsertQuery = replaceMaxValueInQuery(
						boundaryInsertQuery, boundaryMaxValue,
						QueryConstants.MAX_VALUE);
				LOGGER.debug(
						"Query to insert values into boundary table is : {}",
						boundaryInsertQuery);
				executor.executePostgresqlUpdate(boundaryInsertQuery, context);
				String boundaryJobId = "(select id from rithomas.boundary where jobid='"
						+ jobName + "' and region_id='" + region + "')";
				if (boundarySourceId == null) {
					// if multiple source
					for (String sourceJob : sourceJobList) {
						String sourceId = "(select id from rithomas.job_list where jobid='"
								+ sourceJob + "')";
						String jobDictDependencyInsertQuery = QueryConstants.JOB_DICT_DEPENDENCY_INSERT
								.replace(QueryConstants.BOUND_JOB_ID,
										boundaryJobId)
								.replace(QueryConstants.SOURCE_JOB_ID,
										sourceId);
						executor.executePostgresqlUpdate(
								jobDictDependencyInsertQuery, context);
					}
				}
			}
		}
	}

	private boolean populateBoundaryForMoreThanOneRegion(String jobName,
			List<String> totalRgns, String jobType, WorkFlowContext context,
			List<String> jobDictSourceIds, String boundarySourceId,
			String boundarySourceJobType, String boundarySourcePartColumn,
			QueryExecutor executor) throws JobExecutionException {
		boolean success;
		Timestamp boundaryMaxValue = null;
		String sqlForJobDictDepTable = "select source_jobid from job_dict_dependency where bound_jobid = ( select id from boundary where jobid ='"
				+ jobName + "' and region_id is null)";
		String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='"
				+ jobName + "' and region_id is null";
		String sqlToDeleteJobDictEntry = "delete from job_dict_dependency where bound_jobid = ( select id from boundary where jobid ='"
				+ jobName + "' and region_id is null)";
		String sqlToDeleteBoundaryEntry = "delete from boundary where jobid ='"
				+ jobName + "' and region_id is null";
		List jobDictSelectResultSet = executor.executeMetadatasqlQueryMultiple(
				sqlForJobDictDepTable, context);
		LOGGER.debug("jobDictSelectResultSet: {}", jobDictSelectResultSet);
		if (jobDictSelectResultSet != null
				&& !jobDictSelectResultSet.isEmpty()) {
			for (int i = 0; i < jobDictSelectResultSet.size(); i++) {
				BigDecimal result = (BigDecimal) jobDictSelectResultSet.get(i);
				jobDictSourceIds.add(result.toString());
			}
		}
		LOGGER.debug("source job ids : {}", jobDictSourceIds);
		Object[] boundarySelectResultset = executor
				.executeMetadatasqlQuery(sqlForBoundaryTable, context);
		if (boundarySelectResultset != null) {
			boundarySourceId = (String) boundarySelectResultset[0];
			boundarySourceJobType = (String) boundarySelectResultset[1];
			boundarySourcePartColumn = (String) boundarySelectResultset[2];
			boundaryMaxValue = (Timestamp) boundarySelectResultset[3];
		}
		executor.executePostgresqlUpdate(sqlToDeleteJobDictEntry, context);
		executor.executePostgresqlUpdate(sqlToDeleteBoundaryEntry, context);
		// to do: get regions from timzone_region table
		for (String region : totalRgns) {
			String sqlToCheckBoundaryEntryForRegion = QueryConstants.GET_BOUNDARY_REGION_CHECK_QUERY
					.replace(QueryConstants.JOB_ID, jobName)
					.replace(QueryConstants.REGION_ID, region);
			String sqlToCheckJobDictEntryForRegion = "select * from job_dict_dependency where bound_jobid = (select id from boundary where jobid='"
					+ jobName + "' and region_id='" + region + "')";
			Object[] boundaryRgnSelect = executor.executeMetadatasqlQuery(
					sqlToCheckBoundaryEntryForRegion, context);
			if (boundaryRgnSelect == null) {
				// insert row into boundary with region and
				// corresponding entry in job_dict_dependency
				String boundaryInsertQuery = QueryConstants.BOUNDARY_REGION_INSERT
						.replace(QueryConstants.JOB_ID, "'" + jobName + "'")
						.replace(QueryConstants.REGION_ID, "'" + region + "'");
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourceId,
						QueryConstants.SOURCE_JOB_NAMES);
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourceJobType,
						QueryConstants.SOURCE_JOB_TYPE);
				boundaryInsertQuery = replaceNullValueInQuery(
						boundaryInsertQuery, boundarySourcePartColumn,
						QueryConstants.SOURCE_PART_COLUMN);
				// If the job type is Loading then max value will be
				// null for each region boundary entry
				if (JobTypeDictionary.USAGE_JOB_TYPE
						.equalsIgnoreCase(jobType)) {
					boundaryMaxValue = null;
					boundaryInsertQuery = replaceMaxValueInQuery(
							boundaryInsertQuery, boundaryMaxValue,
							QueryConstants.MAX_VALUE);
				} else {
					boundaryInsertQuery = replaceMaxValueInQuery(
							boundaryInsertQuery, boundaryMaxValue,
							QueryConstants.MAX_VALUE);
				}
				LOGGER.debug(
						"Query to check if boundary entry already exists : {}",
						boundaryInsertQuery);
				executor.executePostgresqlUpdate(boundaryInsertQuery, context);
			}
			List<Object[]> jobDictRgnSelect = executor
					.executeMetadatasqlQueryMultiple(
							sqlToCheckJobDictEntryForRegion, context);
			if (boundarySourceId == null
					&& CollectionUtils.isEmpty(jobDictRgnSelect)
					&& CollectionUtils.isNotEmpty(jobDictSourceIds)) {
					LOGGER.debug("inserting into job_dict_dependency");
					for (String sourceId : jobDictSourceIds) {
						String boundaryJobId = "(select id from boundary where jobid='"
								+ jobName + "' and region_id='" + region + "')";
						String jobDictDependencyInsertQuery = QueryConstants.JOB_DICT_DEPENDENCY_INSERT
								.replace(QueryConstants.BOUND_JOB_ID,
										boundaryJobId)
								.replace(QueryConstants.SOURCE_JOB_ID,
										sourceId);
						LOGGER.debug("jobDictDependencyInsertQuery: {}",
								jobDictDependencyInsertQuery);
						executor.executePostgresqlUpdate(
								jobDictDependencyInsertQuery, context);
					}
			}
		}
		success = true;
		return success;
	}

	private boolean populateBoundaryForOneRegion(String jobName,
			List<String> totalRgns, WorkFlowContext context,
			QueryExecutor executor) throws JobExecutionException {
		boolean success;
		// only one region configured
		for (String region : totalRgns) {
			String sqlToUpdateBoundary = "update boundary set region_id='"
					+ region + "' where jobid='" + jobName + "'";
			executor.executePostgresqlUpdate(sqlToUpdateBoundary, context);
		}
		success = true;
		return success;
	}

	private String replaceNullValueInQuery(String query, String val,
			String constantVal) {
		String replacedQuery = null;
		if (val == null) {
			replacedQuery = query.replace(constantVal, "null");
		} else {
			replacedQuery = query.replace(constantVal, "'" + val + "'");
		}
		return replacedQuery;
	}

	private String replaceMaxValueInQuery(String query,
			Timestamp boundaryMaxValue, String constantVal) {
		String replacedQuery = null;
		if (boundaryMaxValue == null) {
			replacedQuery = query.replace(constantVal, "null");
		} else {
			replacedQuery = query.replace(constantVal,
					"'" + boundaryMaxValue + "'");
		}
		return replacedQuery;
	}
}

package com.project.rithomas.jobexecution.reaggregation;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ReaggCommonUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggCommonUtil.class);

	public static void getMinimumOfLoadTime(String jobId,
			QueryExecutor queryExecutor, WorkFlowContext context)
			throws JobExecutionException {
		String query = QueryConstants.GET_MIN_LOAD_TIME
				.replace(QueryConstants.PROVIDER_JOB, jobId);
		Object[] result = queryExecutor.executeMetadatasqlQuery(query, context);
		if (result != null) {
			context.setProperty(JobExecutionContext.MIN_LOAD_TIME,
					result[0].toString());
		}
	}

	public static List getReaggList(String jobId, QueryExecutor queryExecutor,
			WorkFlowContext context) throws JobExecutionException {
		String query = QueryConstants.REAGG_LIST_STATUS_QUERY
				.replace(QueryConstants.JOB_ID, jobId);
		return queryExecutor.executeMetadatasqlQueryMultiple(query, context);
	}

	public static String getSourceJobsInQuotes(List<String> sourceJobs) {
		StringBuilder sourceJobsInQuotesStrBuf = new StringBuilder("");
		for (Integer index = 0; index < sourceJobs.size(); index++) {
			String sourceJob = sourceJobs.get(index);
			sourceJobsInQuotesStrBuf = sourceJobsInQuotesStrBuf
					.append("'" + sourceJob + "'");
			if (index < sourceJobs.size() - 1) {
				sourceJobsInQuotesStrBuf = sourceJobsInQuotesStrBuf.append(",");
			}
		}
		return sourceJobsInQuotesStrBuf.toString();
	}

	public static void handleDependentJobs(String jobId,
			String sourceJobsInQuotes, Set<String> regionIds,
			WorkFlowContext context)
			throws JobExecutionException, WorkFlowExecutionException {
		constructMapForDependentQSJob(context);
		String dependentJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY
				.replace(QueryConstants.JOB_ID, jobId);
		if (CollectionUtils.isNotEmpty(regionIds)) {
			for (String id : regionIds) {
				dependentJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY_TZ
						.replace(QueryConstants.JOB_ID, jobId)
						.replace(QueryConstants.REGION_ID, id)
						.replace(QueryConstants.DEFAULT_PART,
								JobExecutionContext.DEFAULT_TIMEZONE);
				DependentJobUtil.checkForDependentJobs(dependentJobQuery,
						sourceJobsInQuotes, context);
			}
		} else {
			DependentJobUtil.checkForDependentJobs(dependentJobQuery,
					sourceJobsInQuotes, context);
		}
	}

	private static void constructMapForDependentQSJob(WorkFlowContext context) {
		Map<Object, Object> mapForDependentJobs = new HashMap<>();
		mapForDependentJobs.put(JobExecutionContext.QS_LOWER_BOUND,
				context.getProperty(JobExecutionContext.LB_QS_JOB));
		mapForDependentJobs.put(JobExecutionContext.QS_UPPER_BOUND,
				context.getProperty(JobExecutionContext.UB_QS_JOB));
		mapForDependentJobs.put(JobExecutionContext.SOURCE,
				context.getProperty(JobExecutionContext.TARGET));
		mapForDependentJobs.put(JobExecutionContext.SOURCEJOBTYPE,
				context.getProperty(JobExecutionContext.JOBTYPE));
		context.setProperty(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS,
				mapForDependentJobs);
		LOGGER.debug(
				"Adding the source table name : {} and source job type : {}",
				context.getProperty(JobExecutionContext.TARGET),
				context.getProperty(JobExecutionContext.JOBTYPE));
	}

	public static Long getRetentionDayValue(WorkFlowContext context) {
		String perfJobName = (String) context
				.getProperty(JobExecutionContext.PERF_JOB_NAME);
		String plevel = (String) context
				.getProperty(perfJobName + "_" + JobExecutionContext.PLEVEL);
		String retentionDays = (String) context.getProperty(
				perfJobName + "_" + JobExecutionContext.RETENTIONDAYS);
		Long retentionDayValue = DateFunctionTransformation
				.getDateBySubtractingDays(Integer.parseInt(retentionDays),
						context)
				.getTimeInMillis();
		return DateFunctionTransformation.getInstance()
				.getTrunc(retentionDayValue, plevel, (String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY));
	}

	public static boolean shouldTriggerNextLevel(
			DateFunctionTransformation dateTransformer, String aggLevel,
			String prevReportTime, String weekStartDay) {
		boolean trigger = true;
		Calendar prevReportTimeCal = new GregorianCalendar();
		prevReportTimeCal.setTime(dateTransformer.getDate(prevReportTime));
		if (JobExecutionContext.MIN_15.equalsIgnoreCase(aggLevel)
				|| JobExecutionContext.HOUR.equalsIgnoreCase(aggLevel)) {
			trigger = prevReportTimeCal.get(Calendar.HOUR_OF_DAY) == 0;
		}
		if (JobExecutionContext.DAY.equalsIgnoreCase(aggLevel)) {
			trigger = prevReportTimeCal.get(Calendar.DAY_OF_MONTH) == 1
					|| prevReportTimeCal
							.get(Calendar.DAY_OF_WEEK) == dateTransformer
									.getDayOfWeek(weekStartDay);
		}
		return trigger;
	}

	public static void dropPartitionifArchived(WorkFlowContext context,
			Long partitionValue) throws Exception {
		ArchivedPartitionHelper helper = new ArchivedPartitionHelper(context);
		Long archivingDateInMillis = (Long) context
				.getProperty(JobExecutionContext.ARCHIVING_DATE);
		if (helper.isPartitionArchived(partitionValue, archivingDateInMillis)) {
			String targetTable = (String) context
					.getProperty(JobExecutionContext.ARCHIVE_TARGET_TABLE);
			LOGGER.debug(
					"Target table: {} archived partition to be dropped: {}",
					targetTable, partitionValue);
			PartitionUtil.dropPartitionsBasedOnCondition(context, targetTable,
					partitionValue, CommonConstants.EQUAL);
			String tableLocation = getHDFSLocation(context, targetTable);
			deleteHDFSDirectory(context, partitionValue, tableLocation);
			LOGGER.info("Dropped Archived partition: {} from {}",
					partitionValue, targetTable);
		}
	}

	public static void deleteHDFSDirectory(WorkFlowContext context,
			Long partitionValue, String tableLocation) throws IOException {
		if (StringUtils.isNotEmpty(tableLocation)) {
			Path tablePath = new Path(tableLocation);
			FileSystem fileSystem = FileSystem
					.get(GetDBResource.getInstance().getHdfsConfiguration());
			PartitionUtil.deleteParticularDirInHDFS(tablePath, fileSystem,
					PartitionUtil.getPartitionColumn(context),
					partitionValue.toString());
		}
	}

	public static String getHDFSLocation(WorkFlowContext context,
			String targetTable) throws HiveQueryException {
		List<String> tableLocations = HiveTableQueryUtil
				.retrieveHDFSLocationGivenTableName(context, targetTable);
		return CollectionUtils.isNotEmpty(tableLocations)
				? tableLocations.get(0) : null;
	}

	public static void dropPartitions(WorkFlowContext context, String jobId,
			Long retentionDtValue) throws Exception {
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		Long endTime = getMaxEndTimeForDependentJobs(context, jobId,
				retentionDtValue);
		if (endTime != null) {
			if (endTime > retentionDtValue) {
				LOGGER.info("No partitions to drop in re-aggregation");
			} else {
				List<String> partitions = PartitionUtil.showPartitionsResult(
						context, targetTable, false, endTime, retentionDtValue);
				Set<Long> partitionsToDrop = getPartitionsToDrop(context,
						partitions);
				String tableLocation = getHDFSLocation(context, targetTable);
				for (Long partition : partitionsToDrop) {
					LOGGER.info("Dropping partition {}", partition);
					PartitionUtil.dropPartitionsBasedOnCondition(context,
							targetTable, partition, CommonConstants.EQUAL);
					deleteHDFSDirectory(context, partition, tableLocation);
				}
			}
		} else {
			LOGGER.info(
					"No re-aggregation pending for time period less than retention time. Hence dropping partitions will be handled by normal aggregation.");
		}
	}

	private static Set<Long> getPartitionsToDrop(WorkFlowContext context,
			List<String> partitions) {
		Set<Long> partitionsToDrop = new TreeSet<>();
		for (String partition : partitions) {
			Long partitionValue = Long.valueOf(PartitionUtil.getPartitionValue(
					partition, PartitionUtil.getPartitionColumn(context)));
			partitionsToDrop.add(partitionValue);
		}
		return partitionsToDrop;
	}

	public static Long getMaxEndTimeForDependentJobs(WorkFlowContext context,
			String jobId, Long retentionDtValue) throws JobExecutionException {
		DateFunctionTransformation dft = DateFunctionTransformation
				.getInstance();
		String retentionDate = dft.getFormattedDate(retentionDtValue);
		String query = QueryConstants.MAX_END_TIME_DEPENDENT_REAGG
				.replace(QueryConstants.JOB_ID, jobId)
				.replace("$RETENTION_DATE", retentionDate);
		Long maxEndTime = null;
		QueryExecutor queryExec = new QueryExecutor();
		Object[] result = queryExec.executeMetadatasqlQuery(query, context);
		if (result != null) {
			String endTimeString = result[0].toString();
			maxEndTime = dft.getDate(endTimeString).getTime();
		}
		LOGGER.debug("Max end time pending to be re-aggregated is {}",
				maxEndTime);
		return maxEndTime;
	}

	public static boolean populateArchiveInfo(WorkFlowContext context)
			throws JobExecutionException {
		ArchivedPartitionHelper helper = new ArchivedPartitionHelper(context);
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String aggLevel = (String) context
				.getProperty(JobExecutionContext.ALEVEL);
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		Integer archivingDays = helper.getArchivingDaysForJob(jobId);
		LOGGER.debug("Number of archiving days: {}", archivingDays);
		boolean isArchivingEabled = false;
		if (archivingDays > 0) {
			Long archivingDate = helper.getArchivingDate(archivingDays);
			context.setProperty(JobExecutionContext.ARCHIVING_DATE,
					archivingDate);
			String target = helper.isIntervalArchivedInSeparateTable(aggLevel)
					? helper.getCorrespondingArchivingTable(targetTable)
					: targetTable;
			LOGGER.debug("Archive target table: {}", target);
			context.setProperty(JobExecutionContext.ARCHIVE_TARGET_TABLE,
					target);
			isArchivingEabled = true;
		}
		return isArchivingEabled;
	}

	public static boolean isArchivedInDifferentTable(String aggLevel) {
		return JobExecutionContext.ARCHIVING_DAY_JOB_INTERVALS
				.contains(aggLevel);
	}

	public static void printRecordCount(String regionId, Long lb,
			WorkFlowContext context,
			ApplicationLoggerUtilInterface applicationLogger) {
		Long recordCount = 0L;
		HiveRowCountUtil rowCountUtil = new HiveRowCountUtil();
		try {
			if (regionId != null) {
				recordCount = rowCountUtil.getNumberOfRecordsInserted(context,
						regionId, applicationLogger);
				LOGGER.info(
						"Number of records inserted into partition: {}, for timezone: {} is {}",
						lb, regionId, recordCount);
			} else {
				if ((CommonConstants.TZ_OR_DQM_SUPPORT_YES
						.equalsIgnoreCase((String) context.getProperty(
								JobExecutionContext.TIME_ZONE_SUPPORT)))
						&& (TimeZoneUtil.isTimeZoneAgnostic(context))) {
					LOGGER.debug("Inside week adn month check and value: {}",
							JobExecutionContext.DEFAULT_TIMEZONE);
					recordCount = rowCountUtil.getNumberOfRecordsInserted(
							context, JobExecutionContext.DEFAULT_TIMEZONE,
							applicationLogger);
				} else {
					recordCount = rowCountUtil.getNumberOfRecordsInserted(
							context, null, applicationLogger);
				}
				LOGGER.info(
						"Number of records inserted into partition: {} is {}",
						lb, recordCount);
			}
		} catch (Exception e) {
			LOGGER.error("Error while getting record count inserted:{}", e);
		}
	}

	public static void updateReaggListToInitialized(WorkFlowContext context,
			ReaggregationListUtil reaggListUtil, String jobId,
			String sourceJobsInQuotes, String regionId)
			throws WorkFlowExecutionException {
		try {
			reaggListUtil.updateReaggregationList(context, jobId,
					sourceJobsInQuotes, null,
					CommonConstants.INITIALIZED_STATUS,
					CommonConstants.RUNNING_STATUS, regionId);
		} catch (JobExecutionException e) {
			UpdateJobStatus updateJobStatus = new UpdateJobStatus();
			updateJobStatus.updateETLErrorStatusInTable(e, context);
		}
	}
}

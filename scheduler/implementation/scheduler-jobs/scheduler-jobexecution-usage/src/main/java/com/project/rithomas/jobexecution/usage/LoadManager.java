
package com.project.rithomas.jobexecution.usage;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.ThreadContext;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.AddDBConstants;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.quartzdesk.api.agent.log.WorkerThreadLoggingInterceptorRegistry;

public class LoadManager extends AbortableWorkflowStep {

	private static final String TOTAL_DUP_COUNT = "TOTAL_DUP_COUNT";

	private static final String BOUND_LOADED_TIME = "BOUND_LOADED_TIME";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LoadManager.class);

	private static final String FILE_SEPARATOR = System
			.getProperty("file.separator", "/");

	private static final String UNDERSCORE = "_";

	private static final String PROCESS_MONITOR_RECORD = "REPORT_TIME,PROCESSED_RECS,0,TOTAL_RECS,0,DUPLICATE_RECS,LOAD_TIME,,TOPOLOGY_NAME,,,,,,,";

	private static final String STREAMTABLE_HINT = "STREAMTABLE";

	// Constant to append to each of the parallel threads number.
	private static final String PARALLEL_THREAD_APP_STR = "PARALLEL_THREAD";

	private long delay;

	Map<String, String> reaggJobBoundaryMap = null;

	Map<String, String> reaggJobAggLevelMap = null;

	Map<String, String> reaggJobAggJobMap = null;

	private boolean duplicateFilterEnabled = false;

	private Long overallTotalCount, overallDuplicateCount;

	private List<String> appendersForParallelLoading = null;

	private static final String SQL_EXCEPTION = "SQLException:";

	private static final String EXECUTION_EXCEPTION = "ExecutionException:";

	private static final String JOB_EXECUTION_EXCEPTION = "JobExecutionException:";

	private static final String INTERRUPTED_EXCEPTION = "InterruptedException:";

	private Thread jobThread;

	private String executionEngine;

	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		jobThread = Thread.currentThread();
		reaggJobBoundaryMap = new HashMap<String, String>();
		reaggJobAggLevelMap = new HashMap<String, String>();
		reaggJobAggJobMap = new HashMap<String, String>();
		LOGGER.info("Executing Loading Query");
		duplicateFilterEnabled = "YES".equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.DUPLICATE_FILTER_ENABLED));
		delay = (Long) context.getProperty(JobExecutionContext.QUERY_TIMEOUT);
		overallTotalCount = 0L;
		overallDuplicateCount = 0L;
		appendersForParallelLoading = new ArrayList<String>();
		executionEngine = JobExecutionUtil.getExecutionEngine(context);
		LOGGER.debug("Usage query will be run on {}", executionEngine);
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE,
				executionEngine);
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		try {
			Integer numOfParallelThreads = getDegreeOfParallelism(context);
			LOGGER.debug("Number of parallel threads : {}",
					numOfParallelThreads);
			// parallel loading changes for time zone
			if (TimeZoneUtil.isTimeZoneEnabled(context)) {
				success = loadDataWithTimezone(context);
			} // end of parallel loading changes for time zone
			else if (numOfParallelThreads != null
					&& (numOfParallelThreads > 1)) {
				success = loadDataWithParallelThreads(context);
			} else {
				Set<String> availableDirsInWork = (Set<String>) context
						.getProperty(JobExecutionContext.WORK_DIR_PATH);
				List<String> availableDirsInWorkList = new ArrayList<String>();
				availableDirsInWorkList.addAll(availableDirsInWork);
				context.setProperty(
						JobExecutionContext.WORK_DIR_PATH_LIST + UNDERSCORE
								+ null + UNDERSCORE + null,
						availableDirsInWorkList);
				Long totalCount = loadDataFromAvailableDir(context, null, null);
				if (totalCount > 0) {
					checkDependentReaggJobs(context);
					Map<String, String> reaggJobBoundaryMap = (Map<String, String>) context
							.getProperty(
									JobExecutionContext.REAGG_JOB_BOUNDARY_MAP);
					LOGGER.debug("Reaggregation jobs boundary map : {}",
							reaggJobBoundaryMap);
					if (reaggJobBoundaryMap != null
							&& !reaggJobBoundaryMap.isEmpty()) {
						// Do re aggregation related checks only when
						// data
						// source availability is YES
						Boolean isValidForReagg = (Boolean) context.getProperty(
								JobExecutionContext.VALID_FOR_REAGG);
						if (isValidForReagg == null) {
							// first time run
							LOGGER.debug("Validate Job for Reaggregations");
							validateForReaggregation(context);
							isValidForReagg = (Boolean) context.getProperty(
									JobExecutionContext.VALID_FOR_REAGG);
						}
						if (isValidForReagg) {
							checkIfJobNeedsToBeReaggregated(context, null);
						} else {
							LOGGER.info(
									"Source is set to NO. So not triggering Re-aggregate jobs.");
						}
					}
				}
				LOGGER.info(
						"Total number of records inserted: {}; Total duplicate records: {}",
						totalCount, context.getProperty(TOTAL_DUP_COUNT));
				context.setProperty(JobExecutionContext.DESCRIPTION,
						"Number of records inserted: " + totalCount
								+ "; Duplicate record count: "
								+ context.getProperty(TOTAL_DUP_COUNT));
				context.setProperty(JobExecutionContext.TOTAL_COUNT_INSERTED,
						totalCount);
				success = true;
			}
		} catch (JobExecutionException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					JOB_EXECUTION_EXCEPTION + e.getMessage(), e);
		} catch (Exception e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(e.getMessage(), e);
		}
		return success;
	}

	@SuppressWarnings("unchecked")
	private boolean loadDataWithParallelThreads(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		Set<String> availableDirsInWork = (Set<String>) context
				.getProperty(JobExecutionContext.WORK_DIR_PATH);
		LOGGER.debug("All the available directories in the work : {}",
				availableDirsInWork);
		Integer numOfParallelThreads = getDegreeOfParallelism(context);
		List<String> availableDirsInWorkList = new ArrayList<String>();
		availableDirsInWorkList.addAll(availableDirsInWork);
		Map<Integer, List<String>> splitListofDirs = splitDirectoriesForParallelism(
				numOfParallelThreads, availableDirsInWorkList);
		if (splitListofDirs != null && !splitListofDirs.isEmpty()) {
			ExecutorService executors = Executors
					.newFixedThreadPool(splitListofDirs.size());
			List<Future<Void>> futures = new ArrayList<Future<Void>>();
			try {
				runParallelThreads(context, executors, futures, null,
						splitListofDirs);
				for (Future<Void> future : futures) {
					future.get();
					success = true;
				}
				setTotalCountIntoContext(context);
			} catch (Exception e) {
				success = false;
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				LOGGER.error(
						"Exception while executing parallel threads : The job was aborted.",
						e);
				throw new WorkFlowExecutionException(
						SQL_EXCEPTION + e.getMessage(), e);
			} finally {
				// Once all the threads are run, shutting down the executors.
				executors.shutdown();
				if (!success) {
					updateErrorStatus(context);
				}
			}
		}
		return success;
	}

	private Map<Integer, List<String>> splitDirectoriesForParallelism(
			Integer numOfParallelThreads,
			List<String> availableDirsInWorkList) {
		Map<Integer, List<String>> splitListofDirs = new LinkedHashMap<Integer, List<String>>();
		if (numOfParallelThreads != null) {
			for (int i = 0; i < numOfParallelThreads; i++) {
				List<String> splitDirs = new ArrayList<String>();
				int j = i;
				while (j < availableDirsInWorkList.size()) {
					if (j < availableDirsInWorkList.size()) {
						splitDirs.add(availableDirsInWorkList.get(j));
						j += numOfParallelThreads;
					}
				}
				if (splitDirs != null && !splitDirs.isEmpty()) {
					splitListofDirs.put(i, splitDirs);
				}
			}
			LOGGER.debug("The list of directories split up : {}",
					splitListofDirs);
		}
		return splitListofDirs;
	}

	@SuppressWarnings("unchecked")
	private void checkIfJobNeedsToBeReaggregated(WorkFlowContext context,
			String region) throws JobExecutionException {
		DateFunctionTransformation dateFuncTransformer = DateFunctionTransformation
				.getInstance();
		UsageAggregationStatusUtil usageAggStatusUtil = new UsageAggregationStatusUtil();
		// Valid for re aggregation
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		Map<String, String> reaggJobAggLevelMap = (Map<String, String>) context
				.getProperty(JobExecutionContext.REAGG_JOB_AGGLEVEL_MAP);
		Map<String, String> reaggJobAggJobMap = (Map<String, String>) context
				.getProperty(JobExecutionContext.REAGG_JOB_AGGJOB_MAP);
		Map<Long, Long> partitionRecordCountMap = (Map<Long, Long>) context
				.getProperty(JobExecutionContext.PARTITION_COUNT_MAP);
		String lateDataDelay = (String) context
				.getProperty(JobExecutionContext.LATE_DATA_DELAY);
		String lateDataLimit = (String) context
				.getProperty(JobExecutionContext.LATE_DATA_LIMIT);
		LOGGER.debug("Late data delay: {}, late data limit: {}", lateDataDelay,
				lateDataLimit);
		
		Integer lateDataDelayInHours = null;
		if (lateDataDelay != null) {
			lateDataDelayInHours = Integer.parseInt(lateDataDelay);
		}
		Integer retentionDays = Integer.parseInt((String) context
				.getProperty(JobExecutionContext.RETENTIONDAYS));
		Long lowerDateWithRetentionSub = dateFuncTransformer
				.getTruncatedDateAfterSubtractingDays(retentionDays);
		LOGGER.debug(
				"Retention days value : {} and lower date value with retention days been subtracted : {}",
				retentionDays, lowerDateWithRetentionSub);
		Long lowerBoundForReaggCheck = calculateLBForReaggCheck(
				lateDataDelayInHours,
				(Long) context.getProperty(BOUND_LOADED_TIME));
		Integer lateDataLimitInt = 10;
		if (lateDataLimit != null) {
			lateDataLimitInt = Integer.parseInt(lateDataLimit);
		}
		 for(Entry<String, String> entry : reaggJobAggLevelMap.entrySet()) {
			String reaggJobName = entry.getKey();
			String boundaryOfAggJob = null;
			if (region == null || region.isEmpty()) {
				boundaryOfAggJob = reaggJobBoundaryMap.get(reaggJobName);
			} else {
				boundaryOfAggJob = reaggJobBoundaryMap
						.get(reaggJobName + ":" + region);
			}
			if (boundaryOfAggJob != null) {
				String reaggJobAggLevel = reaggJobAggLevelMap.get(reaggJobName);
				String aggJobName = reaggJobAggJobMap.get(reaggJobName);
				Long aggJobBoundaryInMillis = dateFuncTransformer
						.getDate(boundaryOfAggJob).getTime();
				Long nextBoundInMillis = dateFuncTransformer
						.getNextBound(aggJobBoundaryInMillis, reaggJobAggLevel)
						.getTimeInMillis();
				LOGGER.debug("For reagg job: {}, aggregation is done till: {}",
						reaggJobName, nextBoundInMillis);
				for (Entry<Long, Long> partition : partitionRecordCountMap
						.entrySet()) {
					if (partition.getKey() < nextBoundInMillis) {
						// usage data loaded partition is less than the boundary
						// of aggregation which represents late data.
						LOGGER.debug(
								"Loaded late data {} for the partition: {}",
								partition.getValue(), partition.getKey());
						if (partition.getKey() >= lowerBoundForReaggCheck
								&& partition
										.getKey() >= lowerDateWithRetentionSub) {
							LOGGER.info(
									"Data available for partition {} is within the delay time configured. Checking for re-aggregation.",
									partition);
							ReaggregationListUtil reaggListUtil = new ReaggregationListUtil();
							Long lowerBoundForReagg = dateFuncTransformer
									.getTrunc(partition.getKey(),
											reaggJobAggLevel,
											(String) context.getProperty(
													JobExecutionContext.WEEK_START_DAY),
											TimeZoneUtil.getZoneId(context,
													region));
							String reportTimeForReagg = dateFuncTransformer
									.getFormattedDate(
											new Date(lowerBoundForReagg));
							Calendar upperBoundCal = dateFuncTransformer
									.getNextBound(lowerBoundForReagg,
											reaggJobAggLevel);
							String upperBound = dateFuncTransformer
									.getFormattedDate(upperBoundCal.getTime());
							Long aggregatedCount = usageAggStatusUtil
									.getAggregatedRecordCount(context, jobId,
											aggJobName, reportTimeForReagg,
											upperBound, region);
							Long nonAggCount = usageAggStatusUtil
									.getNonAggregatedRecordCount(context, jobId,
											aggJobName, reportTimeForReagg,
											upperBound, region);
							if (nonAggCount != 0
									&& nonAggCount >= (aggregatedCount
											* lateDataLimitInt / 100.0)) {
								LOGGER.info(
										"Late data count {} greater than/equal to configured late data limit {}% of aggregated count {}",
										new Object[] { nonAggCount,
												lateDataLimitInt,
												aggregatedCount });
								String loadTime = dateFuncTransformer
										.getFormattedDate(new Date());
								if (!reaggListUtil.recordExists(context, jobId,
										reaggJobName, reportTimeForReagg,
										region)) {
									reaggListUtil.insertIntoReaggList(context,
											loadTime, reportTimeForReagg,
											reaggJobAggLevel, reaggJobName,
											jobId, region, "Initialized");
								}
							} else {
								if (nonAggCount == 0) {
									LOGGER.info(
											"No late data for tz: {} to be re-aggregated.",
											region == null ? "Default"
													: region);
								} else {
									LOGGER.info(
											"Late data count {} is less than {}% of the aggregated count {}.",
											new Object[] { nonAggCount,
													lateDataLimitInt,
													aggregatedCount });
								}
							}
						} else {
							LOGGER.info(
									"Partition: {} has late arrival data, but the data is less than lower bound which is calculated "
											+ "based on late data delay: {} or retention days : {}",
									new Object[] { partition, lateDataDelay,
											retentionDays });
						}
					}
				}
			}
		}
	}

	private Long calculateLBForReaggCheck(Integer lateDataDelayInHours,
			long calTimeinMillis) {
		// Changed the code to change the default late data
		// delay from retention days configured to 4hrs
		Integer defaultLateDataDelayInHrs = 4;
		Long lowerBoundForReaggCheck = null;
		if (lateDataDelayInHours != null) {
			lowerBoundForReaggCheck = calTimeinMillis
					- (1000L * 60 * 60 * lateDataDelayInHours);
		} else {
			lowerBoundForReaggCheck = calTimeinMillis
					- (1000L * 60 * 60 * defaultLateDataDelayInHrs);
		}
		LOGGER.debug("Lower bound based on configured late data delay: {}",
				lowerBoundForReaggCheck);
		return lowerBoundForReaggCheck;
	}

	@SuppressWarnings("unchecked")
	private Long loadDataFromAvailableDir(WorkFlowContext context,
			String region, String parallelThreadNum)
			throws WorkFlowExecutionException {
		UpdateBoundary updateBoundary = new UpdateBoundary();
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory
				.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		boolean firstRun = false;
		Long boundValue = null;
		int dirCount = 0;
		Long totalCount = 0L;
		Long totalDuplicateCount = 0L;
		String flag = "false";
		Calendar calendar = new GregorianCalendar();
		// Map to store the number of records inserted for each partition
		Map<Long, Long> partitionRecordCountMap = new HashMap<Long, Long>();
		List<String> availableDirsInWork = (List<String>) context
				.getProperty(JobExecutionContext.WORK_DIR_PATH_LIST + UNDERSCORE
						+ region + UNDERSCORE + parallelThreadNum);
		String partColumn = (String) context
				.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN);
		String targetTableName = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String tzColumn = (String) context
				.getProperty(JobExecutionContext.TIMEZONE_PARTITION_COLUMN);
		// if timezone is enabled the value will be region ID, if the parallel
		// threads are enabled it will be the thread ID, if both then
		// combination of region and thread ID.
		String appender = null;
		if (region != null && !region.isEmpty()) {
			appender = region;
			if (parallelThreadNum != null && !parallelThreadNum.isEmpty()) {
				appender += UNDERSCORE + parallelThreadNum;
			}
		} else if (parallelThreadNum != null && !parallelThreadNum.isEmpty()) {
			appender = parallelThreadNum;
		}
		LOGGER.debug("appender value : {}", appender);
		if (!appendersForParallelLoading.contains(appender)) {
			appendersForParallelLoading.add(appender);
		}
		context.setProperty(JobExecutionContext.APPENDERS_PARALLEL_LOADING,
				appendersForParallelLoading);
		Timer connTimer = null;
		ReConnectUtil reConnectUtil = new ReConnectUtil(
				(Integer) context.getProperty(ReConnectUtil.HIVE_RETRY_COUNT),
				(Long) context
						.getProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL));
		schedulerJobRunner jobRunner = null;
		while (reConnectUtil.shouldRetry()) {
			try {
				List<String> queryHints = HiveConfigurationProvider
						.getInstance().getQueryHints(jobId, executionEngine);
				jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				JobExecutionUtil.runQueryHints(jobRunner, queryHints);
				while (dirCount < availableDirsInWork.size()) {
					String availableDir = availableDirsInWork.get(dirCount);
					if (!interrupted) {
						// loading,updating boundary and usage_agg_status if
						// time
						// zone yes then replace lower bound and TZ_REGION_VAL
						String sql = RetrieveDimensionValues
								.replaceDynamicValuesInJobSQL(context,
										(String) context.getProperty(
												JobExecutionContext.SQL),
										jobRunner);
						LOGGER.debug("Directory in work : {}", availableDir);
						connTimer = new Timer();
						String partitionDir = getPartitionDir(partColumn,
								availableDir, tzColumn);
						// Mandatory to set this context key in all scenarios
						// across the module where hive query is to be
						// executed,so that abort job will retrieve the current
						// hive session to kill the corresponding map reduce job
						String currentTimeHint = targetTableName
								+ System.currentTimeMillis();
						if (appender != null && !appender.isEmpty()) {
							context.setProperty(
									appender + JobExecutionContext.TIMESTAMP_HINT,
									currentTimeHint);
						} else {
							context.setProperty(
									JobExecutionContext.TIMESTAMP_HINT,
									currentTimeHint);
						}
						String sqlToExecute = sql
								.replace(JobExecutionContext.LOWER_BOUND,
										partitionDir)
								.replace(JobExecutionContext.TIMESTAMP_HINT,
										currentTimeHint);
						if (region != null && !region.isEmpty()) {
							sqlToExecute = sqlToExecute.replace(
									JobExecutionContext.TZ_REGION_VAL, region);
							sqlToExecute = sqlToExecute.replace(
									JobExecutionContext.TIMEZONE_CHECK,
									" and " + tzColumn + "='" + region + "' ");
						} else if (TimeZoneUtil
								.isDefaultTZSubPartition(context)) {
							sqlToExecute = sqlToExecute.replace(
									JobExecutionContext.TZ_REGION_VAL,
									JobExecutionContext.DEFAULT_TIMEZONE);
							sqlToExecute = sqlToExecute.replace(
									JobExecutionContext.TIMEZONE_CHECK, "");
						}
						LOGGER.debug("The sql to execute : {}", sqlToExecute);
						if (appender != null && !appender.isEmpty()) {
							if (region != null && !region.isEmpty()) {
								ThreadContext.put(
										JobExecutionContext.TZ_REGION_VAL,
										region);
							}
							context.setProperty(
									JobExecutionContext.HIVE_SESSION_QUERY
											+ UNDERSCORE + appender,
									sqlToExecute);
						} else {
							context.setProperty(
									JobExecutionContext.HIVE_SESSION_QUERY,
									sqlToExecute);
						}
						connTimer.schedule(
								new Watcher(jobId, sqlToExecute, delay),
								delay * 1000, delay * 1000);
						String sleepTime = (String) context.getProperty(
								JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
						Thread logThread = applicationLogger
								.startApplicationIdLog(jobThread,
										jobRunner.getPreparedStatement(
												sqlToExecute),
										jobId, sleepTime);
						jobRunner.runQuery(sqlToExecute);
						dirCount++;
						if (dirCount == availableDirsInWork.size()) {
							LOGGER.debug("Last cycle is getting executed");
							flag = "true";
						}
						context.setProperty(JobExecutionContext.FLAG, flag);
						HiveRowCountUtil rowCountUtil = getHiveRowCounter();
						Long recordCount = rowCountUtil
								.getNumberOfRecordsInserted(context, appender,
										applicationLogger);
						if (logThread != null) {
							applicationLogger.stopApplicationIdLog(logThread);
						}
						connTimer.cancel();
						LOGGER.debug("Finshed Query Execution");
						Long loadedTime = Long.parseLong(partitionDir);
						recordCount = fetchActualRecordCount(context, region,
								recordCount, loadedTime);
						if (recordCount == -1) {
							LOGGER.warn(
									" Number of record retrieval for partition {}  failed , Please check records inserted - manually ",
									partitionDir);
						} else {
							if (region != null && !region.isEmpty()) {
								LOGGER.info(
										"Number of records inserted into the partition {} for region {} is: {}",
										new Object[] { partitionDir, region,
												recordCount });
							} else {
								LOGGER.info(
										"Number of records inserted into the partition {} is: {}",
										partitionDir, recordCount);
							}
						}
						partitionRecordCountMap.put(loadedTime, recordCount);
						Long duplicateCount = getDuplicateCount(context,
								partitionDir, recordCount, loadedTime, region);
						totalDuplicateCount += duplicateCount;
						markDepJobsInUsageAggStatus(context, recordCount,
								loadedTime, region);
						totalCount += recordCount;
						synchronized (context) {
							BoundaryQuery boundaryQuery = new BoundaryQuery();
							List<Boundary> boundaryList = boundaryQuery
									.retrieveByJobIdAndRegionId(jobId, region);
							if (boundaryList != null
									&& !boundaryList.isEmpty()) {
								Boundary boundary = boundaryList.get(0);
								if (boundary.getMaxValue() != null) {
									boundValue = boundary.getMaxValue()
											.getTime();
								} else {
									firstRun = true;
								}
							} else {
								firstRun = true;
							}
							LOGGER.debug("First run : {}", firstRun);
							LOGGER.debug("boundValue : {}, loadedTime : {}",
									boundValue, loadedTime);
							if (firstRun) {
								calendar.setTimeInMillis(loadedTime);
								firstRun = false;
							} else {
								calendar.setTimeInMillis(boundValue);
							}
							LOGGER.debug("Updating the boundary table");
							if (loadedTime >= calendar.getTimeInMillis()) {
								context.setProperty(
										JobExecutionContext.QUERYDONEBOUND,
										loadedTime);
								updateBoundary.updateBoundaryTable(context,
										region);
							}
							boundValue = loadedTime;
						}
						context.setProperty(JobExecutionContext.STATUS, "C");
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Record count for time: " + loadedTime + " is "
										+ recordCount);
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								JobExecutionContext.COMMIT_DONE);
						LOGGER.debug("Deleting files in work");
						deleteFilesInWork(context, availableDir);
						LOGGER.debug("Updating final job status in ETL_STATUS");
						if (!interrupted) {
							updateJobStatus.updateFinalJobStatus(context);
							LOGGER.debug("Updated Job status to C");
						}
					} else {
						// if the process is interrupted
						LOGGER.debug("The process is interrupted.");
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								"Interruped by User.");
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Job was aborted by user.");
						updateErrorStatus(context);
						throw new WorkFlowExecutionException(
								"InterruptException: Job was aborted by the user");
					}
				}
				context.setProperty(JobExecutionContext.PARTITION_COUNT_MAP,
						partitionRecordCountMap);
				context.setProperty(BOUND_LOADED_TIME,
						calendar.getTimeInMillis());
				context.setProperty(TOTAL_DUP_COUNT, totalDuplicateCount);
				break;
			} catch (IOException e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				throw new WorkFlowExecutionException(
						"FileSystem Exception: " + e.getMessage(), e);
			} catch (SQLException | UndeclaredThrowableException e) {
				LOGGER.debug("The exception cause is " + e.getMessage());
				if (reConnectUtil.isRetryRequired(e.getMessage())) {
					reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
							context);
				} else {
					String message = getSQLErrorMessage(context, null);
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(SQL_EXCEPTION
							+ e.getMessage() + ". Details: " + message, e);
				}
			} catch (InterruptedException e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				throw new WorkFlowExecutionException(
						INTERRUPTED_EXCEPTION + e.getMessage(), e);
			} catch (JobExecutionException e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				throw new WorkFlowExecutionException(
						JOB_EXECUTION_EXCEPTION + e.getMessage(), e);
			} catch (Exception e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				throw new WorkFlowExecutionException(
						"Exception: " + e.getMessage(), e);
			} finally {
				try {
					if (connTimer != null) {
						connTimer.cancel();
					}
					if (jobRunner != null) {
						jobRunner.closeConnection();
					}
				} catch (Exception e) {
					LOGGER.warn("Exception while closing the connection {}",
							e.getMessage());
				}
			}
		}
		return totalCount;
	}

	HiveRowCountUtil getHiveRowCounter() {
		return new HiveRowCountUtil();
	}

	private String getPartitionDir(String partColumn, String availableDir,
			String tzColumn) {
		String partitionDir = null;
		if (availableDir.indexOf(tzColumn) == -1) {
			partitionDir = availableDir.substring(
					availableDir.indexOf(partColumn) + partColumn.length() + 1,
					availableDir.length());
		} else {
			partitionDir = availableDir.substring(
					availableDir.indexOf(partColumn) + partColumn.length() + 1,
					availableDir.indexOf(tzColumn) - 1);
		}
		LOGGER.debug("partition column value : {}", partitionDir);
		return partitionDir;
	}

	private void markDepJobsInUsageAggStatus(WorkFlowContext context,
			Long recordCount, Long loadedTime, String region)
			throws JobExecutionException {
		DateFunctionTransformation dateFuncTransformer = DateFunctionTransformation
				.getInstance();
		UsageAggregationStatusUtil usageAggStatusUtil = new UsageAggregationStatusUtil();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		// Get the dependent agg jobs list and mark them as not
		// aggregated yet in the usage agg status list table
		LOGGER.debug(
				"Getting Dependent agg jobs list and mark them as not aggregated yet in the usage agg status list table");
		List<String> aggJobs = getDependentAggregationJobs(context);
		LOGGER.debug("The dependent aggregation jobs are : {}", aggJobs);
		for (String perfJobId : aggJobs) {
			usageAggStatusUtil.insertIntoUsageAggList(context, jobId, perfJobId,
					dateFuncTransformer.getFormattedDate(new Date(loadedTime)),
					recordCount, region);
		}
		LOGGER.debug("Marked in Usage Agg list");
	}

	private Long fetchActualRecordCount(WorkFlowContext context,
			String regionId, long recordCount, long loadedTime)
			throws JobExecutionException {
		String query = QueryConstants.USAGE_AGG_STATUS_LAST_INSERT_COUNT;
		if (regionId != null) {
			query = QueryConstants.USAGE_AGG_STATUS_LAST_INSERT_COUNT_WITH_REGION
					.replace(QueryConstants.REGION_ID, regionId);
		}
		DateFunctionTransformation dateFuncTransformer = DateFunctionTransformation
				.getInstance();
		query = query
				.replace(QueryConstants.USAGE_JOB_ID,
						(String) context
								.getProperty(JobExecutionContext.JOB_NAME))
				.replace(QueryConstants.REPORT_TIME, dateFuncTransformer
						.getFormattedDate(new Date(loadedTime)));
		QueryExecutor queryExecutor = new QueryExecutor();
		Object[] queryResult = queryExecutor.executeMetadatasqlQuery(query,
				context);
		Long previousInsertCount = 0L;
		if (ArrayUtils.isNotEmpty(queryResult)) {
			previousInsertCount = Long.parseLong(queryResult[0].toString());
		}
		LOGGER.debug("Previous inserted count in usage agg status : {}",
				previousInsertCount);
		return (recordCount > previousInsertCount)
				? (recordCount - previousInsertCount) : recordCount;
	}

	@SuppressWarnings("unchecked")
	private Long getDuplicateCount(WorkFlowContext context, String partitionDir,
			Long recordCount, Long loadedTime, String region)
			throws IOException {
		Map<Long, Long> inputCountMap = (Map<Long, Long>) context
				.getProperty(JobExecutionContext.INPUT_COUNT_MAP);
		Map<String, Long> timezoneInputCountMap = (Map<String, Long>) context
				.getProperty(JobExecutionContext.TIMEZONE_INPUT_COUNT_MAP);
		Long duplicateCount = 0L;
		if (duplicateFilterEnabled && recordCount > 0
				&& (inputCountMap != null || timezoneInputCountMap != null)) {
			Long inputCount = 0L;
			if (region == null || region.isEmpty()) {
				inputCount = inputCountMap.get(loadedTime);
			} else {
				inputCount = timezoneInputCountMap
						.get(loadedTime + "-" + region);
			}
			LOGGER.debug("inputCount:{} ", inputCount);
			if (inputCount != null && inputCount > 0 && recordCount > 0
					&& inputCount >= recordCount) {
				duplicateCount = inputCount - recordCount;
				LOGGER.debug("duplicateCount:{} ", duplicateCount);
			}
			if (region == null || region.isEmpty()) {
				LOGGER.info("Duplicate count for the partition: {} is {}",
						partitionDir, duplicateCount);
			} else {
				LOGGER.info(
						"Duplicate count for the partition: {} inside the region: {} is {}",
						new Object[] { partitionDir, region, duplicateCount });
			}
			writeToProcessMonitor(context, duplicateCount, recordCount,
					loadedTime);
		}
		return duplicateCount;
	}

	@SuppressWarnings("unchecked")
	private boolean loadDataWithTimezone(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.debug("Loading data when time zone is enabled.");
		boolean success = false;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		Set<String> availableDirsInWork = (Set<String>) context
				.getProperty(JobExecutionContext.WORK_DIR_PATH);
		LOGGER.debug("List of available directories in work directory : {}",
				availableDirsInWork);
		Integer numOfParallelThreads = getDegreeOfParallelism(context);
		List<String> regionList = new ArrayList<String>();
		List<String> timezoneRegion = (List<String>) context
				.getProperty(JobExecutionContext.TIME_ZONE_REGIONS);
		for (String region : timezoneRegion) {
			regionList.add(region);
		}
		LOGGER.debug("regions list : {}", regionList);
		int threadPoolSize = regionList.size();
		if (numOfParallelThreads != null && (numOfParallelThreads > 1)) {
			threadPoolSize = regionList.size() * numOfParallelThreads;
		}
		ExecutorService executors = Executors
				.newFixedThreadPool(threadPoolSize);
		try {
			List<Future<Void>> futures = new ArrayList<Future<Void>>();
			if (regionList.size() > 0) {
				for (String region : regionList) {
					List<String> availableDirsForRegion = new ArrayList<String>();
					for (String workFile : availableDirsInWork) {
						if (workFile.endsWith(
								AddDBConstants.TIMEZONE_PARTITION_COLUMN + "="
										+ region)) {
							availableDirsForRegion.add(workFile);
						}
					}
					if (availableDirsForRegion != null
							&& !availableDirsForRegion.isEmpty()) {
						Map<Integer, List<String>> splitListofDirs = null;
						if (numOfParallelThreads != null
								&& (numOfParallelThreads > 1)) {
							splitListofDirs = splitDirectoriesForParallelism(
									numOfParallelThreads,
									availableDirsForRegion);
						} else {
							splitListofDirs = new LinkedHashMap<Integer, List<String>>();
							splitListofDirs.put(0, availableDirsForRegion);
						}
						runParallelThreads(context, executors, futures, region,
								splitListofDirs);
					}
				}
			}
			for (Future<Void> future : futures) {
				future.get();
				success = true;
			}
			setTotalCountIntoContext(context);
		} catch (SQLException e) {
			success = false;
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.error("Exception while executing threads : {}",
					e.getMessage(), e);
			String message = getSQLErrorMessage(context, null);
			throw new WorkFlowExecutionException(
					"SQLException for " + message + ": " + e.getMessage(), e);
		} catch (ExecutionException e) {
			success = false;
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.error("Exception while executing threads : {}",
					e.getMessage(), e);
			throw new WorkFlowExecutionException(
					EXECUTION_EXCEPTION + e.getMessage(), e);
		} catch (InterruptedException e) {
			success = false;
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.error(
					"Exception while executing threads : The job was aborted.",
					e);
			throw new WorkFlowExecutionException(
					INTERRUPTED_EXCEPTION + e.getMessage(), e);
		} finally {
			// Once all the threads are run, shutting down the
			// executors.
			executors.shutdown();
			if (!success) {
				updateErrorStatus(context);
			}
		}
		// When there is no data for any regions configured, but for
		// other regions(by mistake only), then the job shouldn't fail.
		success = true;
		return success;
	}

	private void setTotalCountIntoContext(WorkFlowContext context) {
		LOGGER.info(
				"Total number of records inserted: {}; Total duplicate records: {}",
				overallTotalCount, overallDuplicateCount);
		context.setProperty(JobExecutionContext.DESCRIPTION,
				"Number of records inserted: " + overallTotalCount
						+ "; Duplicate record count: " + overallDuplicateCount);
		context.setProperty(JobExecutionContext.TOTAL_COUNT_INSERTED,
				overallTotalCount);
	}

	private List<Future<Void>> runParallelThreads(WorkFlowContext context,
			ExecutorService executors, List<Future<Void>> futures,
			String region, Map<Integer, List<String>> splitListofDirs)
			throws SQLException, WorkFlowExecutionException,
			InterruptedException {
		Integer numOfParallelThreads = getDegreeOfParallelism(context);
		for (Entry<Integer, List<String>> entry : splitListofDirs.entrySet()) {
			String parallelThreadStr = null;
			if (numOfParallelThreads != null && (numOfParallelThreads > 1)) {
				parallelThreadStr = PARALLEL_THREAD_APP_STR + UNDERSCORE
						+ entry.getKey().toString();
			}
			// ParallelLoading
			LOGGER.debug("Starting Parallel Loading");
			ParallelLoading parallelLoading = new ParallelLoading(
					Thread.currentThread(), entry.getValue(), region,
					parallelThreadStr, context);
			if (!executors.isShutdown()) {
				Future<Void> f = executors.submit(parallelLoading);
				futures.add(f);
				Thread.sleep(5);
			}
		}
		return futures;
	}

	private Integer getDegreeOfParallelism(WorkFlowContext context) {
		Integer numOfParallelThreads = 1;
		if (context.getProperty(
				JobExecutionContext.DEGREE_OF_PARALLELISM) != null) {
			numOfParallelThreads = Integer.parseInt((String) context
					.getProperty(JobExecutionContext.DEGREE_OF_PARALLELISM));
		}
		return numOfParallelThreads;
	}

	@SuppressWarnings("rawtypes")
	private List<String> getDependentAggregationJobs(WorkFlowContext context)
			throws JobExecutionException {
		List<String> aggJobList = new ArrayList<String>();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String query = QueryConstants.DEPENDANT_AGG_JOBS_QUERY;
		query = query.replace(QueryConstants.JOB_ID, jobId);
		QueryExecutor executor = new QueryExecutor();
		List resultList = executor.executeMetadatasqlQueryMultiple(query,
				context);
		if (resultList != null && !resultList.isEmpty()) {
			for (Object result : resultList) {
				String jobName = (String) result;
				if (jobName.endsWith("AggregateJob")) {
					aggJobList.add((String) result);
				}
			}
		}
		return aggJobList;
	}

	private void deleteFilesInWork(WorkFlowContext context, String availableDir)
			throws IOException {
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		LOGGER.debug("Directories to be deleted :" + availableDir);
		Path path = new Path(availableDir);
		fileSystem.delete(path, true);
	}

	@SuppressWarnings("rawtypes")
	private void checkDependentReaggJobs(WorkFlowContext context)
			throws JobExecutionException {
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		List<String> aggJobs = new ArrayList<String>();
		QueryExecutor queryExecutor = new QueryExecutor();
		// Querying dependent jobs of type re-aggregation
		String dependentJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY
				.replace(QueryConstants.JOB_ID, jobId);
		String regionId = ThreadContext.get(JobExecutionContext.TZ_REGION_VAL);
		if (regionId != null) {
			LOGGER.debug("Region id for the current run : {}", regionId);
			dependentJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY_TZ
					.replace(QueryConstants.JOB_ID, jobId)
					.replace(QueryConstants.REGION_ID, regionId);
		}
		LOGGER.debug("Dependent job query : {}", dependentJobQuery);
		List dependentJobs = queryExecutor
				.executeMetadatasqlQueryMultiple(dependentJobQuery, context);
		if (dependentJobs != null && !dependentJobs.isEmpty()) {
			for (Object itObj : dependentJobs) {
				Object[] data = (Object[]) itObj;
				String providerJob = data[0].toString();
				String maxValue = data[2] != null ? data[2].toString() : null;
				String providerJobAggLevel = data[1].toString();
				String aggJobName = data[3].toString();
				aggJobs.add(aggJobName);
				if (maxValue != null) {
					// If max value is null, then aggregation is not yet done
					// and hence re-aggregation will not be needed.
					if (regionId != null) {
						// If region id is present, maxvalue will differ for
						// each time-zone region, so storing in map
						reaggJobBoundaryMap.put(providerJob + ":" + regionId,
								maxValue);
					} else {
						reaggJobBoundaryMap.put(providerJob, maxValue);
					}
					reaggJobAggLevelMap.put(providerJob, providerJobAggLevel);
					reaggJobAggJobMap.put(providerJob, aggJobName);
				}
			}
		}
		context.setProperty(JobExecutionContext.REAGG_JOB_BOUNDARY_MAP,
				reaggJobBoundaryMap);
		context.setProperty(JobExecutionContext.REAGG_JOB_AGGLEVEL_MAP,
				reaggJobAggLevelMap);
		context.setProperty(JobExecutionContext.REAGG_JOB_AGGJOB_MAP,
				reaggJobAggJobMap);
		context.setProperty(JobExecutionContext.DEPENDENT_AGG_JOBS, aggJobs);
	}

	private void writeToProcessMonitor(WorkFlowContext context,
			Long duplicateCount, Long processedCount, Long partition)
			throws IOException {
		List<String> records = new ArrayList<String>();
		Long currentTimeInMillis = System.currentTimeMillis();
		Long totalCount = processedCount + duplicateCount;
		String path = (String) context.getProperty(
				JobExecutionContext.PROCESS_MONITOR_TABLE_LOCATION);
		String target = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String record = PROCESS_MONITOR_RECORD
				.replace("REPORT_TIME", partition.toString())
				.replace("DUPLICATE_RECS", duplicateCount.toString())
				.replace("TOTAL_RECS", totalCount.toString())
				.replace("PROCESSED_RECS", processedCount.toString())
				.replace("LOAD_TIME", currentTimeInMillis.toString())
				.replace("TOPOLOGY_NAME", target.substring(3));
		records.add(record);
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		FileSystem fs = FileSystem.get(conf);
		String fileName = path + FILE_SEPARATOR + target + UNDERSCORE
				+ partition + UNDERSCORE + currentTimeInMillis;
		Path filePath = new Path(fileName);
		if (fs.exists(filePath)) {
			fs.delete(filePath, true);
		}
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					fs.create(filePath), StandardCharsets.UTF_8));
			writer.append(record + "\n");
		} finally {
			close(writer);
		}
	}

	private void close(Closeable c) throws IOException {
		if (c != null) {
			c.close();
		}
	}

	static class Watcher extends TimerTask {

		String jobName;

		String sql;

		long executionDelay;

		Watcher(String jobId, String sqlToExecute, long delay) {
			jobName = jobId;
			sql = sqlToExecute;
			executionDelay = delay;
		}

		@Override
		public void run() {
			LOGGER.debug("Watcher called at: {}", System.currentTimeMillis());
			LOGGER.info("Job " + jobName + " is not responding for "
					+ (executionDelay / 60) + " mins while executing the query"
					+ sql);
		}
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	@SuppressWarnings("unchecked")
	private void validateForReaggregation(WorkFlowContext context)
			throws WorkFlowExecutionException, JobExecutionException {
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		LOGGER.debug("jobname: {}", jobName);
		String availableDataQuery = QueryConstants.REAGG_PRE_CHECK_RELATION_MAP
				.get(jobName);
		boolean isValidToReaggregate = true;
		if (availableDataQuery != null) {
			LOGGER.debug("availableDataQuery: {}", availableDataQuery);
			List<Object[]> resultSet = null;
			QueryExecutor queryExecutor = new QueryExecutor();
			resultSet = queryExecutor.executeMetadatasqlQueryMultiple(
					availableDataQuery, context);
			if (resultSet == null || resultSet.isEmpty()) {
				isValidToReaggregate = false;
			}
		}
		context.setProperty(JobExecutionContext.VALID_FOR_REAGG,
				isValidToReaggregate);
	}

	private String getSQLErrorMessage(WorkFlowContext context,
			String timezone) {
		String message = null;
		String query = (String) context
				.getProperty(JobExecutionContext.HIVE_SESSION_QUERY);
		if (timezone != null) {
			query = (String) context
					.getProperty(JobExecutionContext.HIVE_SESSION_QUERY
							+ UNDERSCORE + timezone);
		}
		if (query != null && query.contains(STREAMTABLE_HINT)) {
			message = "Query hint: "
					+ context.getProperty(JobExecutionContext.TIMESTAMP_HINT);
			if (timezone != null) {
				message = "Query hint: " + context.getProperty(
						timezone + JobExecutionContext.TIMESTAMP_HINT);
			}
		} else {
			message = "Query: " + query;
		}
		return message;
	}

	// Class to do Parallel loading when time zone is enabled
	class ParallelLoading implements Callable<Void> {

		private final List<String> availableDirInWork;

		private final String region;

		private final WorkFlowContext context;

		private final String parallelThreadNum;

		public ParallelLoading(Thread jobThread,
				List<String> availableDirInWork, String region,
				String parallelThreadNum, WorkFlowContext context) {
			LOGGER.debug("Inside ParallelLoading constructor..");
			this.availableDirInWork = availableDirInWork;
			this.region = region;
			this.context = context;
			this.parallelThreadNum = parallelThreadNum;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Void call() throws Exception {
			LOGGER.debug("inside ParallelLoading call...");
			UpdateJobStatus updateJobStatus = new UpdateJobStatus();
			try {
				WorkerThreadLoggingInterceptorRegistry.INSTANCE
						.startIntercepting(jobThread);
				LOGGER.debug("availableDirInWork : {}", availableDirInWork);
				context.setProperty(
						JobExecutionContext.WORK_DIR_PATH_LIST + UNDERSCORE
								+ region + UNDERSCORE + parallelThreadNum,
						availableDirInWork);
				Long totalCount = loadDataFromAvailableDir(context, region,
						parallelThreadNum);
				overallTotalCount += totalCount;
				overallDuplicateCount += (Long) context
						.getProperty(TOTAL_DUP_COUNT);
				if (totalCount > 0) {
					checkDependentReaggJobs(context);
					Map<String, String> reaggJobBoundaryMap = (Map<String, String>) context
							.getProperty(
									JobExecutionContext.REAGG_JOB_BOUNDARY_MAP);
					LOGGER.debug("Reaggregation jobs boundary map : {}",
							reaggJobBoundaryMap);
					if (reaggJobBoundaryMap != null
							&& !reaggJobBoundaryMap.isEmpty()) {
						// Do re aggregation related checks only when data
						// source availability is YES
						Boolean isValidForReagg = (Boolean) context.getProperty(
								JobExecutionContext.VALID_FOR_REAGG);
						if (isValidForReagg == null) {
							// first time run
							LOGGER.debug("Validate Job for Reaggregations");
							validateForReaggregation(context);
							isValidForReagg = (Boolean) context.getProperty(
									JobExecutionContext.VALID_FOR_REAGG);
						}
						if (isValidForReagg) {
							checkIfJobNeedsToBeReaggregated(context, region);
						} else {
							LOGGER.info(
									"Source is set to NO. So not triggering Re-aggregate jobs.");
						}
					}
				}
			} catch (JobExecutionException e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				LOGGER.error("Job execution exception: {}", e.getMessage(), e);
				throw new WorkFlowExecutionException(
						JOB_EXECUTION_EXCEPTION + e.getMessage(), e);
			} finally {
				WorkerThreadLoggingInterceptorRegistry.INSTANCE
						.stopIntercepting();
			}
			return null;
		}
	}
}

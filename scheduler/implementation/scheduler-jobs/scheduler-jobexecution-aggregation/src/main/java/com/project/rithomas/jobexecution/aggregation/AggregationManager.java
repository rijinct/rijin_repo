
package com.project.rithomas.jobexecution.aggregation;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.ThreadContext;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.DQICalculator;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.boundary.CalculateLBUB;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.AddDBConstants;
import com.project.rithomas.jobexecution.common.util.CommonAggregationUtil;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.MultipleQueryGeneratorBasedOnSubPartition;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.quartzdesk.api.agent.log.WorkerThreadLoggingInterceptorRegistry;

public class AggregationManager extends AbortableWorkflowStep {

	private static final String TIME_ZONE_SUPPORT = "TIME_ZONE_SUPPORT";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AggregationManager.class);

	private long delay;

	private static final String LOWER_BOUND = "LOWERBOUND";

	private static final String UPPER_BOUND = "UPPERBOUND";

	private static final String NEXT_LOWER_BOUND = "NEXTLOWERBOUND";

	private Timer connTimer = new Timer();

	private static final String TZ_OR_DQM_SUPPORT_YES = "YES";

	private static final String SQL_EXCEPTION = "SQLException:";

	private static final String LB_VALUE_STR = "LB value : ";

	private static final String UB_VALUE_STR = "UB value: ";

	private static final String STREAMTABLE_HINT = "STREAMTABLE";

	private static final String UNDERSCORE = "_";

	private String executionEngine;

	private AggregationUtil aggregationUtil = new AggregationUtil();

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		try {
			LOGGER.info("Executing Aggregation Query");
			delay = (Long) context
					.getProperty(JobExecutionContext.QUERY_TIMEOUT);
			context.setProperty(JobExecutionContext.CALLED_FROM,
					JobExecutionContext.AGG_MANAGER);
			Map<String, Long> lowerUpperBoundMap = null;
			LOGGER.debug(" TimeZone support enabled : {}",
					context.getProperty(JobExecutionContext.TIME_ZONE_SUPPORT));
			executionEngine = JobExecutionUtil.getExecutionEngine(context);
			LOGGER.debug("Executing Aggregation query on {}", executionEngine);
			if (TimeZoneUtil.isTimeZoneEnabled(context)
					&& !(TimeZoneUtil.isTimeZoneAgnostic(context))) {
				success = executeQueryTillUpperBoundWithTimezone(context);
			} else {
				CalculateLBUB calculateLBUB = new CalculateLBUB();
				CommonAggregationUtil.constructMapForDependentQSJob(context,
						(Long) context.getProperty(JobExecutionContext.LB),
						(Long) context.getProperty(JobExecutionContext.UB));
				int returnVal = 0;
				Long totalCount;
				do {
					LOGGER.debug("inside do while : {}", context
							.getProperty(JobExecutionContext.SOURCEJOBTYPE));
					lowerUpperBoundMap = getBoundValuesFromContext(context,
							null);
					Long lb = lowerUpperBoundMap
							.get(AggregationManager.LOWER_BOUND);
					Long ub = lowerUpperBoundMap
							.get(AggregationManager.UPPER_BOUND);
					Long nextLb = (Long) context
							.getProperty(JobExecutionContext.NEXT_LB);
					totalCount = executeQueryTillUpperBound(context, lb, ub,
							nextLb);
					context.setProperty(JobExecutionContext.DESCRIPTION,
							"Aggregation is completed till "
									+ context.getProperty(
											JobExecutionContext.AGGREGATIONDONEDATE)
									+ ". Total records inserted: "
									+ totalCount);
					reCalculateLBUB(context, calculateLBUB);
					returnVal = (Integer) context
							.getProperty(JobExecutionContext.RETURN);
					LOGGER.debug("Return value for LB UB is : {}", returnVal);
				} while (isValidForAggregation(context, returnVal, false));
				context.setProperty(JobExecutionContext.RETURN, 0);
				success = true;
			}
		} catch (Exception e) {
			success = false;
			LOGGER.error("Exception generating formula for KPIs :{} ",
					e.getMessage());
			throw new WorkFlowExecutionException("Exception generating formula",
					e);
		} finally {
			try {
				if (!success) {
					updateJobStatus.updateFinalJobStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while closing the connection :{} ",
						e.getMessage());
			}
		}
		return success;
	}

	@SuppressWarnings("unchecked")
	private boolean executeQueryTillUpperBoundWithTimezone(
			WorkFlowContext context) throws WorkFlowExecutionException {
		boolean success = false;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		Map<String, Long> lowerUpperBoundMap;
		if (context.getProperty(
				JobExecutionContext.AGG_TIME_ZONE_REGIONS) != null) {
			List<String> regionIds = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			LOGGER.info(" RegionIds from context: {}", regionIds);
			ExecutorService executors = Executors
					.newFixedThreadPool(regionIds.size());
			try {
				CommonAggregationUtil.constructMapForDependentQSJob(context, getMinOfLB(context),
						getMaxOfUB(context));
				context.setProperty(JobExecutionContext.DESCRIPTION, " ");
				CalculateLBUB calculateLBUB = new CalculateLBUB();
				int returnVal = 0;
				List<String> queryHints = getAllHiveQueryHints(jobId,
						(String) context.getProperty(JobExecutionContext.SQL));
				do {
					List<Future<Void>> futures = new ArrayList<>();
					for (String region : regionIds) {
						lowerUpperBoundMap = getBoundValuesFromContext(context,
								region);
						boolean aggregation = shouldAggregate(
								lowerUpperBoundMap);
						if (aggregation) {
							doParallelLoading(context, lowerUpperBoundMap,
									executors, queryHints, futures, region,
									aggregation);
						} else {
							LOGGER.debug("Skiping for: {}" + region);
							continue;
						}
					}
					for (Future<Void> future : futures) {
						future.get();
						success = true;
					}
					reCalculateLBUB(context, calculateLBUB);
					returnVal = (Integer) context
							.getProperty(JobExecutionContext.RETURN);
				} while (isValidForAggregation(context, returnVal, true));
				setDescriptionWithTz(context, regionIds);
				context.setProperty(JobExecutionContext.RETURN, 0);
			} catch (ExecutionException e) {
				success = false;
				LOGGER.error("Exception while executing threads : {}",
						e.getMessage(), e);
				throwException(context, updateJobStatus, e,
						"ExecutionException: ");
			} catch (InterruptedException e) {
				success = false;
				LOGGER.error(
						"Exception while executing threads : The job was aborted by user",
						e);
				throwException(context, updateJobStatus, e,
						"InterruptedException: ");
			} catch (Exception exception) {
				LOGGER.error("Exception : " + exception.getMessage());
				throw new WorkFlowExecutionException(
						"Exception during query Execution : "
								+ exception.getMessage(),
						exception);
			} finally {
				executors.shutdown();
				LOGGER.debug("Thread Shutting down");
				if (!success) {
					updateErrorStatus(context);
				}
			}
		}
		return success;
	}

	private void reCalculateLBUB(WorkFlowContext context,
			CalculateLBUB calculateLBUB) throws WorkFlowExecutionException {
		if (!(JobTypeDictionary.USAGE_JOB_TYPE.equals(
				context.getProperty(JobExecutionContext.SOURCEJOBTYPE)))) {
			context.setProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC,
					"YES");
			calculateLBUB.execute(context);
		}
	}

	private void doParallelLoading(WorkFlowContext context,
			Map<String, Long> lowerUpperBoundMap, ExecutorService executors,
			List<String> queryHints, List<Future<Void>> futures, String region,
			boolean aggregation) {
		LOGGER.debug("Aggregation: " + aggregation + " " + region);
		LOGGER.debug("LB :"
				+ lowerUpperBoundMap.get(AggregationManager.LOWER_BOUND));
		LOGGER.debug("UB :"
				+ lowerUpperBoundMap.get(AggregationManager.UPPER_BOUND));
		long lb = lowerUpperBoundMap.get(AggregationManager.LOWER_BOUND);
		long ub = lowerUpperBoundMap.get(AggregationManager.UPPER_BOUND);
		long nextLb = lowerUpperBoundMap
				.get(AggregationManager.NEXT_LOWER_BOUND);
		LOGGER.debug(" NEXLB: " + nextLb);
		if (region != null) {
			LOGGER.debug(" Executing for region : " + region);
			ParallelLoading parallelLoading = new ParallelLoading(
					Thread.currentThread(), lb, nextLb, ub, context, region,
					queryHints);
			if (!executors.isShutdown()) {
				Future<Void> f = executors.submit(parallelLoading);
				futures.add(f);
			}
		}
	}

	private void setDescriptionWithTz(WorkFlowContext context,
			List<String> regionIds) {
		for (String region : regionIds) {
			String description = (String) context.getProperty(
					JobExecutionContext.DESCRIPTION + UNDERSCORE + region);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					(String) context
							.getProperty(JobExecutionContext.DESCRIPTION) + " "
							+ (description != null ? description : ""));
		}
	}

	@SuppressWarnings("unchecked")
	private Long getMinOfLB(WorkFlowContext context) {
		List<Long> lbValues = getListOfBoundValues(context, "LB_");
		return Collections.min(lbValues);
	}

	private List<Long> getListOfBoundValues(WorkFlowContext context, String prefix) {
		List<Long> boundValues = new ArrayList<>();
		List<String> regionIds = (List<String>) context
				.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
		for (String regionId : regionIds) {
			if (context.getProperty(prefix + regionId) != null) {
				boundValues.add((Long) context.getProperty(prefix + regionId));
			}
		}
		return boundValues;
	}

	@SuppressWarnings("unchecked")
	private Long getMaxOfUB(WorkFlowContext context) {
		List<Long> ubValues = getListOfBoundValues(context, "UB_");
		return Collections.max(ubValues);
	}

	@SuppressWarnings("unchecked")
	private boolean isValidForAggregation(WorkFlowContext context,
			int returnVal, boolean isTimeZoneEnabled)
			throws WorkFlowExecutionException {
		boolean valid = false;
		if (!(JobTypeDictionary.USAGE_JOB_TYPE
				.equals(context.getProperty(JobExecutionContext.SOURCEJOBTYPE)))
				&& returnVal == 0) {
			if (isTimeZoneEnabled) {
				List<String> regions = (List<String>) context
						.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
				for (String region : regions) {
					Map<String, Long> lowerUpperBoundMap = getBoundValuesFromContext(
							context, region);
					Long nextLb = lowerUpperBoundMap
							.get(AggregationManager.NEXT_LOWER_BOUND);
					if (nextLb != null && nextLb < DateFunctionTransformation
							.getInstance().getSystemTime()) {
						valid = true;
						break;
					}
				}
			} else {
				Long nextLb = (Long) context
						.getProperty(JobExecutionContext.NEXT_LB);
				valid = nextLb != null && nextLb < DateFunctionTransformation
						.getInstance().getSystemTime();
			}
		}
		return valid;
	}

	private Long executeQueryTillUpperBound(WorkFlowContext context, Long lb,
			Long ub, Long nextLb) throws WorkFlowExecutionException {
		Calendar calendarBound = new GregorianCalendar();
		Calendar calendarTemp = new GregorianCalendar();
		Long newUb = 0L;
		HiveRowCountUtil rowCountUtil = new HiveRowCountUtil();
		Long totalCount = (Long) context
				.getProperty(JobExecutionContext.TOTAL_COUNT);
		UpdateBoundary updateBoundary = new UpdateBoundary();
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		ReConnectUtil reConnectUtil = new ReConnectUtil(
				(Integer) context.getProperty(ReConnectUtil.HIVE_RETRY_COUNT),
				(Long) context
						.getProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL));
		schedulerJobRunner jobRunner = null;
		while (reConnectUtil.shouldRetry()) {
			try {
				List<String> hiveSettings = getAllHiveQueryHints(jobId, sql);
				jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				DateFunctionTransformation dateTransformer = DateFunctionTransformation
						.getInstance();
				Calendar calendar;
				JobExecutionUtil.runQueryHints(jobRunner, hiveSettings);
				while (nextLb <= ub
						&& nextLb <= dateTransformer.getSystemTime()) {
					if (!interrupted) {
						connTimer = new Timer();
						calendarBound.setTimeInMillis(lb);
						// start DST changes
						if (isLowerBoundaryFallsInDST(context, lb, ub,
								dateTransformer, null)) {
							break;
						}
						// End of DST changes
						newUb = calculateUbIfMGW(nextLb, newUb, sql);
						String sqlToExecute = getSqlToExecute(calendarBound,
								newUb, lb, nextLb, context, null, jobRunner);
						LOGGER.debug("ELSE-SQL to execute: {}", sqlToExecute);
						context.setProperty(
								JobExecutionContext.HIVE_SESSION_QUERY,
								sqlToExecute);
						connTimer.schedule(new Watcher(), delay * 1000,
								delay * 1000);
						handlingForAPN(context, jobRunner);
						String sleepTime = (String) context.getProperty(
								JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
						LOGGER.debug(
								"In Aggregation manager entering to StartApplicationIdLog");
						ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory
								.getApplicationLogger();
						context.setProperty(
								JobExecutionContext.APPLICATION_ID_LOGGER,
								applicationLogger);
						Thread logThread = applicationLogger
								.startApplicationIdLog(Thread.currentThread(),
										jobRunner.getPreparedStatement(
												sqlToExecute),
										jobId, sleepTime);
						context.setProperty(
								JobExecutionContext.APPLICATION_ID_LOGGER,
								applicationLogger);
						LOGGER.debug("The application logger object is  {}",
								applicationLogger);
						jobRunner.runQuery(sqlToExecute);
						setFLAG(context, ub, nextLb, dateTransformer, null);
						totalCount = printLogs(context, lb, rowCountUtil,
								totalCount, applicationLogger);
						if (logThread != null) {
							applicationLogger.stopApplicationIdLog(logThread);
						}
						connTimer.cancel();
						calculateDQIiFEnabled(context, lb, nextLb);
						updateUsageAggListIfSrcUsage(context, lb, nextLb, jobId,
								dateTransformer, null);
						SubPartitionUtil util = new SubPartitionUtil();
						util.storeParitionInfo(context, jobRunner,
								JobExecutionContext.DEFAULT_TIMEZONE, lb);
						context.setProperty(JobExecutionContext.QUERYDONEBOUND,
								lb);
						updateBoundaryTable(context, updateBoundary);
						lb = nextLb;
						calendarTemp.setTimeInMillis(lb);
						LOGGER.info("New LowerBound value: {}", dateTransformer
								.getFormattedDate(calendarTemp.getTime()));
						calendar = dateTransformer.getNextBound(lb,
								(String) context.getProperty(
										JobExecutionContext.PLEVEL));
						nextLb = calendar.getTimeInMillis();
						LOGGER.info("New Next LowerBound value: {}",
								dateTransformer
										.getFormattedDate(calendar.getTime()));
					} else {
						LOGGER.debug("Job was aborted");
						updateErrorStatus(context);
						throw new WorkFlowExecutionException(
								"InterruptExpetion: Job was aborted by the user");
					}
				}
				context.setProperty(JobExecutionContext.TOTAL_COUNT,
						totalCount);
				break;
			} catch (JobExecutionException e) {
				LOGGER.error(" JobExecutionException {}", e.getMessage(), e);
				throwException(context, updateJobStatus, e, e.getMessage());
			} catch (SQLException | UndeclaredThrowableException e) {
				LOGGER.error(" SQL/Connection Exception {}", e.getMessage(), e);
				connTimer.cancel();
				if (reConnectUtil.isRetryRequired(e.getMessage())) {
					reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
							context);
				} else {
					String message = getSQLErrorMessage(context, null);
					throwException(context, updateJobStatus, e,
							"SQLException for " + message);
				}
			} catch (ClassNotFoundException e) {
				LOGGER.error(" ClassNotFoundException {}", e.getMessage(), e);
				throwException(context, updateJobStatus, e,
						"ClassNotFoundException: ");
			} catch (ETLException e) {
				LOGGER.error(" ETLException {}", e.getMessage(), e);
				throwException(context, updateJobStatus, e, "ETLException: ");
			} catch (IOException e) {
				throwException(context, updateJobStatus, e,
						"FileSystem Exception: ");
			} catch (InterruptedException e) {
				throwException(context, updateJobStatus, e,
						"InterruptedException:");
			} catch (Exception e) {
				throwException(context, updateJobStatus, e, "Excpetion:");
			} finally {
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
			}
		}
		return totalCount;
	}

	private Long calculateUbIfMGW(Long nextLb, Long newUb, String sql) {
		if (sql.contains(JobExecutionContext.PERF_MGW)) {
			newUb = nextLb + 120000;
		}
		return newUb;
	}

	private void setFLAG(WorkFlowContext context, Long ub, Long nextLb,
			DateFunctionTransformation dateTransformer, String region) {
		if (nextLb.equals(dateTransformer.getTrunc(ub,
				(String) context.getProperty(JobExecutionContext.PLEVEL),
				(String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY),
				TimeZoneUtil.getZoneId(context, region)))) {
			context.setProperty(JobExecutionContext.FLAG, "true");
		}
	}

	private void handlingForAPN(WorkFlowContext context,
			schedulerJobRunner runner) throws Exception {
		if (JobExecutionContext.APN_USAGE_JOBID
				.contains(context.getProperty(JobExecutionContext.JOB_NAME))) {
			String query = "truncate table PS_APNHLR_USAGE_1_DAY";
			runner.getPreparedStatement(query);
			runner.runQuery(query);
			LOGGER.info("PS_APNHLR_USAGE_1_DAY table truncated");
		}
	}

	private void throwException(WorkFlowContext context,
			UpdateJobStatus updateJobStatus, Exception e, String exception)
			throws WorkFlowExecutionException {
		updateJobStatus.updateETLErrorStatusInTable(e, context);
		throw new WorkFlowExecutionException(exception + e.getMessage(), e);
	}

	private void updateBoundaryTable(WorkFlowContext context,
			UpdateBoundary updateBoundary) throws WorkFlowExecutionException {
		if ((TZ_OR_DQM_SUPPORT_YES.equalsIgnoreCase(
				(String) context.getProperty(TIME_ZONE_SUPPORT)))
				&& (TimeZoneUtil.isTimeZoneAgnostic(context))) {
			updateBoundary.updateBoundaryTable(context,
					JobExecutionContext.DEFAULT_TIMEZONE);
		} else {
			updateBoundary.updateBoundaryTable(context, null);
		}
	}

	private void updateUsageAggListIfSrcUsage(WorkFlowContext context, Long lb,
			Long nextLb, String jobId,
			DateFunctionTransformation dateTransformer, String region)
			throws JobExecutionException {
		UsageAggregationStatusUtil util = new UsageAggregationStatusUtil();
		util.updateUsageAggList(context, jobId,
				dateTransformer.getFormattedDate(new Date(lb)),
				dateTransformer.getFormattedDate(new Date(nextLb)), region);
	}

	private void calculateDQIiFEnabled(WorkFlowContext context, Long lb,
			Long nextLb) throws WorkFlowExecutionException, SQLException,
			JobExecutionException, ETLException {
		String dqmEnabled = (String) context
				.getProperty(JobExecutionContext.DQI_ENABLED);
		LOGGER.debug("dqmEnabled: {}", dqmEnabled);
		if (TZ_OR_DQM_SUPPORT_YES.equalsIgnoreCase(dqmEnabled)) {
			DQICalculator dqiCalculator = new DQICalculator();
			LOGGER.debug("Calculating DQI values");
			dqiCalculator.calculateDqi(context, lb, nextLb);
		}
	}

	private List<String> getAllHiveQueryHints(String jobId, String sql)
			throws WorkFlowExecutionException {
		List<String> hiveSettings = aggregationUtil
				.setQueryHintForParallelInsert(sql);
		hiveSettings.addAll(HiveConfigurationProvider.getInstance()
				.getQueryHints(jobId, executionEngine));
		return hiveSettings;
	}

	private boolean isLowerBoundaryFallsInDST(WorkFlowContext context, Long lb,
			Long ub, DateFunctionTransformation dateTransformer, String region)
			throws JobExecutionException {
		boolean isLbFallsInDST = false;
		LOGGER.debug("Checking if LB is falling in DST or not");
		String perfLevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		if (!TimeZoneUtil.isSystemInUTC() && (JobExecutionContext.MIN_15
				.equalsIgnoreCase(perfLevel)
				|| JobExecutionContext.HOUR.equalsIgnoreCase(perfLevel))) {
			Long calendarLBForDST = dateTransformer.getTrunc(lb,
					JobExecutionContext.HOUR);
			Integer ubDSTCnt = null;
			ubDSTCnt = getUbDSTCnt(context, region);
			LOGGER.debug("ubDSTCnt  is  {}", ubDSTCnt);
			isLbFallsInDST = getIsLBFallsInDST(context, lb, ub, isLbFallsInDST,
					calendarLBForDST, ubDSTCnt);
		}
		return isLbFallsInDST;
	}

	private boolean getIsLBFallsInDST(WorkFlowContext context, Long lb, Long ub,
			boolean isLbFallsInDST, Long calendarLBForDST, Integer ubDSTCnt)
			throws JobExecutionException {
		if (ubDSTCnt > 0) {
			int lbDSTCnt = getLBDSTCnt(context, calendarLBForDST);
			if (lbDSTCnt > 0) {
				lowerBoundFallingInDSTPeriod(lb, ub, null, context);
				isLbFallsInDST = true;
			}
		}
		return isLbFallsInDST;
	}

	private int getLBDSTCnt(WorkFlowContext context, Long calendarLBForDST)
			throws JobExecutionException {
		DayLightSavingUtil dayLightSavingUtil = new DayLightSavingUtil();
		Calendar truncCalLBForDST = new GregorianCalendar();
		truncCalLBForDST.setTimeInMillis(calendarLBForDST);
		int lbDSTCnt = dayLightSavingUtil
				.getCntIfLBInTimeZoneDST(truncCalLBForDST, null, context);
		LOGGER.debug("lbDSTCnt: {}", lbDSTCnt);
		return lbDSTCnt;
	}

	private Integer getUbDSTCnt(WorkFlowContext context, String region) {
		Integer ubDSTCnt;
		if (region != null) {
			ubDSTCnt = (Integer) context.getProperty(
					JobExecutionContext.UB_DST_COUNT + "_" + region);
		} else {
			ubDSTCnt = (Integer) context
					.getProperty(JobExecutionContext.UB_DST_COUNT);
		}
		return ubDSTCnt;
	}

	private Long printLogs(WorkFlowContext context, Long lb,
			HiveRowCountUtil rowCountUtil, Long totalCount,
			ApplicationLoggerUtilInterface applicationLogger)
			throws WorkFlowExecutionException, IOException,
			InterruptedException {
		LOGGER.debug("Getting number of records");
		Long recordCount = null;
		// When TZ is YES calculate record count inserted
		// into Default partition
		if ((TZ_OR_DQM_SUPPORT_YES.equalsIgnoreCase(
				(String) context.getProperty(TIME_ZONE_SUPPORT)))
				&& (TimeZoneUtil.isTimeZoneAgnostic(context))) {
			LOGGER.debug("Inside week adn month check and value: {}",
					JobExecutionContext.DEFAULT_TIMEZONE);
			recordCount = rowCountUtil.getNumberOfRecordsInserted(context,
					JobExecutionContext.DEFAULT_TIMEZONE, applicationLogger);
		} else {
			recordCount = rowCountUtil.getNumberOfRecordsInserted(context, null,
					applicationLogger);
		}
		totalCount = (totalCount != null ? totalCount : 0) + recordCount;
		if (recordCount == -1) {
			LOGGER.warn(
					" Number of record retrieval for partition {}  failed , Please check records inserted - manually ",
					lb);
		} else {
			LOGGER.info(
					"Number of records inserted into the partition {} is: {}",
					lb, recordCount);
		}
		return totalCount;
	}

	private boolean shouldAggregate(Map<String, Long> lowerUpperBoundMap) {
		return lowerUpperBoundMap.containsKey(AggregationManager.LOWER_BOUND)
				&& lowerUpperBoundMap
						.containsKey(AggregationManager.UPPER_BOUND)
				&& lowerUpperBoundMap
						.containsKey(AggregationManager.NEXT_LOWER_BOUND);
	}

	private String getSQLErrorMessage(WorkFlowContext context,
			String timezone) {
		String message = null;
		String query = (String) context
				.getProperty(JobExecutionContext.HIVE_SESSION_QUERY);
		if (timezone != null) {
			query = (String) context.getProperty(
					JobExecutionContext.HIVE_SESSION_QUERY + "_" + timezone);
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

	protected String getSqlToExecute(Calendar calendarBound, Long newUb,
			Long lb, Long nextLb, WorkFlowContext context, String regionId,
			schedulerJobRunner jobRunner) throws Exception {
		RetrieveDimensionValues.checkSource(context);
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String currentTimeHint = targetTable + System.currentTimeMillis();
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		if (aggregationUtil.isKubernetesEnvironment()) {
			context.setProperty(JobExecutionContext.SQL,
					aggregationUtil.replaceKPIFormula(context));
		} else {
			context.setProperty(JobExecutionContext.SQL, CommonAggregationUtil
					.replaceKPIFormula(jobName, sql, context));
		}
		String sqlToExecute = (String) context
				.getProperty(JobExecutionContext.SQL);
		sqlToExecute = sqlToExecute
				.replace(JobExecutionContext.LOWER_BOUND, lb.toString())
				.replace(JobExecutionContext.UPPER_BOUND, nextLb.toString())
				.replace(JobExecutionContext.LOWERBOUND_DATE,
						DateFunctionTransformation.getInstance()
								.getFormattedDate(calendarBound.getTime()))
				.replace(JobExecutionContext.TIMESTAMP_HINT, currentTimeHint);
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			if (TimeZoneUtil.isTimeZoneAgnostic(context)) {
				sqlToExecute = sqlToExecute
						.replace(JobExecutionContext.TIMEZONE_CHECK, "");
				sqlToExecute = sqlToExecute.replace(
						JobExecutionContext.TIMEZONE_PARTITION,
						" , " + AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='"
								+ JobExecutionContext.DEFAULT_TIMEZONE + "' ");
				context.setProperty(
						JobExecutionContext.DEFAULT_TIMEZONE
								+ JobExecutionContext.TIMESTAMP_HINT,
						currentTimeHint);
				ThreadContext.put(JobExecutionContext.TZ_REGION_VAL,
						JobExecutionContext.DEFAULT_TIMEZONE);
			} else {
				sqlToExecute = sqlToExecute.replace(
						JobExecutionContext.TIMEZONE_CHECK,
						" and " + AddDBConstants.TIMEZONE_PARTITION_COLUMN
								+ "='" + regionId + "' ");
				sqlToExecute = sqlToExecute.replace(
						JobExecutionContext.TIMEZONE_PARTITION,
						" , " + AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='"
								+ regionId + "' ");
				context.setProperty(
						regionId + JobExecutionContext.TIMESTAMP_HINT,
						currentTimeHint);
				ThreadContext.put(JobExecutionContext.TZ_REGION_VAL, regionId);
			}
		} else {
			sqlToExecute = sqlToExecute
					.replace(JobExecutionContext.TIMEZONE_CHECK, "");
			context.setProperty(JobExecutionContext.TIMESTAMP_HINT,
					currentTimeHint);
			if (TimeZoneUtil.isDefaultTZSubPartition(context)) {
				sqlToExecute = sqlToExecute.replace(
						JobExecutionContext.TIMEZONE_PARTITION,
						" , " + AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='"
								+ JobExecutionContext.DEFAULT_TIMEZONE + "' ");
			} else {
				sqlToExecute = sqlToExecute
						.replace(JobExecutionContext.TIMEZONE_PARTITION, "");
			}
		}
		sqlToExecute = RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, sqlToExecute, jobRunner);
		sqlToExecute = replacePlaceHoldersInQuery(context, newUb, sqlToExecute);
		sqlToExecute = MultipleQueryGeneratorBasedOnSubPartition
				.generateQuery(sqlToExecute, context, jobRunner, regionId, lb, nextLb);
		context.setProperty(JobExecutionContext.FLAG, "false");
		return sqlToExecute;
	}

	private String replacePlaceHoldersInQuery(WorkFlowContext context,
			Long newUb, String sqlToExecute) throws JobExecutionException {
		Map<String, String> placeHoldersMap = new HashMap<>();
		placeHoldersMap.put(JobExecutionContext.AVAILABLE_DATA_CHECK,
				RetrieveDimensionValues.getAvailableDataCheck(context));
		placeHoldersMap.put(JobExecutionContext.AVAILABLE_2G_3G,
				RetrieveDimensionValues.getAvailableType2G(context));
		placeHoldersMap.put(JobExecutionContext.AVAILABLE_4G,
				RetrieveDimensionValues.getAvailableType4G(context));
		placeHoldersMap.put(JobExecutionContext.MAX_NO_OF_IMSI_EXPORT,
				((Long) context
						.getProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT))
								.toString());
		placeHoldersMap.put(JobExecutionContext.NEXT_UPPERBOUND,
				newUb.toString());
		placeHoldersMap.put(JobExecutionContext.CUTOFF_VOLUME_2G,
				(String) context
						.getProperty(JobExecutionContext.CUTOFF_VOLUME_2G));
		placeHoldersMap.put(JobExecutionContext.CUTOFF_VOLUME_3G,
				(String) context
						.getProperty(JobExecutionContext.CUTOFF_VOLUME_3G));
		placeHoldersMap.put(JobExecutionContext.CUTOFF_VOLUME_4G,
				(String) context
						.getProperty(JobExecutionContext.CUTOFF_VOLUME_4G));
		placeHoldersMap.put(JobExecutionContext.TYPE_AVAILABLE_HO,
				RetrieveDimensionValues.getTypeAvailableHO(context));
		placeHoldersMap.put(JobExecutionContext.TYPE_AVAILABLE_SGSN,
				RetrieveDimensionValues.getTypeAvailableSGSN(context));
		placeHoldersMap.put(JobExecutionContext.LONG_CALLS_THRESHOLD_VALUE,
				(String) context.getProperty(
						JobExecutionContext.LONG_CALLS_THRESHOLD_VALUE));
		placeHoldersMap.put(JobExecutionContext.AVERAGE_MOS_THRESHOLD,
				(String) context.getProperty(
						JobExecutionContext.AVERAGE_MOS_THRESHOLD));
		LOGGER.debug("placeHoldersMap : {}", placeHoldersMap);
		return StringModificationUtil.replaceMultipleSubStrings(sqlToExecute,
				placeHoldersMap);
	}

	private Map<String, Long> getBoundValuesFromContext(WorkFlowContext context,
			String regionId) throws WorkFlowExecutionException {
		Map<String, Long> lowerUpperBoundValues = new HashMap<>();
		if ((TZ_OR_DQM_SUPPORT_YES.equalsIgnoreCase(
				(String) context.getProperty(TIME_ZONE_SUPPORT)))
				&& !(TimeZoneUtil.isTimeZoneAgnostic(context))) {
			if (context.getProperty("LB_" + regionId) != null) {
				lowerUpperBoundValues.put(AggregationManager.LOWER_BOUND,
						(Long) context.getProperty("LB_" + regionId));
			}
			if (context.getProperty("UB_" + regionId) != null) {
				lowerUpperBoundValues.put(AggregationManager.UPPER_BOUND,
						(Long) context.getProperty("UB_" + regionId));
			}
			if (context.getProperty("NEXT_LB_" + regionId) != null) {
				lowerUpperBoundValues.put(AggregationManager.NEXT_LOWER_BOUND,
						(Long) context.getProperty("NEXT_LB_" + regionId));
			}
		} else {
			lowerUpperBoundValues.put(AggregationManager.LOWER_BOUND,
					(Long) context.getProperty(JobExecutionContext.LB));
			lowerUpperBoundValues.put(AggregationManager.UPPER_BOUND,
					(Long) context.getProperty(JobExecutionContext.UB));
		}
		return lowerUpperBoundValues;
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	class Watcher extends TimerTask {

		@Override
		public void run() {
			LOGGER.debug("Watcher called at: {}", System.currentTimeMillis());
			LOGGER.info("Job is not responding for {} mins.", delay / 60);
		}
	}

	private void lowerBoundFallingInDSTPeriod(long lb, long ub, String region,
			WorkFlowContext context) {
		Calendar tmpLbCalendar = new GregorianCalendar();
		Calendar tmpUbCalendar = new GregorianCalendar();
		LOGGER.debug("lb Falls in DST for region: {}", region);
		tmpLbCalendar.setTimeInMillis(lb);
		tmpUbCalendar.setTimeInMillis(ub);
		if (region != null && !region.isEmpty()) {
			LOGGER.info("The current aggregating data(" + LB_VALUE_STR
					+ DateFunctionTransformation.getInstance()
							.getFormattedDate(tmpLbCalendar.getTime())
					+ " and " + UB_VALUE_STR
					+ DateFunctionTransformation.getInstance()
							.getFormattedDate(tmpUbCalendar.getTime())
					+ ") falls in Timezone DST period for " + region
					+ ". Hence not aggregating further for this region until the data is available after Timezone DST period.");
			context.setProperty(JobExecutionContext.DESCRIPTION,
					"The current aggregating data(" + LB_VALUE_STR
							+ DateFunctionTransformation.getInstance()
									.getFormattedDate(tmpLbCalendar.getTime())
							+ "and " + UB_VALUE_STR
							+ DateFunctionTransformation.getInstance()
									.getFormattedDate(tmpUbCalendar.getTime())
							+ ") falls in Timezone DST period for " + region
							+ ". Hence not aggregating further for this region until the data is available after Timezone DST period.");
		} else {
			LOGGER.info("The current aggregating data(" + LB_VALUE_STR
					+ DateFunctionTransformation.getInstance()
							.getFormattedDate(tmpLbCalendar.getTime())
					+ " and " + UB_VALUE_STR
					+ DateFunctionTransformation.getInstance()
							.getFormattedDate(tmpUbCalendar.getTime())
					+ ") falls in Timezone DST period. Hence not aggregating further until the data is available after DST period.");
			context.setProperty(JobExecutionContext.DESCRIPTION,
					"The current aggregating data(" + LB_VALUE_STR
							+ DateFunctionTransformation.getInstance()
									.getFormattedDate(tmpLbCalendar.getTime())
							+ "and " + UB_VALUE_STR
							+ DateFunctionTransformation.getInstance()
									.getFormattedDate(tmpUbCalendar.getTime())
							+ ") falls in Timezone DST period. Hence not aggregating further until the data is available after DST period.");
		}
	}

	class ParallelLoading implements Callable<Void> {

		private Long lb, nextLb;

		private final Long ub;

		private final WorkFlowContext context;

		private final String region;

		private final List<String> queryHints;

		Thread jobThread;

		public ParallelLoading(Thread parentJobThread, Long lb, Long nextLb,
				Long ub, WorkFlowContext context, String region,
				List<String> queryHints) {
			LOGGER.debug("Parallel Loading Constructor for Region {} called",
					region);
			this.jobThread = parentJobThread;
			this.lb = lb;
			this.nextLb = nextLb;
			this.context = context;
			this.region = region;
			this.ub = ub;
			this.queryHints = queryHints;
		}

		@Override
		public Void call() throws Exception {
			boolean isDst = false;
			boolean success = false;
			LOGGER.debug("Parallel Loading call method");
			Long totalCount = (Long) context.getProperty(
					JobExecutionContext.TOTAL_COUNT + UNDERSCORE + region);
			Long newUb = 0L;
			Timer threadConnTimer = null;
			Calendar calendarTemp = new GregorianCalendar();
			UpdateBoundary updateBoundary = new UpdateBoundary();
			Calendar calendarBound = new GregorianCalendar();
			DateFunctionTransformation dateTransformer = DateFunctionTransformation
					.getInstance();
			String jobId = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			String sql = (String) context.getProperty(JobExecutionContext.SQL);
			ReConnectUtil reConnectUtil = new ReConnectUtil();
			schedulerJobRunner jobRunner = null;
			try {
				while (reConnectUtil.shouldRetry()) {
					try {
						WorkerThreadLoggingInterceptorRegistry.INSTANCE
								.startIntercepting(jobThread);
						jobRunner = schedulerJobRunnerfactory.getRunner(
								executionEngine, (boolean) context.getProperty(
										JobExecutionContext.IS_CUSTOM_DB_URL));
						JobExecutionUtil.runQueryHints(jobRunner, queryHints);
						while (nextLb <= ub
								&& nextLb <= dateTransformer.getSystemTime()) {
							if (!interrupted) {
								LOGGER.debug("Region {} and LB: {}", region,
										lb);
								threadConnTimer = new Timer();
								calendarBound.setTimeInMillis(lb);
								// start DST changes
								if (isLowerBoundaryFallsInDST(context, lb, ub,
										dateTransformer, region)) {
									break;
								}
								// End of DST changes
								LOGGER.debug("When DST is not true");
								newUb = calculateUbIfMGW(nextLb, newUb, sql);
								String sqlToExecutes = getSqlToExecute(
										calendarBound, newUb, lb, nextLb,
										context, region, jobRunner);
								LOGGER.debug(
										"Sql to execute: {}  " + sqlToExecutes);
								context.setProperty(
										JobExecutionContext.HIVE_SESSION_QUERY
												+ "_" + region,
										sqlToExecutes);
								threadConnTimer.schedule(new Watcher(),
										delay * 1000, delay * 1000);
								LOGGER.debug("Executing {}", sqlToExecutes);
								String sleepTime = (String) context.getProperty(
										JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
								ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory
										.getApplicationLogger();
								context.setProperty(
										JobExecutionContext.APPLICATION_ID_LOGGER,
										applicationLogger);
								Thread logThread = applicationLogger
										.startApplicationIdLog(jobThread,
												jobRunner.getPreparedStatement(
														sqlToExecutes),
												jobId, sleepTime);
								jobRunner.runQuery(sqlToExecutes);
								LOGGER.debug(
										"Getting number of records inserted");
								setFLAG(context, newUb, nextLb, dateTransformer,
										region);
								HiveRowCountUtil rowCountUtil = new HiveRowCountUtil();
								Long recordCount = rowCountUtil
										.getNumberOfRecordsInserted(context,
												region, applicationLogger);
								if (logThread != null) {
									applicationLogger
											.stopApplicationIdLog(logThread);
								}
								LOGGER.debug("Executed {}", sqlToExecutes);
								threadConnTimer.cancel();
								if (recordCount == -1) {
									LOGGER.warn(
											" Number of record retrieval for partition {}  failed , Please check records inserted - manually ",
											lb);
								} else {
									LOGGER.info(
											"Number of records inserted into partition: {}, for timezone: {} is {}",
											lb, region,	recordCount);
								}
								totalCount = (totalCount != null ? totalCount
										: 0) + recordCount;
								calculateDQIiFEnabled(context, lb, nextLb);
								updateUsageAggListIfSrcUsage(context, lb,
										nextLb, jobId, dateTransformer,
										this.region);
								context.setProperty(
										JobExecutionContext.QUERYDONEBOUND, lb);
								SubPartitionUtil util = new SubPartitionUtil();
								util.storeParitionInfo(context, jobRunner,
										this.region, lb);
								updateBoundary.updateBoundaryTable(context,
										this.region);
								lb = nextLb;
								calendarTemp.setTimeInMillis(lb);
								LOGGER.info(
										"New LowerBound value for {} is : {}",
										region,
										dateTransformer.getFormattedDate(
												calendarTemp.getTime()));
								Calendar calendar = dateTransformer
										.getNextBound(lb,
												(String) context.getProperty(
														JobExecutionContext.PLEVEL));
								nextLb = calendar.getTimeInMillis();
								LOGGER.info(
										"New Next LowerBound value for {} is : {}",
										region,
										dateTransformer.getFormattedDate(
												calendar.getTime()));
							} else {
								LOGGER.debug("Job was aborted");
								updateErrorStatus(context);
								throw new WorkFlowExecutionException(
										"InterruptExpetion: Job was aborted by the user");
							}
						}
						if (!isDst) {
							context.setProperty(JobExecutionContext.TOTAL_COUNT
									+ UNDERSCORE + region, totalCount);
							context.setProperty(
									JobExecutionContext.DESCRIPTION + UNDERSCORE
											+ region,
									"Aggregation is completed till "
											+ context.getProperty(
													JobExecutionContext.AGGREGATIONDONEDATE)
											+ ". Total records inserted for "
											+ region + " is: " + totalCount);
						}
						success = true;
						break;
					} catch (SQLException | UndeclaredThrowableException e) {
						if (threadConnTimer != null) {
							threadConnTimer.cancel();
						}
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage() + ". For region: " + region
										+ ".");
						String message = getSQLErrorMessage(context, region);
						if (reConnectUtil.isRetryRequired(e.getMessage())) {
							reConnectUtil.updateRetryAttemptsForHive(
									e.getMessage(), context);
						} else {
							throw new WorkFlowExecutionException(SQL_EXCEPTION
									+ e.getMessage() + ". For region: " + region
									+ ". Details: " + message, e);
						}
					} finally {
						try {
							WorkerThreadLoggingInterceptorRegistry.INSTANCE
									.stopIntercepting();
						} catch (Exception e) {
							success = false;
							LOGGER.warn(
									"Exception while clossing the connection {}",
									e.getMessage());
						}
					}
				}
			} finally {
				jobRunner.closeConnection();
				if (!success) {
					updateErrorStatus(context);
				}
			}
			return null;
		}
	}
}

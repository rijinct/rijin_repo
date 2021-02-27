
package com.project.rithomas.jobexecution.tnp;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.KpiUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ThresholdCalculationJob extends AbortableWorkflowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ThresholdCalculationJob.class);

	private static long delay;

	private static final String LOWER_BOUND = "LOWERBOUND";

	private static final String UPPER_BOUND = "UPPERBOUND";

	private Timer connTimer;

	private static void setDelay(long del) {
		delay = del;
	}
	private String executionEngine;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		UpdateBoundary updateBoundary = new UpdateBoundary();
		Calendar calendarBound = new GregorianCalendar();
		Calendar calendarTemp = new GregorianCalendar();
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		schedulerJobRunner jobRunner = null;
		String query = null;
		executionEngine = JobExecutionUtil.getExecutionEngine(context);
		LOGGER.debug("Executing Threshold query on {}", executionEngine);
		Map<String, Long> lowerUpperBoundMap = getBoundValuesFromContext(
				context);
		long lb = lowerUpperBoundMap.get(ThresholdCalculationJob.LOWER_BOUND);
		long ub = lowerUpperBoundMap.get(ThresholdCalculationJob.UPPER_BOUND);
		Long nextLb = (Long) context.getProperty(JobExecutionContext.NEXT_LB);
		ReConnectUtil reConnectUtil = new ReConnectUtil(
				(Integer) context.getProperty(ReConnectUtil.HIVE_RETRY_COUNT),
				(Long) context
						.getProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL));
		while (reConnectUtil.shouldRetry()) {
			try {
				LOGGER.info("Executing ThresholdCalculation Query");
				query = getThresholdQuery(context);
				Long totalCount = 0L;
				jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				executeHiveUDF(jobId, jobRunner);
				Long startTimeOfJob = Calendar.getInstance().getTimeInMillis()
						/ 1000;
				Map<Object, Object> boundaryTimeTakenMap = getBoundaryTimeTakenMap(
						context);
				while (nextLb <= ub
						&& nextLb <= dateTransformer.getSystemTime()) {
					if (!interrupted) {
						Long startTime = Calendar.getInstance()
								.getTimeInMillis() / 1000;
						KpiUtil.setFlagInContext(context, nextLb, ub);
						executeThresholdQuery(context, updateBoundary,
								calendarBound, dateTransformer, jobRunner,
								query, ub, lb, nextLb);
						Long endTime = Calendar.getInstance().getTimeInMillis()
								/ 1000;
						LOGGER.info(
								"Threshold Calculation for time: {} is completed in {}",
								lb, dateTransformer.getTimeDescription(
										endTime - startTime));
						Long thresholdTimeTaken = endTime - startTime;
						boundaryTimeTakenMap.put(
								JobExecutionContext.THRESHOLD + nextLb,
								thresholdTimeTaken);
						logTotalLatency(boundaryTimeTakenMap,
								thresholdTimeTaken, lb, nextLb);
						lb = nextLb;
						nextLb = getNextLb(context, calendarTemp,
								dateTransformer, lb);
					} else {
						handleAbortedJob(context);
					}
				}
				Long endTimeOfJob = Calendar.getInstance().getTimeInMillis()
						/ 1000;
				LOGGER.info(
						"Threshold Calculation done till time: {}. Total time taken: {}",
						nextLb, dateTransformer.getTimeDescription(
								endTimeOfJob - startTimeOfJob));
				jobRunner.setQueryHint(
						QueryConstants.RESET_HIVE_OUTPUT_FILE_EXTENSION);
				context.setProperty(JobExecutionContext.DESCRIPTION,
						"Threshold Calculation completed till "
								+ context.getProperty(
										JobExecutionContext.AGGREGATIONDONEDATE)
								+ ". Total records inserted: " + totalCount);
				success = true;
				break;
			} catch (SQLException | UndeclaredThrowableException e) {
				LOGGER.error(" SQL/connection Exception {}", e.getMessage(), e);
				if (reConnectUtil.isRetryRequired(e.getMessage())) {
					if (connTimer != null) {
						connTimer.cancel();
					}
					reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
							context);
				} else {
					handleException(e, context, updateJobStatus);
				}
			} catch (Exception e) {
				LOGGER.error(" Exception {}", e.getMessage(), e);
				context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
						e.getMessage());
				throw new WorkFlowExecutionException(
						"Exception: " + e.getMessage(), e);
			} finally {
				try {
					jobRunner.closeConnection();
				} catch (Exception e) {
					LOGGER.warn("Exception while closing the connection {}",
							e.getMessage());
				}
			}
		}
		return success;
	}

	private Long getNextLb(WorkFlowContext context, Calendar calendarTemp,
			DateFunctionTransformation dateTransformer, long lb) {
		Long nextLb;
		calendarTemp.setTimeInMillis(lb);
		LOGGER.info("New LowerBound value: {}",
				dateTransformer.getFormattedDate(calendarTemp.getTime()));
		Calendar calendar = dateTransformer.getNextBound(lb,
				(String) context.getProperty(JobExecutionContext.PLEVEL));
		nextLb = calendar.getTimeInMillis();
		LOGGER.info("New Next LowerBound value: {}",
				dateTransformer.getFormattedDate(calendar.getTime()));
		return nextLb;
	}

	private void handleAbortedJob(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.debug("Job was aborted");
		updateErrorStatus(context);
		throw new WorkFlowExecutionException(
				"InterruptExpetion: Job was aborted by the user");
	}

	private void executeThresholdQuery(WorkFlowContext context,
			UpdateBoundary updateBoundary, Calendar calendarBound,
			DateFunctionTransformation dateTransformer,
			schedulerJobRunner jobRunner, String query, Long newUb, long lb,
			Long nextLb) throws SQLException, WorkFlowExecutionException,
			IOException, InterruptedException, JobExecutionException {
		connTimer = new Timer();
		calendarBound.setTimeInMillis(lb);
		String sqlToExecute = getSqlToExecute(calendarBound, dateTransformer,
				query, newUb, lb, nextLb, context).replaceAll("\\p{Cntrl}", "");
		ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		LOGGER.debug("ELSE-SQL to execute: {}", sqlToExecute);
		context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
				sqlToExecute);
		connTimer.schedule(new Watcher(), delay * 1000, delay * 1000);
		String sleepTime = (String) context.getProperty(
				JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		Thread logThread = applicationLogger.startApplicationIdLog(
				Thread.currentThread(),
				jobRunner.getPreparedStatement(sqlToExecute), jobId, sleepTime);
		jobRunner.runQuery(sqlToExecute);
		HiveRowCountUtil rowCountUtil = new HiveRowCountUtil();
		Long rowCount = rowCountUtil.getNumberOfRecordsInserted(context, null,
				applicationLogger);
		applicationLogger.stopApplicationIdLog(logThread);
		connTimer.cancel();
		LOGGER.info("The number of records inserted : {}", rowCount);
		context.setProperty(JobExecutionContext.QUERYDONEBOUND, lb);
		updateBoundary.updateBoundaryTable(context, null);
	}

	@SuppressWarnings("unchecked")
	private Map<Object, Object> getBoundaryTimeTakenMap(
			WorkFlowContext context) {
		Map<Object, Object> boundaryTimeTakenMap = new HashMap<Object, Object>();
		if (context
				.getProperty(JobExecutionContext.MAP_FROM_SOURCE_JOB) != null) {
			boundaryTimeTakenMap = (Map<Object, Object>) context
					.getProperty(JobExecutionContext.MAP_FROM_SOURCE_JOB);
		}
		return boundaryTimeTakenMap;
	}

	private void executeHiveUDF(String jobId, schedulerJobRunner jobRunner)
			throws WorkFlowExecutionException, SQLException {
			for (String hiveUDF : HiveConfigurationProvider.getInstance()
				.getQueryHints(jobId,
				executionEngine)) {
			if (hiveUDF.contains(QueryConstants.JOB_ID)) {
				hiveUDF = hiveUDF.replace(QueryConstants.JOB_ID, jobId);
			}
			LOGGER.debug("Hint: {}", hiveUDF);
			jobRunner.setQueryHint(hiveUDF);
		}
	}

	private String getThresholdQuery(WorkFlowContext context) {
		String sql;
		setDelay((Long) context.getProperty(JobExecutionContext.QUERY_TIMEOUT));
		sql = (String) context.getProperty(JobExecutionContext.SQL);
		return sql;
	}

	private void logTotalLatency(Map<Object, Object> boundaryTimeTakenMap,
			Long thresholdTimeTaken, Long lb, Long nextLb) {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		Long importTimeTaken = null;
		Long kpiTimeTaken = null;
		Long profileTimeTaken = null;
		Long totalLatency = thresholdTimeTaken;
		StringBuffer buffer = new StringBuffer(
				"Total latency summary for time ").append(lb).append(":\n");
		if (boundaryTimeTakenMap
				.get(JobExecutionContext.IMPORT + nextLb) != null) {
			importTimeTaken = Long.parseLong((String)boundaryTimeTakenMap
					.get(JobExecutionContext.IMPORT + nextLb));
			totalLatency += importTimeTaken;
			buffer.append("Import job took ").append(
					dateTransformer.getTimeDescription(importTimeTaken));
		}
		if (boundaryTimeTakenMap
				.get(JobExecutionContext.KPI + nextLb) != null) {
			kpiTimeTaken = Long.parseLong((String)boundaryTimeTakenMap
					.get(JobExecutionContext.KPI + nextLb));
			totalLatency += kpiTimeTaken;
			buffer.append("\nKPI job took ")
					.append(dateTransformer.getTimeDescription(kpiTimeTaken));
		}
		if (boundaryTimeTakenMap
				.get(JobExecutionContext.PROFILE + nextLb) != null) {
			profileTimeTaken = Long.parseLong((String)boundaryTimeTakenMap
					.get(JobExecutionContext.PROFILE + nextLb));
			totalLatency += profileTimeTaken;
			buffer.append("\nProfile job took ").append(
					dateTransformer.getTimeDescription(profileTimeTaken));
		}
		buffer.append("\nThreshold job took ")
				.append(dateTransformer.getTimeDescription(thresholdTimeTaken));
		buffer.append("\nOverall latency: ")
				.append(dateTransformer.getTimeDescription(totalLatency));
		if (importTimeTaken == null || kpiTimeTaken == null) {
			buffer.append(
					"\nNote: KPI time taken will not be available (NA) if not through auto trigger");
		}
		LOGGER.info(buffer.toString());

	}

	private void handleException(Exception e, WorkFlowContext context,
			UpdateJobStatus updateJobStatus) throws WorkFlowExecutionException {
		LOGGER.error(" Exception {}", e.getMessage());
		context.setProperty(JobExecutionContext.STATUS, "E");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				e.getMessage());
		updateJobStatus.updateFinalJobStatus(context);
		if (connTimer != null) {
			connTimer.cancel();
		}
		throw new WorkFlowExecutionException("Exception: " + e.getMessage(), e);
	}

	protected String getSqlToExecute(Calendar calendarBound,
			DateFunctionTransformation dateTransformer, String sql, Long newUb,
			Long lb, Long nextLb, WorkFlowContext context) {
		String sqlToExecute = "";
		String timeStampHint = context.getProperty(JobExecutionContext.JOB_NAME)
				+ Long.valueOf(System.currentTimeMillis()).toString();
		sqlToExecute = sql
				.replace(JobExecutionContext.LOWER_BOUND, lb.toString())
				.replace(JobExecutionContext.TIMESTAMP_HINT, timeStampHint);
		sqlToExecute = sqlToExecute.replace(JobExecutionContext.PI_END_TIME,
				nextLb.toString());
		sqlToExecute = sqlToExecute.replace(JobExecutionContext.LOWERBOUND_DATE,
				dateTransformer.getFormattedDate(calendarBound.getTime()));
		sqlToExecute = sqlToExecute.replace(JobExecutionContext.NEXT_UPPERBOUND,
				newUb.toString());
		String enrichMentRequired = (String) context
				.getProperty(JobExecutionContext.ALERT_ENRICHMENT);
		sqlToExecute = sqlToExecute.replace(
				JobExecutionContext.HASH_ALERT_ENRICHMENT,
				enrichMentRequired == null ? "false" : enrichMentRequired);
		context.setProperty(JobExecutionContext.TIMESTAMP_HINT, timeStampHint);
		KpiUtil.setFlagInContext(context, nextLb, newUb);
		return sqlToExecute;
	}

	private Map<String, Long> getBoundValuesFromContext(
			WorkFlowContext context) {
		Map<String, Long> lowerUpperBoundValues = new HashMap<String, Long>();
		lowerUpperBoundValues.put(ThresholdCalculationJob.LOWER_BOUND,
				(Long) context.getProperty(JobExecutionContext.LB));
		lowerUpperBoundValues.put(ThresholdCalculationJob.UPPER_BOUND,
				(Long) context.getProperty(JobExecutionContext.UB));
		return lowerUpperBoundValues;
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	private static class Watcher extends TimerTask {

		public Watcher() {
			super();
		}

		@Override
		public void run() {
			LOGGER.debug("Watcher called at: {}", System.currentTimeMillis());
			LOGGER.info("Job is not responding for {} mins.", delay / 60);
		}
	}
}

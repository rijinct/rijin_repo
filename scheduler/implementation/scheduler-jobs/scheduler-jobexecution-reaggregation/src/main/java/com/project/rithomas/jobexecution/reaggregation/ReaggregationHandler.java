
package com.project.rithomas.jobexecution.reaggregation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.etl.notifier.NotificationInitializationException;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.AutoTriggerJobs;
import com.project.rithomas.jobexecution.common.DQICalculator;
import com.project.rithomas.jobexecution.common.JobTriggerPrecheck;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public abstract class ReaggregationHandler extends AbortableWorkflowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggregationHandler.class);

	DateFunctionTransformation dateTransformer = DateFunctionTransformation
			.getInstance();

	public String jobId;

	private String sourceJobsInQuotes;

	private NotificationHandler notificationHandler = new NotificationHandler();

	private Long retentionDtValue;

	protected boolean isArchivingEnabled;

	private boolean updateQsUpperBoundary;

	ReaggregationListUtil reaggListUtil = new ReaggregationListUtil();

	private String regionId = null;

	protected WorkFlowContext context;

	protected String aggLevel;

	protected ApplicationLoggerUtilInterface applicationLogger;

	UpdateJobStatus updateJobStatus = new UpdateJobStatus();

	private String aggJobName;

	private List<String> sourceJobs = new ArrayList<String>();
	
	private List reaggInitializedList = null;

	public ReaggregationHandler(WorkFlowContext context) {
		this.context = context;
		this.jobId = (String) context.getProperty(JobExecutionContext.JOB_NAME);
		this.retentionDtValue = ReaggCommonUtil.getRetentionDayValue(context);
		this.aggJobName = (String) context
				.getProperty(JobExecutionContext.PERF_JOB_NAME);
		this.aggLevel = (String) context
				.getProperty(JobExecutionContext.ALEVEL);
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		sourceJobs = boundaryQuery.getSourceJobIds(jobId);
		this.sourceJobsInQuotes = ReaggCommonUtil
				.getSourceJobsInQuotes(sourceJobs);
	}

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		applicationLogger = ApplicationLoggerFactory.getApplicationLogger();
		QueryExecutor queryExecutor = new QueryExecutor();
		ReConnectUtil reConnectUtil = new ReConnectUtil(
				(Integer) context.getProperty(ReConnectUtil.HIVE_RETRY_COUNT),
				(Long) context
						.getProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL));
		LOGGER.info("Reaggregation process started....");
		if (sourceJobsInQuotes.isEmpty()) {
			throw new WorkFlowExecutionException(
					"No source jobs found for the given re-aggregation job!");
		}
		while (reConnectUtil.shouldRetry()) {
			try {
				ReaggCommonUtil.getMinimumOfLoadTime(jobId, queryExecutor,
						context);
				this.isArchivingEnabled = ReaggCommonUtil
						.populateArchiveInfo(context);
				reaggInitializedList = ReaggCommonUtil.getReaggList(jobId,
						queryExecutor, context);
				boolean isLoopBackRequired = true;
				if (CollectionUtils.isNotEmpty(reaggInitializedList)) {
					Set<String> regionIds = new HashSet<>();
					String prevReportTime = null;
					do {
						updateQsUpperBoundary = true;
						for (Object itObj : reaggInitializedList) {
							if (!interrupted) {
								if (itObj.getClass().isArray()) {
									if (prevReportTime != null) {
										triggerNextLevel(dateTransformer,
												prevReportTime, context,
												regionIds);
									}
									Object[] data = (Object[]) itObj;
									String reportTime = data[3].toString();
									updateTzInfoAndReaggList(data, regionIds,
											context);
									checkIfAggCompleted(context, queryExecutor,
											reportTime);
									context.setProperty(
											JobExecutionContext.REAGG_LAST_LB,
											reportTime);
									Long lowerBoundInMillis = dateTransformer
											.getDate(reportTime).getTime();
									Calendar nextBound = dateTransformer
											.getNextBound(lowerBoundInMillis,
													aggLevel);
									context.setProperty(
											JobExecutionContext.LB_QS_JOB,
											lowerBoundInMillis);
									context.setProperty(
											JobExecutionContext.REAGG_LB,
											lowerBoundInMillis);
									context.setProperty(
											JobExecutionContext.REAGG_NEXT_LB,
											Long.valueOf(nextBound
													.getTimeInMillis()));
									context.setProperty(
											JobExecutionContext.REAGG_TZ_RGN,
											regionId);
									context.setProperty(
											JobExecutionContext.REAGG_QS_UB_STATUS,
											updateQsUpperBoundary);
									Long reagg_lb = (Long) context
											.getProperty("REAGG_LB");
									LOGGER.info("Lower bound value: {}",
											reportTime);
									LOGGER.info("Next bound value: {}",
											nextBound.getTime());
									executeHandler(context);
									calculateDQI(context, lowerBoundInMillis,
											nextBound);
									updateReaggStatus(
											aggJobName, reportTime, nextBound);
									prevReportTime = reportTime;
								}
							}
						}
						isLoopBackRequired = checkLoopBackRequired(
								context, queryExecutor);
					} while (isLoopBackRequired);
					if (isArchivingEnabled && ReaggCommonUtil
							.isArchivedInDifferentTable(aggLevel)) {
						ReaggCommonUtil.dropPartitionifArchived(context,
								dateTransformer.getDate(prevReportTime)
										.getTime());
					}
					ReaggCommonUtil.dropPartitions(context, jobId,
							retentionDtValue);
					ReaggCommonUtil.handleDependentJobs(jobId,
							sourceJobsInQuotes, regionIds, context);
				} else {
					LOGGER.info(
							"No values returned from re-aggregation list. No data to be re-aggregated. ");
					context.setProperty(JobExecutionContext.DESCRIPTION,
							"No data to be re-aggregated.");
				}
				sendNotification();
				LOGGER.info("Reaggregation process completed successfully.");
				break;
			} catch (NotificationInitializationException e) {
				updateJobStatus.updateETLErrorStatusInTable(e, context);
				context.setProperty(
						JobExecutionContext.UPDATE_FINAL_STATUS_REQUIRED,
						"false");
				break;
			} catch (Exception e) {
				if (e instanceof SQLException
						&& reConnectUtil.isRetryRequired(e.getMessage())) {
					reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
							context);
				} else {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					ReaggCommonUtil.updateReaggListToInitialized(context,
							reaggListUtil, jobId, sourceJobsInQuotes, regionId);
					throw new WorkFlowExecutionException(
							"Exception while executing postgres query: "
									+ e.getMessage(),
							e);
				}
			}
		}
		return true;
	}

	private boolean checkLoopBackRequired(WorkFlowContext context,
			QueryExecutor queryExecutor) throws JobExecutionException {
		boolean isLoopBackRequired;
		reaggInitializedList = ReaggCommonUtil.getReaggList(jobId,
				queryExecutor, context);
		updateQsUpperBoundary = true;
		isLoopBackRequired = CollectionUtils.isNotEmpty(reaggInitializedList);
		if (isLoopBackRequired) {
			LOGGER.info(
					"Continuing Reaggregation process with newly Initialized entries...");
		}
		return isLoopBackRequired;
	}
	
	

	private void checkIfAggCompleted(WorkFlowContext context,
			QueryExecutor queryExecutor, String reportTime)
			throws JobExecutionException, WorkFlowExecutionException {
		String maxValueQuery = "select maxvalue from rithomas.boundary where jobid='"
				+ aggJobName + "' and maxvalue>='" + reportTime + "'";
		Object[] result = queryExecutor.executeMetadatasqlQuery(maxValueQuery,
				context);
		if (result == null || result.length == 0) {
			LOGGER.error(
					"Normal aggregation not yet done for the report time: {}",
					reportTime);
			throw new WorkFlowExecutionException(
					"Normal aggregation is not yet done for the interval: "
							+ reportTime);
		}
	}

	private void updateReaggStatus(String aggJobName, String reportTime,
			Calendar nextBound) throws JobExecutionException {
		UsageAggregationStatusUtil util = new UsageAggregationStatusUtil();
		util.updateUsageAggList(context, aggJobName, reportTime,
				dateTransformer.getFormattedDate(nextBound.getTime()),
				regionId);
		reaggListUtil.updateReaggregationList(context, jobId,
				sourceJobsInQuotes, reportTime,
				CommonConstants.COMPLETED_PART_STATUS,
				CommonConstants.RUNNING_STATUS, regionId);
	}

	private void calculateDQI(WorkFlowContext context, Long lowerBoundInMillis,
			Calendar nextBound) throws WorkFlowExecutionException, SQLException,
			JobExecutionException, ETLException {
		String dqmEnabled = (String) context
				.getProperty(JobExecutionContext.DQI_ENABLED);
		if ("YES".equalsIgnoreCase(dqmEnabled)) {
			DQICalculator dqiCalculator = new DQICalculator();
			dqiCalculator.calculateDqi(context, lowerBoundInMillis,
					Long.valueOf(nextBound.getTimeInMillis()));
		}
	}

	private void triggerNextLevel(DateFunctionTransformation dateTransformer,
			String prevReportTime, WorkFlowContext context,
			Set<String> regionIds) throws Exception {
		boolean trigger = ReaggCommonUtil.shouldTriggerNextLevel(
				dateTransformer, aggLevel, prevReportTime, (String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY));
		if (trigger) {
			ReaggCommonUtil.handleDependentJobs(jobId, sourceJobsInQuotes,
					regionIds, context);
			sendNotification();
			new JobTriggerPrecheck().execute(context);
			new AutoTriggerJobs().execute(context);
			ReaggCommonUtil.dropPartitions(context, jobId, retentionDtValue);
			if (isArchivingEnabled
					&& ReaggCommonUtil.isArchivedInDifferentTable(aggLevel)) {
				ReaggCommonUtil.dropPartitionifArchived(context,
						dateTransformer.getDate(prevReportTime).getTime());
			}
			this.updateQsUpperBoundary = true;
		}
	}

	private void sendNotification()
			throws JobExecutionException, NotificationInitializationException {
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		notificationHandler.handleNotifications(context, reaggListUtil,
				this.jobId, targetTable, CommonConstants.EMAIL_HEADER,
				sourceJobsInQuotes);
	}

	private void updateTzInfoAndReaggList(Object[] data, Set<String> regionIds,
			WorkFlowContext context) throws JobExecutionException {
		String reportTime = data[3].toString();
		this.regionId = data[5] != null ? data[5].toString() : null;
		if (regionId != null) {
			regionIds.add(regionId);
		}
		reaggListUtil.updateReaggregationList(context, jobId,
				sourceJobsInQuotes, reportTime, CommonConstants.RUNNING_STATUS,
				CommonConstants.INITIALIZED_STATUS, regionId);
	}
	
	

	public abstract void executeHandler(WorkFlowContext context)
			throws SQLException, Exception;
}

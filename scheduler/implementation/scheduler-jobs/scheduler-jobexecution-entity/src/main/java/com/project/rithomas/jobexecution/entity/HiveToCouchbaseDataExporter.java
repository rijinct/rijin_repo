
package com.project.rithomas.jobexecution.entity;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.IOException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.etl.common.utils.ETLCommonUtils;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.entity.notifier.DimensionRefresher;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class HiveToCouchbaseDataExporter extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveToCouchbaseDataExporter.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		boolean jobExecuted = false;
		if (context != null) {
			boolean cacheLoadingEnabled = toBoolean(
					(String) context.getProperty(
							GeneratorWorkFlowContext.CACHE_LOADING_ENABLED));
			LOGGER.debug(
					"Cache loading property:{} is {}, so exporting data from Hive to Couchbase.",
					GeneratorWorkFlowContext.CACHE_LOADING_ENABLED,
					cacheLoadingEnabled);
			if (cacheLoadingEnabled) {
				LOGGER.info("Exporting data from Hive to CouchBase.");
				try {
					String jobName = (String) context
							.getProperty(JobExecutionContext.JOB_NAME);
					if (isClearCodeDimensionJob(jobName)) {
						LOGGER.info("It's a clear code dimension");
						jobExecuted = loadClearCodeDimensions(context);
					} else {
						LOGGER.info("Invoking loadDataToCouchbase");
						jobExecuted = loadDataToCouchbase(context);
					}
					LOGGER.info("jobExecuted : {}", jobExecuted);
					String description = schedulerConstants.DIMENSION_LOADING_SUCCESS_MESSAGE;
					if (jobExecuted) {
						triggerTopologyRefresh(ETLCommonUtils.getEntitySpecFromJobName(jobName).getName());
					} else {
						description = "Target Table Not Defined.";
					}
					context.setProperty(JobExecutionContext.DESCRIPTION,
							description);
					LOGGER.debug("Completed loading for Non-CLEAR_CODE jobs");
				} catch (Exception exception) {
					String errorDescription = new StringBuilder(
							schedulerConstants.HIVE_TO_COUCHBASE_ERROR_MESSAGE)
									.append(exception.getLocalizedMessage())
									.toString();
					context.setProperty(JobExecutionContext.DESCRIPTION,
							errorDescription);
					updateJobStatus.updateETLErrorStatusInTable(exception,
							context);
					LOGGER.error(errorDescription);
					throw new WorkFlowExecutionException(errorDescription,
							exception);
				}
			} else {
				jobExecuted = true;
			}
		}
		return jobExecuted;
	}

	private void triggerTopologyRefresh(String dimensionName)
			throws InterruptedException, IOException {
		getDimensionRefresher().notifyTopicsToRefreshCache(dimensionName);
	}

	private boolean loadClearCodeDimensions(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean jobExecuted;
		context.setProperty(JobExecutionContext.JOB_NAME,
				JobExecutionContext.CLEAR_CODE_JOBNAME);
		context.setProperty(JobExecutionContext.TARGET,
				JobExecutionContext.CLEAR_CODE_TABLENAME);
		jobExecuted = loadDataToCouchbase(context);
		LOGGER.debug("Completed loading for CLEAR_CODE job");
		// run SMS_CLEAR_CODE_JOB after running
		// CLEAR_CODE_GROUP_JOB
		context.setProperty(JobExecutionContext.JOB_NAME,
				JobExecutionContext.SMS_CLEAR_CODE_JOBNAME);
		context.setProperty(JobExecutionContext.TARGET,
				JobExecutionContext.SMS_CLEAR_CODE_TABLENAME);
		jobExecuted = loadDataToCouchbase(context);
		LOGGER.debug("Completed loading for SMS CLEAR_CODE group");
		return jobExecuted;
	}

	private boolean isClearCodeDimensionJob(String jobName) {
		return JobExecutionContext.CLEAR_CODE_GROUP_JOBNAME
				.equalsIgnoreCase(jobName);
	}

	private DimensionRefresher getDimensionRefresher() {
		return new DimensionRefresher();
	}

	private boolean loadDataToCouchbase(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.info("Inside loadDataToCouchbase");
		String sourceJobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String sourceHiveTableName = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String parallelism = (String) context
				.getProperty(JobExecutionContext.CACHE_LOADING_PARALLELISM);
		LOGGER.info(
				"sourceJobName : {}, sourceHiveTableName : {}, parallelism : {}",
				sourceJobName, sourceHiveTableName, parallelism != null
						? parallelism : "default number of threads");
		CouchbaseDataLoader couchbaseloader = getCouchbaseDataLoader(
				sourceJobName, sourceHiveTableName, parallelism);
		return couchbaseloader.load();
	}

	private CouchbaseDataLoader getCouchbaseDataLoader(String jobName,
			String hiveTableName, String parallelThreadCount) {
		return new CouchbaseDataLoader(jobName, hiveTableName,
				parallelThreadCount);
	}
}
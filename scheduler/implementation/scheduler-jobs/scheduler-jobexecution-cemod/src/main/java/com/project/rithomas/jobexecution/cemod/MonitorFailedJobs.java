
package com.project.rithomas.jobexecution.project;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.AutoTriggerJobs;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class MonitorFailedJobs extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MonitorFailedJobs.class);

	private static final String JOB_EXECUTION_EXCEPTION = "JobExecutionException:";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		Long currentDate = dateTransformer.getSystemTime();
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobId = null;
		AutoTriggerJobs autoTriggerJobs = new AutoTriggerJobs();
		context.setProperty(JobExecutionContext.CURRENT_SYSTEM_DATE,
				currentDate);
		String jobTypesToBeExcluded = (String) context
				.getProperty(JobExecutionContext.JOBTYPE_TO_BE_EXCLUDED);
		LOGGER.debug("The monitor job got triggred at " + currentDate);
		QueryExecutor queryExecutor = new QueryExecutor();
		Calendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(currentDate);
		String formattedDate = DateFunctionTransformation.getInstance()
				.getFormattedDate(calendar.getTime());
		calendar.setTime(DateFunctionTransformation.getInstance()
				.getDate(formattedDate));
		Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
		try {
			String getListOfRetriggre = "select  distinct job_name from rithomas.etl_status where type not in ("
					+ jobTypesToBeExcluded
					+ ") and end_time > (select greatest(maxvalue, current_date - interval '1 day') from rithomas.boundary where jobid='MONITOR_FAILED_JOBS') and status='E'  and end_time < '"
					+ timestamp + "'";
			LOGGER.info("The string to be exec " + getListOfRetriggre);
			List<String> jobsTobeAdded = queryExecutor
					.executeMetadatasqlQueryMultiple(getListOfRetriggre,
							context);
			List<String> jobsToBeTriggered = new ArrayList<String>();
			for (String jobTobeAdded : jobsTobeAdded) {
				jobsToBeTriggered.add(jobTobeAdded);
				LOGGER.debug("The jobs to be triggred " + jobTobeAdded);
			}
			context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
					jobsToBeTriggered);
			LOGGER.debug("Number of jobs to be retriggred which were in E "
					+ +jobsToBeTriggered.size());
			if (isKubernetesEnvironment()) {
				context.setProperty(JobExecutionContext.ASYNCHRONOUS_EXECUTION, true);
			}
			autoTriggerJobs.execute(context);

			jobId = (String) context.getProperty(JobExecutionContext.JOB_NAME);
			LOGGER.info(" the job name is " + jobId);
			context.setProperty(JobExecutionContext.QUERYDONEBOUND,
					currentDate);
			UpdateBoundary boundary = new UpdateBoundary();
			boundary.updateBoundaryTable(context, null);
			success = true;
		} catch (JobExecutionException e) {
			success = false;
			LOGGER.error("The job failed for below error " + e.getMessage());
			throw new WorkFlowExecutionException(
					JOB_EXECUTION_EXCEPTION + e.getMessage(), e);
		} finally {
			updateETLStatus(context, updateJobStatus, jobId, success);
		}
		return success;
	}

	protected void updateETLStatus(WorkFlowContext context,
			UpdateJobStatus updateJobStatus, String jobId, boolean success)
			throws WorkFlowExecutionException {
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		LOGGER.debug("updating status for job " + jobId);
		JobDictionary jobDictionary = jobDictionaryQuery.retrieve(jobId);
		String jobType = jobDictionary.getTypeid().getJobtypeid();
		context.setProperty(JobExecutionContext.RETURN, 0);
		if (success) {
			context.setProperty(JobExecutionContext.STATUS, "S");
		} else {
			context.setProperty(JobExecutionContext.STATUS, "E");
		}
		context.setProperty(JobExecutionContext.JOBTYPE, jobType);
		updateJobStatus.updateFinalJobStatus(context);
	}
	
	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}
}

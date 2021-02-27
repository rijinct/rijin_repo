
package com.project.rithomas.jobexecution.usage;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.AutoTriggerJobs;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class LoopBack extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LoopBack.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		Date currentDate = new Date();
		List<String> jobsToBeTriggered = new ArrayList<String>();
		if (JobExecutionContext.TRUE.equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.CONTINUOUS_LOADING))) {
			Date startDate = DateFunctionTransformation.getInstance()
					.getDate((String) context
							.getProperty(JobExecutionContext.JOB_START_DATE));
			LOGGER.debug(" Current  date of System is   "
					+ DateFunctionTransformation.getInstance()
							.getFormattedDate(currentDate)
					+ " The  start date diff " + DateFunctionTransformation
							.getInstance().getFormattedDate(startDate));
			int usageRetriggerDuration = Integer.valueOf((String) context
					.getProperty(JobExecutionContext.USAGE_RETRIGGER_DURATION));
			if (!isKubernetesEnvironment() && isDateChanged(currentDate, startDate,
					usageRetriggerDuration)) {
				LOGGER.info("Usage job completed for the scheduled duration  "
						+ usageRetriggerDuration
						+ " hours .Thus Ending the current usage Job and Auto triggering the same");
				autoTriggerSameJob(context, jobsToBeTriggered);
			} else {
				LOGGER.debug(
						"Continuous loading set to true. Looping back for checking files in import..");
				context.setProperty(JobExecutionContext.SEQUENCE, 1);
				context.setProperty(JobExecutionContext.PREV_STATUS, "S");
			}
		} else {
			LOGGER.debug(
					"Continuous loading set to false. Exiting the execution of the job..");
		}
		return true;
	}

	protected void autoTriggerSameJob(WorkFlowContext context,
			List<String> jobsToBeTriggered) throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.RETURN, 0);
		context.setProperty(JobExecutionContext.PREV_STATUS, "S");
		jobsToBeTriggered.add(
				(String) context.getProperty(JobExecutionContext.JOB_NAME));
		context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
				jobsToBeTriggered);
		AutoTriggerJobs autoTriggerJobs = new AutoTriggerJobs();
		autoTriggerJobs.execute(context);
	}

	private boolean isDateChanged(Date currentDate, Date startDate,
			int usageRetriggerDuration) {
		long timeDiff = Math
				.abs(((long)currentDate.getTime()) - startDate.getTime());
		return TimeUnit.MILLISECONDS.toSeconds(timeDiff)
				/ (JobExecutionContext.USAGE_SPLIT_TIME
						* usageRetriggerDuration) > 0;
	}
	
	private boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}
}

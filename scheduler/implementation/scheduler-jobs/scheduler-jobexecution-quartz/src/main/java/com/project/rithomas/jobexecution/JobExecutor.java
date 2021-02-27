
package com.project.rithomas.jobexecution;

import java.util.Map;

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.UnableToInterruptJobException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.BaseJobProcessor;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class JobExecutor extends BaseJobProcessor implements InterruptableJob {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobExecutor.class);

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobKey jobKeyInfo = context.getJobDetail().getKey();
		String jobName = jobKeyInfo.getName();
		String jobGroup = jobKeyInfo.getGroup();
		Map<String, Object> jobMetaData = context.getMergedJobDataMap();
		try {
			processJob(jobName, jobMetaData, jobGroup, "default");
		} catch (WorkFlowExecutionException exception) {
			LOGGER.error("Exception while processing Job Execution :",
					exception);
			throw new JobExecutionException("WorkFlowExecutionException : ",
					exception);
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		interruptJob();
	}
}
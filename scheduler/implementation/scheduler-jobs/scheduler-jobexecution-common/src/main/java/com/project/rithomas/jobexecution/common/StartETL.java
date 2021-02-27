
package com.project.rithomas.jobexecution.common;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class StartETL extends AbstractWorkFlowStep {

	public final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(StartETL.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		LOGGER.info("Status of job is updating...");
		context.setProperty(JobExecutionContext.STATUS, "R");
		updateJobStatus.insertJobStatus(context);
		context.setProperty(JobExecutionContext.RETURN, 0);
		return success;
	}
}

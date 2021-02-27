
package com.project.rithomas.jobexecution.common;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class StopETL extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(StopETL.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "S");
		LOGGER.info("Updating the final Job Status");
		if (context.getProperty(
				JobExecutionContext.UPDATE_FINAL_STATUS_REQUIRED) == null
				|| context.getProperty(
						JobExecutionContext.UPDATE_FINAL_STATUS_REQUIRED) != "false") {
			updateJobStatus.updateFinalJobStatus(context);
		} else {
			LOGGER.info("Updating the final Job Status to E");
		}
		context.setProperty(JobExecutionContext.RETURN, 0);
		return success;
	}
}

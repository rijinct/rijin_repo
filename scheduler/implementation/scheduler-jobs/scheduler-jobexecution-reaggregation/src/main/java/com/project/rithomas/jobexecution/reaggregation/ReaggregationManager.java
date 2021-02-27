
package com.project.rithomas.jobexecution.reaggregation;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ReaggregationManager extends AbortableWorkflowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggregationManager.class);

	private static final String SCRIPT_NAME = "SCRIPT_NAME";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		AbortableWorkflowStep obj;
		boolean rtv = false;
		try {
			String aggJobName = (String) context
					.getProperty(JobExecutionContext.PERF_JOB_NAME);
			String scriptToExecute = (String) context
					.getProperty(aggJobName + "_" + SCRIPT_NAME);
			LOGGER.info("Script to execute:{}", scriptToExecute);
			if (scriptToExecute == null) {
				obj = new HiveReaggregation(context);
			} else {
				obj = new ReaggScriptExecutor(context);
			}
			rtv = obj.execute(context);
		} catch (Exception ex) {
			LOGGER.error("Exception occured", ex);
		}
		return rtv;
	}
}

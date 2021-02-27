
package com.project.rithomas.jobexecution.common.boundary;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class RuntimePropertyRetriever {

	private static final Long THRESHOLD_VALUE = 3600000L * 24L;

	WorkFlowContext context;

	public RuntimePropertyRetriever(WorkFlowContext context) {
		this.context = context;
	}

	public Long getWaitTimeToProceedAgg() {
		Long thresholdValue = THRESHOLD_VALUE;
		if (context.getProperty(JobExecutionContext.THRESHOLDVALUE) != null) {
			thresholdValue = (Long) context
					.getProperty(JobExecutionContext.THRESHOLDVALUE);
		}
		return thresholdValue;
	}

	public String getWeekStartDay() {
		return (String) context.getProperty(JobExecutionContext.WEEK_START_DAY);
	}
}

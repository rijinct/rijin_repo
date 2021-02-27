
package com.project.rithomas.jobexecution.common.util;

import java.util.HashMap;
import java.util.Map;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class CommonCorrelationUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CommonCorrelationUtil.class);

	public static void constructMapForDependentQSJob(WorkFlowContext context) {
		Map<Object, Object> mapForDependentJobs = new HashMap<Object, Object>();
		mapForDependentJobs.put(JobExecutionContext.SOURCE,
				context.getProperty(JobExecutionContext.TARGET));
		mapForDependentJobs.put(JobExecutionContext.SOURCEJOBTYPE,
				context.getProperty(JobExecutionContext.JOBTYPE));
		mapForDependentJobs.put(JobExecutionContext.SOURCE_JOB_NAME,
				context.getProperty(JobExecutionContext.JOB_NAME));
		context.setProperty(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS,
				mapForDependentJobs);
		LOGGER.debug(
				"Adding the source table name : {} and source job type : {}",
				context.getProperty(JobExecutionContext.TARGET),
				context.getProperty(JobExecutionContext.JOBTYPE));
	}
}

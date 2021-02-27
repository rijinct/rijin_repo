
package com.rijin.analytics.scheduler.k8s;

import org.springframework.stereotype.Component;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@Component
public class JobStatusUpdater {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobStatusUpdater.class);

	public void updateJobsToError() {
		WorkFlowContext context = new JobExecutionContext();
		QueryExecutor executor = new QueryExecutor();
		try {
			GetDBResource.getInstance()
					.retrievePostgresObjectProperties(context);
			executor.executePostgresqlUpdate(
					QueryConstants.SE_RESTART_UPDATE_QUERY, context);
		} catch (Exception e) {
			LOGGER.error("Exception while updating DB", e);
		}
	}
}


package com.project.rithomas.jobexecution.common.step;

import org.apache.hadoop.conf.Configuration;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public abstract class AbortableWorkflowStep extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AbortableWorkflowStep.class);

	protected boolean interrupted;

	@Override
	public boolean abort(WorkFlowContext context) {
		LOGGER.debug("Calling abort..");
		ApplicationLoggerUtilInterface applicationLogger = (ApplicationLoggerUtilInterface) context
				.getProperty(JobExecutionContext.APPLICATION_ID_LOGGER);
		LOGGER.debug("The application logger object retrived is  {}",
				applicationLogger);
		this.interrupted = (Boolean) context
				.getProperty(JobExecutionContext.INTERRUPTED);
		if (applicationLogger != null) {
			Configuration conf = GetDBResource.getInstance()
					.getHdfsConfiguration();
			String jobId = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			applicationLogger.killApplication(conf, jobId);
		}
		LOGGER.info("Thread has been interrupted ? :: {}", interrupted);
		return false;
	}
}

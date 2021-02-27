
package com.project.rithomas.jobexecution.common;

import java.util.List;

import org.hibernate.Session;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlow;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowType;

public class JobExecutionWorkflow extends AbstractWorkFlow {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobExecutionWorkflow.class);

	private static final boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private WorkFlowContext context;

	private boolean interrupted;

	private WorkFlowStep step;

	@Override
	public void executeWorkflow(WorkFlowContext context)
			throws WorkFlowExecutionException {
		List<WorkFlowStep> workFlowSteps = getWorkFlowSteps();
		this.context = context;
		context.setProperty(JobExecutionContext.IS_PARTITION_DROPPED, "false");
		for (Integer sequence = 0; sequence < workFlowSteps
				.size(); sequence++) {
			if (DEBUGMODE) {
				LOGGER.debug("Thread has been interrupted ? :: {}",
						interrupted);
			}
			boolean success = false;
			boolean stepExecuted = false;
			
			try {
				if (!interrupted) {
					context.setProperty(JobExecutionContext.SEQUENCE, sequence);
					step = workFlowSteps.get(sequence);
					success = step.execute(context);
					stepExecuted = true;
					sequence = (Integer) context
							.getProperty(JobExecutionContext.SEQUENCE);
					Integer returnVal = (Integer) context
							.getProperty(JobExecutionContext.RETURN);
					LOGGER.debug("return value : {}, is success : {}",
							returnVal, success);
					if (success) {
						if (returnVal == 1) {
							sequence = workFlowSteps.size() - 2;
						} else if (returnVal == 2) {
							sequence = workFlowSteps.size() - 1;
						}
						LOGGER.debug("sequence value : {}", sequence);
					}
				} else {
					throw new WorkFlowExecutionException(
							"Unable to continue with workflow execution as job "
									+ context.getProperty(
											JobExecutionContext.JOB_NAME)
									+ " was aborted by the user.");
				}
			} finally {
				LOGGER.debug(
						"Is step success : {}, does etl status have entry : {}",
						success, context.getProperty(
								JobExecutionContext.ETL_STATUS_SEQ));
				if (!success && context.getProperty(
						JobExecutionContext.ETL_STATUS_SEQ) != null) {
					LOGGER.debug("Updating the etl status to E.");
					context.setProperty(JobExecutionContext.STATUS, "E");
					UpdateJobStatus updateJobStatus = new UpdateJobStatus();
					updateJobStatus.updateFinalJobStatus(context);
					if (!interrupted && stepExecuted) {
						throw new WorkFlowExecutionException(
								"The workflow execution fails for the job "
										+ context.getProperty(
												JobExecutionContext.JOB_NAME));
					}
				}
			}
		}
	}


	
	@Override
	public WorkFlowType getWorkFlowType() {
		return WorkFlowType.JOB_EXECUTION;
	}

	public void interrupt(boolean interrupted) {
		LOGGER.info(
				"Received interrupt signal from JobExecutionWorkflow for job : {} ",
				this.context.getProperty(JobExecutionContext.JOB_NAME));
		this.context.setProperty(JobExecutionContext.DESCRIPTION,
				"Interrupted by User");
		Session postgreSession = (Session) this.context
				.getProperty(JobExecutionContext.POSTGRE_SESSION);
		if (postgreSession != null && postgreSession.isOpen()) {
			if (DEBUGMODE) {
				LOGGER.debug("Cancelling postgre query");
			}
			postgreSession.cancelQuery();
		}
		this.interrupted = interrupted;
		context.setProperty(JobExecutionContext.INTERRUPTED, true);
		LOGGER.debug("Calling abort on " + step.toString());
		step.abort(context);
		LOGGER.debug("Exiting from interrupt method");
	}

}


package com.project.rithomas.jobexecution.project;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class SubscriptionUpdateAge extends AbortableWorkflowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SubscriptionUpdateAge.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		LOGGER.info("Starting Dimension Update");
		schedulerJobRunner jobRunner = null;
		String sqlToExecute = null;
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory
				.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		String executionEngine = JobExecutionUtil.getExecutionEngine(context);
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		try {
			ReConnectUtil reConnectUtil = new ReConnectUtil();
			while (reConnectUtil.shouldRetry()) {
				if (!interrupted) {
					try {
						context.setProperty(JobExecutionContext.ADAPTATION_ID,
								"CRM_DIMENSION");
						context.setProperty(
								JobExecutionContext.ADAPTATION_VERSION, "1");
						GetDBResource.getInstance()
								.retrievePostgresObjectProperties(context);
						jobRunner = schedulerJobRunnerfactory.getRunner(
								executionEngine, (boolean) context.getProperty(
										JobExecutionContext.IS_CUSTOM_DB_URL));
						for (String hiveUDF : HiveConfigurationProvider
								.getInstance()
								.getQueryHints(jobId, executionEngine)) {
							jobRunner.setQueryHint(hiveUDF);
						}
						sqlToExecute = QueryConstants.UPDATE_SUBSCRIPTION_AGE;
						context.setProperty(
								JobExecutionContext.HIVE_SESSION_QUERY,
								sqlToExecute);
						LOGGER.info(
								"Executing query for updating Customer age. SQL:{}",
								sqlToExecute);
						String sleepTime = (String) context.getProperty(
								JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
						Thread logThread = applicationLogger
								.startApplicationIdLog(Thread.currentThread(),
										jobRunner.getPreparedStatement(
												sqlToExecute),
										jobId, sleepTime);
						jobRunner.runQuery(sqlToExecute);
						applicationLogger.stopApplicationIdLog(logThread);
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Dimension Update successful");
						success = true;
						context.setProperty(JobExecutionContext.RETURN, 0);
						break;
					} catch (SQLException | UndeclaredThrowableException e) {
						LOGGER.error(" Exception {}", e.getMessage());
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						if (e.getMessage()
								.contains(ReConnectUtil.TTRANSPORT_EXCEPTION)) {
							reConnectUtil.updateRetryAttemptsForHive(
									e.getMessage(), context);
						} else {
							throw new WorkFlowExecutionException(
									"SQLException: " + e.getMessage(), e);
						}
					} catch (Exception e) {
						LOGGER.error(" Exception {}", e.getMessage());
						context.setProperty(
								JobExecutionContext.ERROR_DESCRIPTION,
								e.getMessage());
						throw new WorkFlowExecutionException(
								"Exception: " + e.getMessage(), e);
					}
				} else {
					throw new WorkFlowExecutionException(
							"Unable to continue with workflow execution as job "
									+ context.getProperty(
											JobExecutionContext.JOB_NAME)
									+ " was aborted by the user.");
				}
			}
		} finally {
			try {
				jobRunner.closeConnection();
				if (!success) {
					updateJobStatus.updateFinalJobStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while clossing the connection {}",
						e.getMessage());
			}
		}
		return success;
	}
}

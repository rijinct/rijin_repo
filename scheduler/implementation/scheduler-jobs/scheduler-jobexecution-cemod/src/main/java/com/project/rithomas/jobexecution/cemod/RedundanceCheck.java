
package com.project.rithomas.jobexecution.project;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class RedundanceCheck extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AbstractWorkFlowStep.class);

	private static final String SQL_EXCEPTION = "SQLException:";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		schedulerJobRunner jobRunner = null;
		ResultSet resultSet = null;
		String sqlToExecute = null;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		try {
			jobRunner = schedulerJobRunnerfactory.getRunner(
					schedulerConstants.HIVE_DATABASE, (boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			if (JobExecutionContext.ES_LOCATION_JOBID.equals(jobId)) {
				for (String hiveUDF : HiveConfigurationProvider.getInstance()
						.getQueryHints(jobId,
								schedulerConstants.HIVE_DATABASE)) {
					jobRunner.setQueryHint(hiveUDF);
				}
				context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
						sqlToExecute);
				context.setProperty(JobExecutionContext.FLAG, "false");
				LOGGER.info(
						"The query for searching redundant ES_LOCATION_1 : {}",
						QueryConstants.SQL_REDUNDANT_REGION);
				resultSet = (ResultSet) jobRunner.runQuery(
						QueryConstants.SQL_REDUNDANT_REGION, null, context);
				if (resultSet.next()) {
					LOGGER.info(" The value of region : {}",
							resultSet.getMetaData().getColumnCount());
					LOGGER.info(
							"For location job loading each region shd be mapped to same mnc and mnc else Latitude and longitude will shown incorrectly");
					LOGGER.info(
							"Because few regions have multiple mcc,mnc job is failing");
					LOGGER.info("the record being rejected is : {},{},{}",
							resultSet.getString("region"),
							resultSet.getString("mcc"),
							resultSet.getString("mnc"));
					while (resultSet.next()) {
						LOGGER.info("the record being rejected is : {},{},{}",
								resultSet.getString("region"),
								resultSet.getString("mcc"),
								resultSet.getString("mnc"));
					}
					success = false;
				}
			}
		} catch (SQLException e) {
			success = false;
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.error(
					"Exception while checking redundancy for ES_LOCATION : {}",
					e.getMessage(), e);
			throw new WorkFlowExecutionException(SQL_EXCEPTION + e.getMessage(),
					e);
		} catch (Exception e) {
			LOGGER.error(" Exception {}", e.getMessage());
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException("Exception: " + e.getMessage(),
					e);
		} finally {
			try {
				ConnectionManager.closeResultSet(resultSet);
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
				if (!success) {
					updateErrorStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
		return success;
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}
}

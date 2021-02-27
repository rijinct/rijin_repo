
package com.project.rithomas.jobexecution.project;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.sdk.model.meta.RuntimeProperty;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ETLStatusDelete extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ETLStatusDelete.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		String retentionDaysString = "5";
		RuntimePropertyQuery runtimePropertyQuery = new RuntimePropertyQuery();
		RuntimeProperty runtimeProperty = runtimePropertyQuery
				.retrieve(JobExecutionContext.ETL_STATUS_DAYS_KEPT);
		if (runtimeProperty != null) {
			retentionDaysString = runtimeProperty.getParamvalue();
		}
		int retentionDays = Integer.parseInt(retentionDaysString);
		Calendar calendar = Calendar.getInstance();
		Date date = new Date();
		long dateInUnixFormat = date.getTime();
		calendar.setTimeInMillis(dateInUnixFormat);
		calendar.add(Calendar.DAY_OF_MONTH, -retentionDays);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String etlStatusDeleteQuery = QueryConstants.ETL_STATUS_DELETE.replace(
				QueryConstants.END_TIME, sdf.format(calendar.getTime()));
		Connection connection = null;
		PreparedStatement preparedStmt = null;
		try {
			GetDBResource.getInstance().retrievePropertiesFromConfig();
			connection = DBConnectionManager.getInstance()
					.getPostgresConnection(GetDBResource.getPostgreDriver(),
							GetDBResource.getPostgresUrl(),
							GetDBResource.getPostgreUserName(),
							GetDBResource.getPostgrePassword());
			preparedStmt = connection.prepareStatement(etlStatusDeleteQuery);
			int result = preparedStmt.executeUpdate();
			if (result > 0) {
				LOGGER.info(
						"Deleted {} rows in ETL STATUS which were older than {} days.",
						result, retentionDays);
			} else {
				LOGGER.info("There are no rows older than {} days.",
						retentionDays);
			}
			success = true;
			context.setProperty(JobExecutionContext.RETURN, 0);
		} catch (SQLException e) {
			LOGGER.error("Exception while executing query '{}': {}",
					etlStatusDeleteQuery, e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while executing query.", e);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Unable to load jdbc driver class {}.",
					e.getMessage());
			throw new WorkFlowExecutionException(
					"Unable to load jdbc driver class.", e);
		} catch (JobExecutionException e) {
			LOGGER.error("Exception occured when loading postgres properties", e);
			throw new WorkFlowExecutionException(
					"Exception occured when loading postgres properties", e);
		} finally {
			try {
				DBConnectionManager.getInstance().closeConnection(null,
						preparedStmt, connection);
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
		return success;
	}
}

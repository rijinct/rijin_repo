
package com.project.rithomas.jobexecution.common.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ETLStatusUpdater {

	private Connection connection;

	private PreparedStatement prepareStmt;

	private int result;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ETLStatusUpdater.class);

	public void updateStatusForAllRunningJobsToError() {
		try {
			GetDBResource.getInstance().retrievePropertiesFromConfig();
			connection = DBConnectionManager.getInstance()
					.getPostgresConnection(GetDBResource.getPostgreDriver(),
							GetDBResource.getPostgresUrl(),
							GetDBResource.getPostgreUserName(),
							GetDBResource.getPostgrePassword());
			prepareStmt = connection.prepareStatement(
					QueryConstants.ETL_STATUS_UPDATE_FOR_RUNNING_IDLE_TO_E);
			result = prepareStmt.executeUpdate();
			LOGGER.info("Updated {} number of rows in ETL STATUS from R/I to E",
					result);
		} catch (SQLException e) {
			LOGGER.error(
					"Exception while executing query {}: " + e.getMessage(),
					QueryConstants.ETL_STATUS_UPDATE_FOR_RUNNING_IDLE_TO_E);
		} catch (ClassNotFoundException e) {
			LOGGER.error(
					"Unable to load jdbc driver class {}" + e.getMessage());
		} catch (JobExecutionException e) {
			LOGGER.error("Exception occured when loading db properties", e);			
		} finally {
			try {
				DBConnectionManager.getInstance().closeConnection(null,
						prepareStmt, connection);
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
	}

	public void updateStatusForRunningJobToError(String jobName)
			throws WorkFlowExecutionException {
		try {
			GetDBResource.getInstance().retrievePropertiesFromConfig();
			connection = DBConnectionManager.getInstance()
					.getPostgresConnection(GetDBResource.getPostgreDriver(),
							GetDBResource.getPostgresUrl(),
							GetDBResource.getPostgreUserName(),
							GetDBResource.getPostgrePassword());
			prepareStmt = connection.prepareStatement(
					QueryConstants.ETL_STATUS_UPDATE_FOR_RUNNING_IDLE_TO_E_ON_ABORT);
			prepareStmt.setString(1, jobName);
			result = prepareStmt.executeUpdate();
			LOGGER.info("Updated {} number of rows in ETL STATUS from R/I to E",
					result);
		} catch (SQLException e) {
			LOGGER.error("Exception while executing query {}", e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while executing query.", e);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Unable to load jdbc driver class {}.",
					e.getMessage());
			throw new WorkFlowExecutionException(
					"Unable to load jdbc driver class.", e);
		} catch (JobExecutionException e) {
			LOGGER.error("Exception occured when loading postgres properties",
					e);
			throw new WorkFlowExecutionException(
					"Exception occured when loading postgres properties", e);
		} finally {
			try {
				DBConnectionManager.getInstance().closeConnection(null,
						prepareStmt, connection);
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
	}

	public static void main(String[] args) {
		LOGGER.info(
				"Updating ETL status to E for previously running and idle jobs.");
		ETLStatusUpdater statusUpdate = new ETLStatusUpdater();
		statusUpdate.updateStatusForAllRunningJobsToError();
	}
}

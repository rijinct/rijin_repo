
package com.project.rithomas.jobexecution.core;

import static com.project.rithomas.jobexecution.common.util.QueryConstants.DIM_VALS_TABLE_LOCATION;
import static com.project.rithomas.jobexecution.common.util.QueryConstants.ES_DIM_VALS_TABLE;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class RITHOMASCorrelationManager extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(RITHOMASCorrelationManager.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		LOGGER.info("Starting Dimension Loading");
		Connection hiveConnection = null;
		Statement hiveStatement = null;
		try {
			LOGGER.debug("Deleting the older files");
			deleteExistingFilesInHDFS();
			hiveConnection = ConnectionManager
					.getConnection(schedulerConstants.HIVE_DATABASE);
			hiveStatement = hiveConnection.createStatement();
			hiveStatement.execute("SET mapred.reduce.tasks=1");
			insertDataIntoDimVals(context, hiveStatement);
			success = true;
		} catch (SQLException e) {
			LOGGER.error(" Exception {}", e.getMessage());
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException(
					"SQLException: " + e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.error(" IOException {}", e.getMessage());
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException(
					"IOException: " + e.getMessage(), e);
		} catch (Exception e) {
			LOGGER.error(" Exception {}", e.getMessage());
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException("Exception: " + e.getMessage(),
					e);
		} finally {
			try {
				if (hiveConnection != null) {
					ConnectionManager.closeStatement(hiveStatement);
					ConnectionManager.releaseConnection(hiveConnection,
							schedulerConstants.HIVE_DATABASE);
				}
				if (!success) {
					updateErrorStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while clossing the connection {}", e);
			}
		}
		return success;
	}

	private void insertDataIntoDimVals(WorkFlowContext context,
			Statement hiveStatement)
			throws JobExecutionException, SQLException {
		LOGGER.debug("Postgres query to fetch dim val queries:{} ",
				QueryConstants.DIM_VALS_QUERY);
		QueryExecutor executor = new QueryExecutor();
		List<Object[]> resultSet = executor.executeMetadatasqlQueryMultiple(
				QueryConstants.DIM_VALS_QUERY, context);
		if (resultSet != null && !resultSet.isEmpty()) {
			Integer maxCount = 0;
			for (Object[] resultVal : resultSet) {
				ResultSet rs = loadDimValsTable(hiveStatement, maxCount,
						resultVal[1].toString());
				if (rs.next()) {
					maxCount = rs.getInt(1);
				}
				rs.close();
			}
		}
	}

	private ResultSet loadDimValsTable(Statement hiveStatement,
			Integer maxCount, String query) throws SQLException {

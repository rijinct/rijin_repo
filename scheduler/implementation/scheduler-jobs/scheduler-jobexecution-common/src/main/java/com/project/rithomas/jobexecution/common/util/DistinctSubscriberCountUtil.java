
package com.project.rithomas.jobexecution.common.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.util.UsageSpecificationGeneratorUtil;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DistinctSubscriberCountUtil {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DistinctSubscriberCountUtil.class);

	private static final boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private static final String USAGE_TABLE_QUERY = "show tables 'us*' ";

	public static List<String> getSourceTableNames(WorkFlowContext context)
			throws JobExecutionException {
		List<String> sourceTables = new ArrayList<String>();
		Connection conn = null;
		Statement statement = null;
		ResultSet result = null;
		try {
			conn = ConnectionManager
					.getConnection(schedulerConstants.HIVE_DATABASE);
			statement = conn.createStatement();
			result = statement.executeQuery(USAGE_TABLE_QUERY);
			while (result.next()) {
				if (DEBUGMODE) {
					LOGGER.debug("query out put" + result.getString(1));
				}
				sourceTables.add(result.getString(1));
			}
		} catch (SQLException e) {
			LOGGER.error("Connection Error while getting configuration. ", e);
			throw new JobExecutionException(
					"Connection error: " + e.getMessage(), e);
		} catch (Exception e) {
			LOGGER.error(" Exception {}", e.getMessage());
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new JobExecutionException("Exception: " + e.getMessage(), e);
		} finally {
			try {
				ConnectionManager.closeResultSet(result);
				ConnectionManager.closeStatement(statement);
				ConnectionManager.releaseConnection(conn,
						schedulerConstants.HIVE_DATABASE);
			} catch (Exception ex) {
				LOGGER.warn("Error while closing connection. ", ex);
			}
		}
		LOGGER.debug("SourceTables {}", sourceTables);
		return sourceTables;
	}

	public static List<String> getSourceJobIds(List<String> sourceTables) {
		List<String> sourceJobs = new ArrayList<String>();
		for (String tableName : sourceTables) {
			String sourceJob = null;
			sourceJob = UsageSpecificationGeneratorUtil.getUsageSpecJobName(
					HiveTableQueryUtil.getSpecIDVerFromTabName(tableName).get(0)
							.toUpperCase(),
					HiveTableQueryUtil.getSpecIDVerFromTabName(tableName).get(1)
							.toUpperCase());
			LOGGER.debug("sourceJobName: {}", sourceJob);
			sourceJobs.add(sourceJob);
		}
		return sourceJobs;
	}
}


package com.project.rithomas.jobexecution.common.util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class PartitionUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(PartitionUtil.class);

	private static final String GET_PARTITION_LIST = "show partitions ${TABLE_NAME}";

	public static String getPartitionColumn(WorkFlowContext context) {
		String partitionColumn = "dt";
		if (context.getProperty(
				JobExecutionContext.HIVE_PARTITION_COLUMN) != null) {
			partitionColumn = (String) context
					.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN);
		}
		return partitionColumn;
	}

	public static String getQueryToFetchPartitionsForTable(String targetTable) {
		Map<String, String> listOfPartitionsToBeArchived = new HashMap<>();
		listOfPartitionsToBeArchived.put("TABLE_NAME", targetTable);
		return StrSubstitutor.replace(GET_PARTITION_LIST,
				listOfPartitionsToBeArchived);
	}

	public static List<String> showPartitionsResult(WorkFlowContext context,
			String targetTable, boolean shouldGetOnlyFirstPartition)
			throws Exception {
		return showPartitionsResult(context, targetTable,
				shouldGetOnlyFirstPartition, null, null);
	}
	
	public static List<String> showPartitionsResult(WorkFlowContext context,
			String targetTable, boolean shouldGetOnlyFirstPartition,
			Long partitionsAfter, Long partitionsBefore) throws Exception {
		String partitionListQuery = getQueryToFetchPartitionsForTable(
				targetTable);
		Statement st = null;
		ResultSet rs = null;
		Connection con = ConnectionManager
				.getConnection(schedulerConstants.HIVE_DATABASE);
		List<String> partitionColumns = new ArrayList<>();
		try {
			st = con.createStatement();
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
					partitionListQuery);
			LOGGER.debug("Show partitions sql: {}", partitionListQuery);
			rs = st.executeQuery(partitionListQuery);
			partitionColumns = retrievePartitions(rs,
					shouldGetOnlyFirstPartition, partitionsBefore, partitionsAfter);
		} catch (SQLException e) {
			LOGGER.warn("Unable to list partitions : ", e);
		} finally {
			try {
				if (st != null) {
					st.close();
					st = null;
				}
				if (rs != null) {
					rs.close();
					rs = null;
				}
				ConnectionManager.releaseConnection(con,
						schedulerConstants.HIVE_DATABASE);
			} catch (Exception e) {
				LOGGER.warn("Error in closing PreparedStatement->", e);
			}
		}
		LOGGER.debug("result set : {}", partitionColumns);
		return partitionColumns;
	}

	private static List<String> retrievePartitions(ResultSet rs,
			boolean shouldGetOnlyFirstPartition,
			Long partitionsBefore, Long partitionsAfter)
			throws SQLException {
		List<String> partitionColumns = new ArrayList<>();
		if (shouldGetOnlyFirstPartition && rs.next()) {
			partitionColumns.add(rs.getString(1));
		} else {
			while (rs.next()) {
				if (partitionsAfter != null
						&& Long.valueOf(getPartitionValue(rs.getString(1),
								"dt")) < partitionsAfter) {
					continue;
				}
				if (partitionsBefore != null
						&& Long.valueOf(getPartitionValue(rs.getString(1),
								"dt")) >= partitionsBefore) {
					break;
				}
				partitionColumns.add(rs.getString(1));
			}
		}
		return partitionColumns;
	}

	public static String getPartitionValue(String partition,
			String partitionColumn) {
		return StringUtils.substringBetween(partition,
				new StringBuilder(partitionColumn).append("=").toString(), "/");
	}

	public static boolean isArchiveEnabled(WorkFlowContext context) {
		return NumberUtils.toInt((String) context
				.getProperty(JobExecutionContext.ARCHIVING_DAYS)) != 0;
	}

	public static void deleteParticularDirInHDFS(Path dir,
			FileSystem fileSystem, String partitionColumn, String partitionVal)
			throws IOException {
		String dirVal = dir + File.separator + partitionColumn + "="
				+ partitionVal;
		LOGGER.debug("archived directory to be deleted : {}", dirVal);
		Path paritionPath = new Path(dirVal);
		if (fileSystem.exists(paritionPath)) {
			fileSystem.delete(paritionPath, true);
}
	}
	
	public static void dropPartitionsBasedOnCondition(WorkFlowContext context,
			String tableName, long timeInMillis, String condition)
			throws Exception {
		StringBuilder dropPartitionQuery = new StringBuilder();
		dropPartitionQuery.append("ALTER TABLE ").append(tableName)
				.append(" DROP PARTITION (")
				.append(PartitionUtil.getPartitionColumn(context))
				.append(condition).append(" ?");
		String jobTypePartitionValue = getJobTypePartitionValue(context,
				dropPartitionQuery);
		dropPartitionQuery.append(")");
		executeDropPartitionQuery(context, timeInMillis,
				dropPartitionQuery.toString(), jobTypePartitionValue);
	}

	private static String getJobTypePartitionValue(WorkFlowContext context,
			StringBuilder dropPartitionQuery) {
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobTypePartitionValue = "";
		if (JobTypeDictionary.PROFILE_JOB_TYPE.equalsIgnoreCase(jobType)
				|| JobTypeDictionary.TNP_PERF_JOB_TYPE
						.equalsIgnoreCase(jobType)) {
			jobTypePartitionValue = (String) context
					.getProperty(JobExecutionContext.JOB_DESCRIPTION);
			jobTypePartitionValue = jobTypePartitionValue.replace(",", "-");
			dropPartitionQuery.append(",gp=?");
		} else if (JobTypeDictionary.TNP_THRESHOLD_JOB_TYPE
				.equalsIgnoreCase(jobType)) {
			jobTypePartitionValue = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			dropPartitionQuery.append(",src_job=?");
		}
		return jobTypePartitionValue;
	}

	private static void executeDropPartitionQuery(WorkFlowContext context,
			long timeInMillis, String sql, String jobTypePartitionValue)
			throws Exception {
		String formattedSql = String.format(sql.replace("?", "%s"),
				timeInMillis, jobTypePartitionValue);
		Connection con = null;
		PreparedStatement statement = null;
		try {
			con = DBConnectionManager.getInstance().getConnection(
					schedulerConstants.HIVE_DATABASE, (boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			statement = con.prepareStatement(sql);
			statement.setString(1, String.valueOf(timeInMillis));
			if (!jobTypePartitionValue.isEmpty()) {
				statement.setString(2, jobTypePartitionValue);
			}
			LOGGER.debug("Drop partition sql: {}", formattedSql);
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
					formattedSql);
			statement.execute();
		} catch (SQLException e) {
			LOGGER.warn("Unable to Drop partition", e);
		} finally {
			try {
				if (statement != null) {
					statement.close();
					statement = null;
				}
				ConnectionManager.releaseConnection(con,
						schedulerConstants.HIVE_DATABASE);
			} catch (Exception e) {
				LOGGER.warn("Error in closing PreparedStatement->", e);
			}
		}
	}
}

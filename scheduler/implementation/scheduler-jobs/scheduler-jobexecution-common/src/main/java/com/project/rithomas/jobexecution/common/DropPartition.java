
package com.project.rithomas.jobexecution.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DropPartition extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DropPartition.class);
	private static final String LESS_THAN ="<";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		if (shouldDropPartitions(context)) {
			int retentionDays = getRetentionDays(context);
			schedulerJobRunner jobRunner = null;
		try {
				ReaggregationListUtil reaggUtil = new ReaggregationListUtil();
				if (!reaggUtil.isAnyReaggDependentJobsActive(context,
						retentionDays)) {
					dropPartitionsFromTargetTable(context, retentionDays);
					dropPartitionsFromDqiTable(context, retentionDays);
					if (JobTypeDictionary.USAGE_JOB_TYPE
							.equalsIgnoreCase((String) context.getProperty(
									JobExecutionContext.JOBTYPE))) {
				jobRunner = schedulerJobRunnerfactory.getRunner(
						schedulerConstants.HIVE_DATABASE,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
						deleteDirectoriesForUsageJob(context, retentionDays,
								jobRunner);
				}
				}else {
					LOGGER.info(
							"Skipping drop partitions as Historic reaggregation dependent jobs are still active.. ");
				}
			success = true;
		} catch (IOException e) {
				LOGGER.error("Exception in creating hadoop fileSystem :", e);
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception in creating hadoop fileSystem ", e);
		} catch (Exception e) {
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
				LOGGER.error("Exception Query : ", e);
				throw new WorkFlowExecutionException("Excpetion: ", e);
		} finally {
			try {
					if(jobRunner != null) {
					jobRunner.closeConnection();
					}
				if (!success) {
					updateErrorStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while clossing the connection {}",
							e);
			}
		}
			context.setProperty(JobExecutionContext.IS_PARTITION_DROPPED,
					"true");
		} else {
			LOGGER.info(
					"All older partitions were dropped. Hence not dropping any more partitions in this cycle");
			success = true;
		}
		return success;
	}

	private void deleteDirectoriesForUsageJob(WorkFlowContext context,
			int retentionDays, schedulerJobRunner jobRunner) throws IOException,
			WorkFlowExecutionException, JobExecutionException {
		// If the job is usage then delete the folders in work and
		// import
		// older than configured number of days
		deleteDirectoryFromImportWork(context, jobRunner);
		cleanupAggStatus(context, retentionDays);
	}

	private void dropPartitionsFromDqiTable(WorkFlowContext context,
			int retentionDays) throws Exception {
		String diTableName = (String) context
				.getProperty(JobExecutionContext.DQI_TABLE_NAME);
		if (diTableName != null) {
			LOGGER.debug("Dropping old partitions for DQI table..");
			dropPartitions(context, retentionDays, diTableName);
		}
	}

	private void dropPartitionsFromTargetTable(WorkFlowContext context,
			int retentionDays) throws Exception {
		String tableName = ((String) context
				.getProperty(JobExecutionContext.TARGET)).toLowerCase();
		List<String> partitionsToBeDropped = null;
		boolean archiveEnabled = PartitionUtil.isArchiveEnabled(context);
		if (archiveEnabled) {
			partitionsToBeDropped = getPartitionsToBeDropped(
					context, tableName);
		}
		dropPartitions(context, retentionDays, tableName);
		if (archiveEnabled) {
			deleteArchivedPartitions(context, partitionsToBeDropped);
		}
	}

	private int getRetentionDays(WorkFlowContext context) {
		int retentionDays = 0;
		if (context.getProperty(JobExecutionContext.RETENTIONDAYS) != null) {
			retentionDays = Integer.parseInt((String) context
					.getProperty(JobExecutionContext.RETENTIONDAYS));
		}
		return retentionDays;
	}

	private boolean shouldDropPartitions(WorkFlowContext context) {
		return !Boolean.parseBoolean((String) context
				.getProperty(JobExecutionContext.IS_PARTITION_DROPPED));
	}

	private void cleanupAggStatus(WorkFlowContext context, int retention)
			throws JobExecutionException {
		LOGGER.info(
				"Cleaning up aggregation stats of usage data older than retention..");
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String reportTime = DateFunctionTransformation.getInstance()
				.getFormattedDate(DateFunctionTransformation
						.getDateBySubtractingDays(retention, context)
						.getTime());
		String sql = String.format(QueryConstants.CLEANUP_USAGE_AGG_STATUS,
				jobName, reportTime);
		QueryExecutor executor = new QueryExecutor();
		executor.executePostgresqlUpdate(sql, context);
		LOGGER.info(
				"Cleaned up aggregation stats of usage data older than retention..");
	}
	
	private List<String> getPartitionsToBeDropped(WorkFlowContext context,
			String targetTable) throws WorkFlowExecutionException {
		List<String> partitionsToBeDropped = new ArrayList<>();
		Calendar calendarPartStartDate = DateFunctionTransformation
				.getDateBySubtractingDays(NumberUtils.toInt((String) context
						.getProperty(JobExecutionContext.RETENTIONDAYS)),
						context);
		long retentionTimeInMillis = calendarPartStartDate.getTimeInMillis();
		LOGGER.info("START: Listing archived partitions to be dropped");
		Connection con = null;
		boolean shouldGetOnlyFirstPartition = false;
		try {
			con = DBConnectionManager.getInstance().getConnection(
					schedulerConstants.HIVE_DATABASE, (boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			List<String> partitionCols = PartitionUtil.showPartitionsResult(
					context, targetTable, shouldGetOnlyFirstPartition);
			for (String partitionCol : partitionCols) {
				String partitionVal = PartitionUtil.getPartitionValue(
						partitionCol,
						PartitionUtil.getPartitionColumn(context));
				LOGGER.debug("partition Value : {}", partitionVal);
				if (NumberUtils.toLong(partitionVal) < retentionTimeInMillis) {
					partitionsToBeDropped.add(partitionVal);
				}
			}
		} catch (Exception e) {
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			LOGGER.error("Exception Query {}", e.getMessage());
			throw new WorkFlowExecutionException("Exception: " + e.getMessage(),
					e);
		} finally {
			ConnectionManager.releaseConnection(con,
					schedulerConstants.HIVE_DATABASE);
		}
		LOGGER.info("END: Listing archived partitions to be dropped");
		LOGGER.info("Parititons to be Dropped based on retention days : {}",
				partitionsToBeDropped);
		return partitionsToBeDropped;
	}

	private void deleteArchivedPartitions(WorkFlowContext context,
			List<String> partitionsToBeDropped)
			throws HiveQueryException, IOException, WorkFlowExecutionException {
		LOGGER.info("Deleting the archived partitions");
		if (CollectionUtils.isNotEmpty(partitionsToBeDropped)) {
			FileSystem fileSystem = FileSystem
					.get(GetDBResource.getInstance().getHdfsConfiguration());
			Path tablePath = new Path(
					HiveTableQueryUtil.getTableLocation(context));
			for (String eachPartition : partitionsToBeDropped) {
				PartitionUtil.deleteParticularDirInHDFS(tablePath, fileSystem,
						PartitionUtil.getPartitionColumn(context),
						eachPartition);
			}
		}
		LOGGER.info("Deleted the archived partitions successfully");
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}

	@SuppressWarnings("unchecked")
	private void deleteDirectoryFromImportWork(WorkFlowContext context,
			schedulerJobRunner jobRunner)
			throws IOException, WorkFlowExecutionException {
		Configuration conf = GetDBResource.getInstance().getHdfsConfiguration();
		FileSystem fileSystem = FileSystem.get(conf);
		String sourceTableName = ((String) context.getProperty("SOURCE"))
				.toLowerCase();
		String importDir = (String) context
				.getProperty(JobExecutionContext.IMPORT_PATH);
		String workDir = importDir.replace("import", "work");
		Path importPath = new Path(importDir);
		Path workPath = new Path(workDir);
		// delete files in import
		deleteFilesInHdfs(importPath, fileSystem, context);
		// delete files in work
		deleteFilesInHdfs(workPath, fileSystem, context);
		Set<String> countFiles = (Set<String>) context
				.getProperty(JobExecutionContext.COUNT_FILES);
		if (countFiles != null && !countFiles.isEmpty()) {
			LOGGER.debug("Deleteing the count files : {}", countFiles);
			for (String countFile : countFiles) {
				fileSystem.delete(new Path(countFile), true);
			}
		}
		dropExternalTablePartitions(context, jobRunner, sourceTableName);
	}

	private int getStageRetention(String stageRetention) {
		return StringUtils.isNotEmpty(stageRetention)
				? Integer.parseInt(stageRetention) : 7;
	}

	private void deleteFilesInHdfs(Path dir, FileSystem fileSystem,
			WorkFlowContext context)
			throws IOException, WorkFlowExecutionException {
		LOGGER.debug("Checking for files in the directory : {}", dir);
		LOGGER.info("START: Deleting files from hdfs directory: {}", dir);
		int stageRetentionDays = getStageRetention((String) context
				.getProperty(JobExecutionContext.STAGE_RETENTIONDAYS));
		Calendar calendar = DateFunctionTransformation
				.getDateBySubtractingDays(stageRetentionDays, context);
		FileStatus[] files = fileSystem.listStatus(dir);
		String dirLimit = new StringBuilder(dir.toString()).append("/")
				.append((String) context
						.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN))
				.append("=").append(Long.toString(calendar.getTimeInMillis()))
				.toString();
		if (files.length != 0) {
			if (dir.toString().contains("import")) {
				LOGGER.info(
						"Stage retention days is : {}. Deleting the files older than : {} from {}",
						stageRetentionDays, calendar.getTime(), dir);
			}
			for (int cnt = 0; cnt < files.length; cnt++) {
				String fileStatus = files[cnt].getPath().toString();
				String filePath = fileStatus
						.substring(fileStatus.indexOf("/rithomas"));
				LOGGER.debug("Checking for the file: {}", filePath);
				if (dirLimit.compareToIgnoreCase(filePath) <= 0) {
					break;
				} else if (!fileSystem.delete(files[cnt].getPath(), true)) {
						throw new WorkFlowExecutionException(
								"Error in deleting directory :"
										+ files[cnt].getPath());
					}
				}
			}
		LOGGER.info("END: Deleting files from hdfs directory: {}", dir);
	}

	private void dropPartitions(WorkFlowContext context, int retentionDays,
			String tableName) throws Exception {
		LOGGER.info("START: Dropping Partitions older than {} days..",
				retentionDays);
		Calendar calendarPartStartDate = DateFunctionTransformation
				.getDateBySubtractingDays(retentionDays, context);
		long timeInMillis = calendarPartStartDate.getTimeInMillis();
		PartitionUtil.dropPartitionsBasedOnCondition(context, tableName, timeInMillis, LESS_THAN);
		LOGGER.info("END: Dropping Partitions older than {} days..",
				retentionDays);
		calendarPartStartDate.add(Calendar.DAY_OF_MONTH, -1);
		context.setProperty(JobExecutionContext.CALLED_FROM,
				JobExecutionContext.DROP_PART);
	}

	private void dropExternalTablePartitions(WorkFlowContext context,
			schedulerJobRunner jobRunner, String tableName) {
		String partitionColumn = "dt";
		String minPartition = "1";
		LOGGER.info("START: Dropping partitions for External table :{}",
				tableName);
		if (context.getProperty(
				JobExecutionContext.HIVE_PARTITION_COLUMN) != null) {
			partitionColumn = (String) context
					.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN);
		}
		String sql = String.format("ALTER TABLE %s DROP PARTITION (%s>'%s')",
				tableName, partitionColumn, minPartition);
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, sql);
			LOGGER.debug("Drop exeternal table partitions: {}", sql);
			try {
			jobRunner.runQuery(sql);
		} catch (SQLException | JobExecutionException e) {
				LOGGER.warn("Unable to Drop partition.", e);
		}
		LOGGER.info("END: Dropping partitions for External table :{}",
				tableName);
	}
}

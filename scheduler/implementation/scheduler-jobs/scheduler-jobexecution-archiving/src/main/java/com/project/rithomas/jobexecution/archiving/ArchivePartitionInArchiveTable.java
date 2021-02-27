
package com.project.rithomas.jobexecution.archiving;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.AddDBConstants;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ArchivePartitionInArchiveTable extends BaseArchivePartitioner {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ArchivePartitionInArchiveTable.class);

	DateFunctionTransformation dateTransformer = DateFunctionTransformation
			.getInstance();

	public ArchivePartitionInArchiveTable(WorkFlowContext context)
			throws WorkFlowExecutionException {
		super(context);
	}

	@Override
	public void archive(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();

		try {
			for (String region : tzRegions) {
				Long upperBound = getTimeInMillis(context,
						NumberUtils.toInt((String) context
								.getProperty(JobExecutionContext.ARCHIVING_DAYS)), region);
				Long lowerBound = calculateLB(context, region);
				Long nextLb = dateTransformer
						.getNextBound(lowerBound, archivingPlevel)
						.getTimeInMillis();
				LOGGER.info("For tz: {}, \nLB {} ,\nUB {} ,\nNB: {} ",
						region,	dateTransformer.getFormattedDate(lowerBound),
						dateTransformer.getFormattedDate(upperBound),
						dateTransformer.getFormattedDate(nextLb));
				while (isNextLBValid(upperBound, nextLb)) {
					LOGGER.debug("valid next Bound");
					copyDataFromSourceToArchiveTable(context, lowerBound,
							nextLb, region);
					archivePartition(context, lowerBound, nextLb, region);
					lowerBound = nextLb;
					nextLb = dateTransformer
							.getNextBound(nextLb, archivingPlevel)
							.getTimeInMillis();
				}
			}
		} catch (Exception e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			LOGGER.error("Exception during archiving: ", e);
			throw new WorkFlowExecutionException("Exception during archiving",
					e);
		} finally {
			closeConnection();
		}
	}

	private void copyDataFromSourceToArchiveTable(WorkFlowContext context,
			Long lowerBound, Long nextLb, String region) throws WorkFlowExecutionException {
		ReConnectUtil reConnectUtil = new ReConnectUtil();
		while (reConnectUtil.shouldRetry()) {
			try {
				if (!interrupted) {
					String targetTableName = (String) context
							.getProperty(JobExecutionContext.TARGET);
					String currentTimeHint = targetTableName
							+ System.currentTimeMillis();
					String sqlToExecute = retrieveSqlToExecute(context,
							lowerBound, nextLb, currentTimeHint, region);
					LOGGER.info("Query after replacing LB UB :{}",
							sqlToExecute);
					context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
							sqlToExecute);
					Thread logThread = applicationLogger.startApplicationIdLog(
							Thread.currentThread(),
							jobRunner.getPreparedStatement(sqlToExecute),
							(String) context
									.getProperty(JobExecutionContext.JOB_NAME),
							sleepTime);
					LOGGER.info("Executing copy query");
					jobRunner.runQuery(sqlToExecute);
					applicationLogger.stopApplicationIdLog(logThread);
				} else {
					performInterruptOperation(context);
				}
				break;
			} catch (SQLException | UndeclaredThrowableException e) {
				if (reConnectUtil.isRetryRequired(e.getMessage())) {
					reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
							context);
				} else {
					updateETLErrorStatusInTable(context, e);
				}
			} catch (Exception e) {
				updateETLErrorStatusInTable(context, e);
			}
		}
	}

	private void archivePartition(WorkFlowContext context, Long partition,
			Long nextLb, String region) throws WorkFlowExecutionException {
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String sourceTable = (String) context
				.getProperty(JobExecutionContext.SOURCE);
		String partitionColumn = PartitionUtil.getPartitionColumn(context);
		Map<String, String> defaultElemToArchivePart = getArchiveTableTemplate(
				targetTable, partitionColumn);
		try {
			LOGGER.debug("partition Value : {}", partition);
			archiveEachPartition(defaultElemToArchivePart, partition, context, region);
			dropSourceTableData(context, sourceTable, nextLb, region);
		} catch (Exception e) {
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			throw new WorkFlowExecutionException("Exception: ", e);
		}
	}

	private void dropSourceTableData(WorkFlowContext context, String tableName,
			Long nextLb, String region)
			throws JobExecutionException, WorkFlowExecutionException {
		try {
			String dropPartitionQuery = String.format(
					"ALTER TABLE %s DROP PARTITION (dt < %d, tz='%s')", tableName,
					nextLb, region);
			LOGGER.debug("Drop partition query: {}", dropPartitionQuery);
			jobRunner = schedulerJobRunnerfactory.getRunner(
					JobExecutionUtil.getExecutionEngine(context),
					(boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			jobRunner.runQuery(dropPartitionQuery);
		} catch (Exception e) {
			throw new WorkFlowExecutionException(
					"Unable to drop partition : " + e.getMessage(), e);
		}
	}

	private String retrieveSqlToExecute(WorkFlowContext context,
			Long lowerBound, Long nextLb, String currentTimeHint,
			String region) {
		String sqlToExecute = (String) context
				.getProperty(JobExecutionContext.SQL);
		String timeZoneString = AddDBConstants.TIMEZONE_PARTITION_COLUMN
										+ "='" + region + "'";
		sqlToExecute = sqlToExecute
				.replace(JobExecutionContext.LOWER_BOUND, lowerBound.toString())
				.replace(JobExecutionContext.UPPER_BOUND, nextLb.toString())
				.replace(JobExecutionContext.TIMESTAMP_HINT, currentTimeHint)
				.replace(JobExecutionContext.TIMEZONE_CHECK,
						" and " + timeZoneString)
				.replace(JobExecutionContext.TIMEZONE_PARTITION,
						" , " + timeZoneString);
		return sqlToExecute;
	}

	private Long calculateLB(WorkFlowContext context, String region) throws Exception {
		Long maxValue = getMaxDayValue(context, region);
		return (maxValue == null) ? getSourceTableLeastPartition(context, region)
				: dateTransformer.getNextBound(maxValue, archivingPlevel)
						.getTimeInMillis();
	}

	private Long getSourceTableLeastPartition(WorkFlowContext context, String region)
			throws Exception {
		boolean shouldGetOnlyFirstPartition = true;
		List<String> partitions = PartitionUtil.showPartitionsResult(context,
				(String) context.getProperty(JobExecutionContext.SOURCE),
				shouldGetOnlyFirstPartition);
		Long leastPartiton = Long.parseLong(
				PartitionUtil.getPartitionValue(partitions.toString(),
						PartitionUtil.getPartitionColumn(context)));
		return dateTransformer.getTrunc(leastPartiton, archivingPlevel, null,
				TimeZoneUtil.getZoneId(context, region));
	}

	private boolean isNextLBValid(Long ub, Long nextLb) {
		return nextLb <= ub && nextLb <= DateFunctionTransformation
				.getInstance().getSystemTime();
	}
}

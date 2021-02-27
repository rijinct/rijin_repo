
package com.project.rithomas.jobexecution.archiving;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.DeployerConstants;

public abstract class BaseArchivePartitioner {

	protected schedulerJobRunner jobRunner = null;

	protected ApplicationLoggerUtilInterface applicationLogger;

	protected boolean interrupted;

	protected String archivingPlevel;

	protected String sleepTime;
	
	protected boolean istimeZoneEnabledAndNotAgnostic;
	
	protected List<String> tzRegions = new ArrayList<>();

	private ReConnectUtil reConnectUtil = new ReConnectUtil();

	protected static final UpdateJobStatus updateJobStatus = new UpdateJobStatus();

	protected static final UpdateBoundary updateBoundary = new UpdateBoundary();

	public static final String ENABLE_ARCHIVE_HINT = "set hive.archive.enabled=true";

	public static final String DISABLE_ARCHIVE_HINT = "set hive.archive.enabled=false";

	public static final String ARCHIVE_PARTITIONS = "alter table ${TABLE_NAME} archive partition(${PARTITION_COLUMN}='${PARTITION_VALUE}', ${TZ_PARTITION}='${TZ_PARTITION_VALUE}')";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(BaseArchivePartitioner.class);

	public BaseArchivePartitioner(WorkFlowContext context)
			throws WorkFlowExecutionException {
		try {
			jobRunner = schedulerJobRunnerfactory.getRunner(
					JobExecutionUtil.getExecutionEngine(context),
					(boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			JobExecutionUtil.runQueryHints(jobRunner,
					HiveConfigurationProvider.getInstance().getQueryHints(
							(String) context
									.getProperty(JobExecutionContext.JOB_NAME),
							JobExecutionUtil.getExecutionEngine(context)));
			applicationLogger = ApplicationLoggerFactory.getApplicationLogger();
			sleepTime = (String) context.getProperty(
					JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
			archivingPlevel = getArchivingPlevel(
					(String) context.getProperty(JobExecutionContext.PLEVEL));
			istimeZoneEnabledAndNotAgnostic = TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context);
			if (istimeZoneEnabledAndNotAgnostic) {
				tzRegions = (List<String>) context
						.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			} else {
				tzRegions.add(JobExecutionContext.DEFAULT_TIMEZONE);
			}

		} catch (Exception e) {
			LOGGER.error("Exception encountered while archiving :", e);
			throw new WorkFlowExecutionException("Exception while archiving: ",
					e);
		}
	}

	protected void closeConnection() throws WorkFlowExecutionException {
		try {
			if (jobRunner != null) {
				jobRunner.closeConnection();
			}
		} catch (Exception e) {
			LOGGER.error("Exception while closing the hive connection:", e);
			throw new WorkFlowExecutionException(
					"Exception while clossing the connection: ", e);
		}
	}

	public abstract void archive(WorkFlowContext context)
			throws WorkFlowExecutionException;

	private static int getArchivingDay(WorkFlowContext context) {
		return NumberUtils.toInt((String) context
				.getProperty(JobExecutionContext.ARCHIVING_DAYS));
	}

	public Long getTimeInMillis(WorkFlowContext context, int day, String region) {
		return DateFunctionTransformation.getDateBySubtractingDays(day, context, region)
				.getTimeInMillis();
	}

	public Set<Long> getDistinctPartitions(String partitionColumn,
			List<String> partitions, String region) {
		Set<Long> partitionValList = new TreeSet<>();
		for (String partition : partitions) {
			if (partition.contains("tz=" + region)) {
				Long partitionVal = NumberUtils.toLong(PartitionUtil
						.getPartitionValue(partition, partitionColumn));
				partitionValList.add(partitionVal);
			}
		}
		LOGGER.debug("distinct partition values list : {}", partitionValList);
		return partitionValList;
	}

	protected void setReplicationFactor(String partitionColumn,
			Long partitionVal, WorkFlowContext context, String region)
			throws IOException, WorkFlowExecutionException, HiveQueryException {
		Path tablePath = new Path(HiveTableQueryUtil.getTableLocation(context));
		FileSystem fileSystem = FileSystem
				.get(GetDBResource.getInstance().getHdfsConfiguration());
		String dirVal = new StringBuilder(tablePath.toString()).append("/")
				.append(partitionColumn).append("=")
				.append(partitionVal.toString()).append("/")
				.append("tz=")
				.append(region).toString();
		LOGGER.debug("directory to change replication factor : {}", dirVal);
		Path partitionPath = new Path(dirVal);
		if (fileSystem.exists(partitionPath)) {
			RemoteIterator<LocatedFileStatus> filesList = fileSystem
					.listFiles(partitionPath, true);
			while (filesList.hasNext()) {
				fileSystem.setReplication(filesList.next().getPath(),
						Short.parseShort((String) context.getProperty(
								JobExecutionContext.ARCHIVING_REPLICATION_FACTOR)));
			}
		}
	}

	public Long getArchivingDayValue(WorkFlowContext context, String region) {
		int archivingDays = getArchivingDay(context);
		LOGGER.info("Archiving the partitions from {} days upto retention days for region: {}",
				archivingDays, region);
		Long archiveDayValue = getTimeInMillis(context, archivingDays, region);
		LOGGER.debug("Archive day : {} in milliseconds : {}", archivingDays,
				archiveDayValue);
		return archiveDayValue;
	}

	public Long getMaxDayValue(WorkFlowContext context, String region) {
		Long maxDayValue = null;
		Timestamp maxValue = getMaxValue(
				(String) context.getProperty(JobExecutionContext.JOB_NAME),
				getTz(region));
		String plevel = getArchivingPlevel(
				(String) context.getProperty(JobExecutionContext.PLEVEL));
		if (maxValue != null) {
			maxDayValue = DateFunctionTransformation.getInstance().getTrunc(
					maxValue.getTime(), plevel,
					(String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY),
					TimeZoneUtil.getZoneId(context, region));
		}
		LOGGER.info("Max day value for region {} is : {}", region, maxDayValue);
		return maxDayValue;
	}

	private Timestamp getMaxValue(String jobName, String tz) {
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		List<Boundary> boundaryList = boundaryQuery
				.retrieveByJobIdAndRegionId(jobName, tz);
		Timestamp maxvalue = null;
		if (CollectionUtils.isNotEmpty(boundaryList)) {
			maxvalue = boundaryList.get(0).getMaxValue();
		}
		return maxvalue;
	}

	private String getArchivingPlevel(String plevel) {
		String archivingInterval = plevel;
		if (JobExecutionContext.ARCHIVING_DAY_JOB_INTERVALS.contains(plevel)) {
			archivingInterval = DeployerConstants.DAY_INTERVAL;
		}
		LOGGER.info("Archiving plevel : {}", archivingInterval);
		return archivingInterval;
	}

	public Map<String, String> getArchiveTableTemplate(String targetTable,
			String partitionColumn) {
		Map<String, String> archivePartitionsMap = new HashMap<>();
		archivePartitionsMap.put("TABLE_NAME", targetTable);
		archivePartitionsMap.put("PARTITION_COLUMN", partitionColumn);
		archivePartitionsMap.put("TZ_PARTITION", "tz");
		return archivePartitionsMap;
	}

	public void archiveEachPartition(
			Map<String, String> defaultElemToArchivePart, Long partitionVal,
			WorkFlowContext context, String region) throws WorkFlowExecutionException {
		String partitionColumn = PartitionUtil.getPartitionColumn(context);
		defaultElemToArchivePart.put("PARTITION_VALUE",
				partitionVal.toString());
		defaultElemToArchivePart.put("TZ_PARTITION_VALUE", region);
		String archivePartitionQuery = StrSubstitutor
				.replace(ARCHIVE_PARTITIONS, defaultElemToArchivePart);
		LOGGER.info("Archiving the partition : {}, tz: {}", partitionVal, region);
		while (reConnectUtil.shouldRetry()) {
			try {
				if (!interrupted) {
					LOGGER.info("Executing archive query: {} ",
							archivePartitionQuery);
					Thread logThread = getLogThread(context,
							archivePartitionQuery);
					jobRunner.setQueryHint(ENABLE_ARCHIVE_HINT);
					jobRunner.runQuery(archivePartitionQuery);
					LOGGER.info("END: Archived partition");
					jobRunner.setQueryHint(DISABLE_ARCHIVE_HINT);
					applicationLogger.stopApplicationIdLog(logThread);
					setReplicationFactor(partitionColumn, partitionVal,
							context, region);
					context.setProperty(JobExecutionContext.QUERYDONEBOUND,
							partitionVal);
					LOGGER.debug("Updated boundary to {}", partitionVal);
					String tz = getTz(region);
					updateBoundary.updateBoundaryTable(context, tz);
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

	private String getTz(String region) {
		return JobExecutionContext.DEFAULT_TIMEZONE
				.equalsIgnoreCase(region) ? null : region;
	}

	protected void updateETLErrorStatusInTable(WorkFlowContext context,
			Exception e) throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				e.getMessage() == null ? e.getClass().toString()
						: e.getMessage());
		throw new WorkFlowExecutionException("Exception: ", e);
	}

	private Thread getLogThread(WorkFlowContext context,
			String archivePartitionQuery) throws SQLException {
		return applicationLogger.startApplicationIdLog(Thread.currentThread(),
				jobRunner.getPreparedStatement(archivePartitionQuery),
				(String) context.getProperty(JobExecutionContext.JOB_NAME),
				sleepTime);
	}

	public void performInterruptOperation(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.debug("The process is interrupted.");
		context.setProperty(JobExecutionContext.DESCRIPTION,
				"Job was aborted by user.");
		updateJobStatus.updateETLErrorStatusInTable(
				new Exception("Job interrupted by User"), context);
		throw new WorkFlowExecutionException(
				"InterruptException: Job was aborted by the user");
	}
}

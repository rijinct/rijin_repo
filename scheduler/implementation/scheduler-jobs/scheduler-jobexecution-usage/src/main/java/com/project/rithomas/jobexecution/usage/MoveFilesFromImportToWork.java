
package com.project.rithomas.jobexecution.usage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class MoveFilesFromImportToWork extends AbortableWorkflowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MoveFilesFromImportToWork.class);

	private Set<String> availableDirsInWork;

	private static final String QUERY_TO_ADD_PARTITION = "alter table <TABLE_NAME> add if not exists partition (<PARTITION_COL>='<DIR>')";

	private static final String QUERY_TO_ADD_PARTITION_FOR_TZ = "alter table <TABLE_NAME> add if not exists partition (<PARTITION_COL>='<DIR>',<TIME_ZONE_COL>='<TZ_DIR>')";

	private static final String TABLE_NAME_CONSTANT = "<TABLE_NAME>";

	private static final String PARTITION_COL_CONSTANT = "<PARTITION_COL>";

	private static final String DIR_CONSTANT = "<DIR>";

	private static final String TIMEZONE_COL_CONSTANT = "<TIME_ZONE_COL>";

	private static final String TZ_DIR_CONSTANT = "<TZ_DIR>";

	private String errorMess = null;

	private Set<String> countFiles;

	private Map<Long, Long> inputCountMap = new HashMap<Long, Long>();

	private Map<String, Long> timezoneInputCountMap = new HashMap<String, Long>();

	private boolean duplicateFilterEnabled = false;

	private ApplicationLoggerUtilInterface applicationLogger;

	private Connection con;
	private Configuration conf;
	private FileSystem fileSystem;
	private String timezoneColumn;
	private String externalTableName;
	private String partitionColumn;
	private boolean isDefaultTzSubPartition;
	private boolean timezoneEnabled;
	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		this.availableDirsInWork = new TreeSet<String>();
		this.countFiles = new TreeSet<String>();
		this.inputCountMap = new HashMap<Long, Long>();
		this.timezoneInputCountMap = new HashMap<String, Long>();
		applicationLogger = ApplicationLoggerFactory.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		duplicateFilterEnabled = "YES".equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.DUPLICATE_FILTER_ENABLED));
		Set<String> availableDirsInImport = (Set<String>) context
				.getProperty(JobExecutionContext.IMPORT_DIR_PATH);
		this.timezoneEnabled = TimeZoneUtil.isTimeZoneEnabled(context);
		boolean success = false;
		Statement st = null;
		LOGGER.info("Moving files from import to work");
		this.externalTableName = (String) context
				.getProperty(JobExecutionContext.SOURCE_TABLE_NAME);
		this.partitionColumn = (String) context
				.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN);
		this.isDefaultTzSubPartition = TimeZoneUtil
				.isDefaultTZSubPartition(context);
		try {
			this.conf = GetDBResource.getInstance()
					.getHdfsConfiguration();
			this.fileSystem = FileSystem.get(conf);
			this.con = ConnectionManager
					.getConnection(schedulerConstants.HIVE_DATABASE);
			this.timezoneColumn = (String) context
					.getProperty(JobExecutionContext.TIMEZONE_PARTITION_COLUMN);
			LOGGER.debug("availableDirsInImport: {}",
					availableDirsInImport.size());
			for (String availableDir : availableDirsInImport) {
				if (!interrupted) {
					// Timezone changes
					addPartitionsAndMoveFiles(context, availableDir);

				} else {
					throw new WorkFlowExecutionException(
							"Unable to continue with workflow execution as job "
									+ context.getProperty(
											JobExecutionContext.JOB_NAME)
									+ " was aborted by the user.");
				}
			}
			context.setProperty(JobExecutionContext.WORK_DIR_PATH,
					availableDirsInWork);
			context.setProperty(JobExecutionContext.COUNT_FILES, countFiles);
			context.setProperty(JobExecutionContext.INPUT_COUNT_MAP,
					inputCountMap);
			if (timezoneEnabled) {
				LOGGER.debug("Setting Map");
				for (Map.Entry<String, Long> entry : timezoneInputCountMap
						.entrySet()) {
					LOGGER.debug("getKey() : {} getValue(): {} ",
							entry.getKey(), entry.getValue());
				}
				context.setProperty(
						JobExecutionContext.TIMEZONE_INPUT_COUNT_MAP,
						timezoneInputCountMap);
			}
			success = true;
		} catch (IOException e) {
			errorMess = e.getMessage();
			LOGGER.error("Error occured in creating Hadoop filesystem : {}",
					e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception in hadoop filesystem creation", e);
		} catch (JobExecutionException e) {
			errorMess = e.getMessage();
			LOGGER.error("Error occured while renaming the files : {}",
					e.getMessage());
			throw new WorkFlowExecutionException(
					"Error occured while renaming the files :", e);
		} catch (SQLException e) {
			errorMess = e.getMessage();
			LOGGER.error("Error while trying to refresh external table: {}",
					errorMess, e);
			throw new WorkFlowExecutionException("SQLException: " + errorMess,
					e);
		} catch (Exception e) {
			errorMess = e.getMessage();
			LOGGER.error("Connection exception {}", errorMess, e);
			throw new WorkFlowExecutionException("Exception: " + errorMess, e);
		} finally {
			try {
				ConnectionManager.closeStatement(st);
				ConnectionManager.releaseConnection(con,
						schedulerConstants.HIVE_DATABASE);
				if (!success) {
					updateErrorStatus(context);
				}
			} catch (Exception e) {
				success = false;
				LOGGER.warn("Exception while clossing the connection {}",
						e.getMessage());
			}
		}
		return success;
	}

	private void addPartitionsAndMoveFiles(WorkFlowContext context, String availableDir)
			throws IOException, SQLException, JobExecutionException {
		String query;
		query = getQueryToAddPartition(context, timezoneEnabled,
				externalTableName, partitionColumn,
				isDefaultTzSubPartition, timezoneColumn,
				availableDir);
		createWorkDirPartition(fileSystem, availableDir,
				timezoneEnabled, isDefaultTzSubPartition);
		String partitionVal = availableDir.substring(
				availableDir.indexOf(partitionColumn)
						+ partitionColumn.length() + 1);
		LOGGER.info("Starting Partition creation");
		addPartitionQuery(con, query, context);
		LOGGER.info("Partition {} created successfully", partitionVal);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		LOGGER.debug("job name value : {}", jobName);
		moveFilesFromImportToWork(availableDir, conf,
				timezoneEnabled, isDefaultTzSubPartition);
		LOGGER.info("Successfully moved files from import dir to work directory");
	}
	private void addPartitionQuery(Connection con, String query,
			WorkFlowContext context) throws SQLException {
		Statement st = con.createStatement();
		context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, query);
		try {
			String sleepTime = (String) context.getProperty(
					JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC);
			String jobName = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			Thread logThread = applicationLogger.startApplicationIdLog(
					Thread.currentThread(), st, jobName, sleepTime);
			LOGGER.debug("Executing partition creation");
			st.execute(query);
			applicationLogger.stopApplicationIdLog(logThread);
		} catch (SQLException e) {
			LOGGER.warn("Partition already exists");
		}
		finally {
			if (st != null) {
				st.close();
			}
		}
	}

	private String getQueryToAddPartition(WorkFlowContext context,
			boolean timezoneEnabled, String externalTableName,
			String partitionColumn, boolean isDefaultTzSubPartition,
			String timezoneColumn, String availableDir) {
		String query;
		if (!timezoneEnabled && !(isDefaultTzSubPartition
				&& availableDir.contains("work"))) {
			if (!TimeZoneUtil.isDefaultTZSubPartition(context)) {
				query = QUERY_TO_ADD_PARTITION.replace(DIR_CONSTANT,
						availableDir
								.substring(availableDir.indexOf(partitionColumn)
										+ partitionColumn.length() + 1));
			} else {
				query = QUERY_TO_ADD_PARTITION_FOR_TZ
						.replace(DIR_CONSTANT,
								availableDir.substring(
										availableDir.indexOf(partitionColumn)
												+ partitionColumn.length() + 1))
						.replace(TZ_DIR_CONSTANT,
								JobExecutionContext.DEFAULT_TIMEZONE);
			}
		} else {
			query = QUERY_TO_ADD_PARTITION_FOR_TZ
					.replace(DIR_CONSTANT,
							availableDir.substring(
									availableDir.indexOf(partitionColumn)
											+ partitionColumn.length() + 1,
									availableDir.indexOf(timezoneColumn) - 1))
					.replace(TZ_DIR_CONSTANT,
							availableDir.substring(
									availableDir.indexOf(timezoneColumn)
											+ timezoneColumn.length() + 1));
		}
		query = query.replace(TABLE_NAME_CONSTANT, externalTableName)
				.replace(TIMEZONE_COL_CONSTANT, timezoneColumn)
				.replace(PARTITION_COL_CONSTANT, partitionColumn);
		LOGGER.debug("Query to update the partitions: {}", query);
		return query;
	}

	private void createWorkDirPartition(FileSystem fileSystem,
			String availableDir, boolean timezoneEnabled,
			boolean isDefaultTzSubPartition) throws IOException {
		String destDirectoryName = availableDir.replace("import", "work");
		Path destPath = null;
		if (!timezoneEnabled && isDefaultTzSubPartition
				&& availableDir.contains("import")) {
			destPath = new Path(destDirectoryName.concat("/tz=")
					.concat(JobExecutionContext.DEFAULT_TIMEZONE));
		} else {
			destPath = new Path(destDirectoryName);
		}
		if (!(fileSystem.exists(destPath))) {
			fileSystem.mkdirs(destPath);
		}
	}

	private boolean moveFilesFromImportToWork(String availableDir,
			Configuration conf, boolean timezoneEnabled,
			boolean isDefaultTzSubPartition)
			throws IOException, JobExecutionException {
		LOGGER.debug("timezoneInputCountMap.size(): {}",
				timezoneInputCountMap.size());
		boolean status = true;
		FileSystem fileSystem = FileSystem.get(conf);
		String srcDirectoryName = availableDir;
		String destDirectoryName = availableDir.replace("import", "work");
		if (!timezoneEnabled && isDefaultTzSubPartition
				&& availableDir.contains("import")) {
			destDirectoryName = destDirectoryName.concat("/tz=")
					.concat(JobExecutionContext.DEFAULT_TIMEZONE);
		}
		this.availableDirsInWork.add(destDirectoryName);
		LOGGER.debug(
				"Renaming files from source directory: {} to destination directory: {}",
				srcDirectoryName, destDirectoryName);
		Path srcPath = new Path(srcDirectoryName);
		Path destPath = new Path(destDirectoryName);
		if (!(fileSystem.exists(srcPath))) {
			status = false;
			throw new JobExecutionException(
					"No Such path: " + srcDirectoryName);
		}
		FileStatus files[] = fileSystem.listStatus(srcPath, new PathFilter() {

			@Override
			public boolean accept(Path path) {
				return path.toString().endsWith(".dat");
			}
		});
		LOGGER.info("Moving {} files from source directory: {} to destination directory: {}", files.length, srcDirectoryName, destDirectoryName);
		if (files.length != 0) {
			LOGGER.debug("Files Length: {}", files.length);
			for (int cnt = 0; cnt < files.length; cnt++) {
				LOGGER.debug("File Names: {}", files[cnt].getPath().toString());
				boolean isRenamed = false;
				String countFileName = null;
				if (!srcDirectoryName.equals(destDirectoryName)) {
					isRenamed = fileSystem.rename(files[cnt].getPath(),
							destPath);
				} else {
					LOGGER.debug(
							"Source and destination directories are same, hence skipping renaming..");
					isRenamed = true;
					countFileName = files[cnt].getPath().toString()
							.replace("work", "import").replace(".dat", ".cnt");
				}
				if (!isRenamed) {
					LOGGER.warn("Renaming {} to {} failed.",
							files[cnt].getPath(), destPath);
				}
				if (duplicateFilterEnabled) {
					getCountDetails(timezoneEnabled, fileSystem, files[cnt],
							countFileName);
				}
			}
		}
		return status;
	}

	private void getCountDetails(boolean timezoneEnabled, FileSystem fileSystem,
			FileStatus file, String countFileName) throws IOException {
		Long count = 0L;
		if (countFileName == null) {
			countFileName = file.getPath().toString().replace(".dat", ".cnt");
		}
		Path countFilePath = new Path(countFileName);
		LOGGER.debug("countFileName : {}", countFileName);
		if (fileSystem.exists(countFilePath)) {
			countFiles.add(countFileName);
			if (countFileName.contains("=")) {
				String partitionTime = countFileName.split("=")[1]
						.split("/")[0];
				Long partitionTimeLong = Long.parseLong(partitionTime);
				count = getCountFromFile(fileSystem, countFilePath);
				LOGGER.debug("count: {} ", count);
				// Timezone changes
				if (timezoneEnabled) {
					String regionVal = countFileName.split("=")[2]
							.split("/")[0];
					LOGGER.debug("regionVal: {} ", regionVal);
					String timezonePartVal = partitionTime + "-" + regionVal;
					LOGGER.debug("timezonePartVal: {} ", timezonePartVal);
					if (timezoneInputCountMap.get(timezonePartVal) != null) {
						count += timezoneInputCountMap.get(timezonePartVal);
					}
					timezoneInputCountMap.put(timezonePartVal, count);
					for (Map.Entry<String, Long> entry : timezoneInputCountMap
							.entrySet()) {
						LOGGER.debug("getKey() : {} getValue(): {} ",
								entry.getKey(), entry.getValue());
					}
				} else {
					if (inputCountMap.get(partitionTimeLong) != null) {
						count += inputCountMap.get(partitionTimeLong);
					}
					inputCountMap.put(partitionTimeLong, count);
				}
			}
		} else {
			LOGGER.info("Count file missing for the dat file {}",
					file.getPath());
		}
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION, errorMess);
		updateJobStatus.updateFinalJobStatus(context);
	}

	private Long getCountFromFile(FileSystem fileSystem, Path countFilePath)
			throws IOException {
		LOGGER.debug("countFilePath: {}", countFilePath);
		Long count = 0L;
		BufferedReader br = null;
		try {
			br = new BufferedReader(
					new InputStreamReader(fileSystem.open(countFilePath),
							StandardCharsets.UTF_8));
			String line = br.readLine();
			LOGGER.debug("line: {}", line);
			if (line != null && line.trim().matches("[\\d]*")) {
				count = Long.parseLong(line.trim());
				LOGGER.debug("getCountFromFile-count: {}", count);
			} else {
				LOGGER.warn("Invalid data in count file: {}", line);
			}
		} finally {
			if (br != null) {
				br.close();
			}
		}
		return count;
	}
}

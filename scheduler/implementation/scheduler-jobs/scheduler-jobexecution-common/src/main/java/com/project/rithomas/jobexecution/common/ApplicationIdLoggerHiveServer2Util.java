
package com.project.rithomas.jobexecution.common;

import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.hive.jdbc.HiveStatement;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.quartzdesk.api.agent.log.WorkerThreadLoggingInterceptorRegistry;

public class ApplicationIdLoggerHiveServer2Util
		implements ApplicationLoggerUtilInterface {

	private static final String STAGE_INFO = "Stage-Stage-";

	private static final String TOTAL_TIME_SPENT = "Completed executing command(";

	private static final String TOTAL_MAP_REDUCE_CPU_TIME_SPENT = "Total MapReduce CPU Time Spent:";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ApplicationIdLoggerHiveServer2Util.class);

	private Pattern pattern = Pattern.compile(
			"[.|\\s*]\\bjob_[0-9]*_[0-9]*[,|\\s]*\\b",
			Pattern.CASE_INSENSITIVE);

	Pattern rowCountPattern = Pattern.compile("numRows=[0-9]*",
			Pattern.CASE_INSENSITIVE);

	Pattern partitionPattern = Pattern.compile("dt=[0-9]*",
			Pattern.CASE_INSENSITIVE);

	private Pattern tzPattern = Pattern.compile("tz=[a-zA-Z0-9_\\s-]*",
			Pattern.CASE_INSENSITIVE);

	private Pattern nonAlphaNumeric = Pattern.compile("[^a-z0-9_+]*",
			Pattern.CASE_INSENSITIVE);

	private Map<String, List<String>> jobIdApplicationIdMap = new HashMap<String, List<String>>();

	private static final int DEFAULT_SLEEP_TIME_IN_SEC = 1;

	private static final int SLEEP_TIME_BEFORE_MONITROING_LOGS = 3000;

	private static final String JOB_ID_PREFIX = "job_";

	private static final String APPLICATION_ID_PREFIX = "application_";

	private static final ApplicationIdLoggerHiveServer2Util applicationIdLoggerHiveServer2Util = new ApplicationIdLoggerHiveServer2Util();

	private boolean isRowCountChecked = false;

	long rowCount = 0;

	String partition = null;

	String timezone = null;

	private Thread jobThread;

	private ApplicationIdLoggerHiveServer2Util() {
	}

	public static ApplicationLoggerUtilInterface getInstance() {
		return applicationIdLoggerHiveServer2Util;
	}

	@Override
	public Thread startApplicationIdLog(Thread parentThread, Statement stmnt,
			String jobId, String sleepTime) {
		Thread logThread = null;
		this.jobThread = parentThread;
		isRowCountChecked = false;
		rowCount = 0;
		logThread = new Thread(createLogRunnable(stmnt, jobId, sleepTime));
		logThread.setDaemon(true);
		logThread.start();
		return logThread;
	}

	public void stopApplicationIdLog(Thread logThread) {
		logThread.interrupt();
	}

	private Runnable createLogRunnable(Statement statement, final String jobId,
			final String sleepTime) {
		if (statement != null && (statement instanceof HiveStatement
				|| statement instanceof HivePreparedStatement)) {
			final HiveStatement hiveStatement = (HiveStatement) statement;
			String loadTime = DateFunctionTransformation.getInstance()
					.getFormattedDate(new Date());
			JsonObject stats = new JsonObject();
			LOGGER.debug("Started monitoring yarn logs for application id");
			return new Runnable() {

				@Override
				public void run() {
					List<String> processedJobIds = new ArrayList<String>();
					try {
						Thread.sleep(SLEEP_TIME_BEFORE_MONITROING_LOGS);
					} catch (InterruptedException e1) {
						LOGGER.debug(
								"InterruptedException occured while waiting for yarn logs.");
					}
					WorkerThreadLoggingInterceptorRegistry.INSTANCE
							.startIntercepting(jobThread);
					try {
						while (hiveStatement.hasMoreLogs()) {
							Collection<String> logList = hiveStatement
									.getQueryLog(true, 0);
							for (String log : logList) {
								if (log != null) {
									processedJobIds.add(logApplicationId(log,
											jobId, processedJobIds));
									logQueryExecutionStats(log, jobId, stats);
									logRecordCountPartitionTimezone(log);
								}
							}
							int intervalInSec = DEFAULT_SLEEP_TIME_IN_SEC;
							if (null != sleepTime) {
								try {
									intervalInSec = Integer.parseInt(sleepTime);
								} catch (Exception e) {
									// do nothing
								}
							}
							intervalInSec = intervalInSec <= 0
									? DEFAULT_SLEEP_TIME_IN_SEC : intervalInSec;
							// convert sec to millis
							LOGGER.debug("Sleep time: {}", intervalInSec);
							Thread.sleep(intervalInSec * 1000);
						}
					} catch (Exception e) {
						LOGGER.debug(
								"Exception occured when trying to get logs from hive for tracking application id of scheduler job : {}",
								jobId);
						LOGGER.debug(e.getMessage(), e);
						return;
					} finally {
						WorkerThreadLoggingInterceptorRegistry.INSTANCE
								.stopIntercepting();
						try {
								insertStatsToDB(jobId, loadTime, stats);
						} catch (JobExecutionException e) {
							LOGGER.error(
									"Exception while inserting stats into postgres {}",
									e);
						}
					}
				}

			};
		} else {
			LOGGER.debug(
					"statement is not a hivestatement hence application id cannot be fetched ignoring.");
			return new Runnable() {

				@Override
				public void run() {
					// do nothing.
				}
			};
		}
	}

	private void insertStatsToDB(final String jobId, String loadTime,
			JsonObject stats) throws JobExecutionException {
		stats.addProperty("Records Inserted", getRowCount());
		if (partition != null) {
			QueryExecutor executor = new QueryExecutor();
			String sqlToInserJobStats = QueryConstants.INSERT_JOB_STATS
					.replace(QueryConstants.LOAD_TIME, loadTime)
					.replace(QueryConstants.JOB_ID, jobId)
					.replace(QueryConstants.REPORT_TIME, partition)
					.replace(QueryConstants.TIMEZONE_PARTITION,
							timezone == null ? "'NA'" : "'" + timezone + "'")
					.replace("$APP_IDS",
							"ARRAY['" + StringUtils.join(
									jobIdApplicationIdMap.get(jobId), "','")
									+ "']")
					.replace("$STATS", stats.toString());
			JobExecutionContext context = new JobExecutionContext();
			LOGGER.debug("Inserting records into JOB_EXE_STATS table Query:{} ",
					sqlToInserJobStats);
			try {
				GetDBResource.getInstance()
						.retrievePostgresObjectProperties(context);
			} catch (JobExecutionException e) {
				LOGGER.error("Exception while retrieving postgres properties{}",
						e);
			}
			executor.executePostgresqlUpdate(sqlToInserJobStats, context);
		}
	}

	private String logApplicationId(String logLine, String schedulerJobId,
			List<String> processedJobIds) {
		String yarnJobId = "";
		LOGGER.debug("logLine : {}", logLine);
		Matcher matcher = pattern.matcher(logLine);
		if (matcher.find()) {
			yarnJobId = matcher.group();
			// replace additional space and , characters if found.
			yarnJobId = nonAlphaNumeric.matcher(yarnJobId).replaceAll("");
			if (yarnJobId != null
					&& !processedJobIds.contains(yarnJobId.trim())) {
				// replace job_ with application_ prefix
				String applicationId = yarnJobId.toLowerCase()
						.replace(JOB_ID_PREFIX, APPLICATION_ID_PREFIX);
				LOGGER.info("scheduler with job id :  " + schedulerJobId
						+ " is running on yarn with application id : "
						+ applicationId);
				List<String> applicationIdsList = jobIdApplicationIdMap
						.get(schedulerJobId);
				if (null == applicationIdsList) {
					applicationIdsList = new ArrayList<String>();
				}
				applicationIdsList.add(applicationId);
				LOGGER.debug("Application LIST is  "
						+ applicationIdsList.toString());
				this.jobIdApplicationIdMap.put(schedulerJobId,
						applicationIdsList);
				LOGGER.debug("Application id in map is "
						+ jobIdApplicationIdMap.keySet() + "Applicaton values "
						+ jobIdApplicationIdMap.values());
			}
		}
		return yarnJobId.trim();
	}

	private void logQueryExecutionStats(String logLine, String schedulerJobId,
			JsonObject stats) {
		if (logLine.contains("number of mappers:")) {
			String mapRedInfo = StringUtils.substringAfter(logLine,
					"Hadoop job information for");
			LOGGER.info("Mappers and reducers for job id :{} -->{}",
					schedulerJobId, mapRedInfo);
			populateMapReduceStats(mapRedInfo, stats);
		} else if (logLine.contains(TOTAL_MAP_REDUCE_CPU_TIME_SPENT)) {
			String totalCpuTimeSpent = StringUtils.substring(logLine,
					StringUtils.indexOf(logLine,
							TOTAL_MAP_REDUCE_CPU_TIME_SPENT));
			LOGGER.info("scheduler with job id :{} {}", schedulerJobId,
					totalCpuTimeSpent);
			populateCpuTime(totalCpuTimeSpent, stats);
		} else if (logLine.contains(STAGE_INFO)) {
			String mapRedStageInfo = StringUtils.substring(logLine,
					StringUtils.indexOf(logLine, STAGE_INFO));
			LOGGER.info("scheduler with job id :{} {}", schedulerJobId,
					mapRedStageInfo);
		} else if (logLine.contains(TOTAL_TIME_SPENT)) {
			String totalTimeSpent = StringUtils.substring(logLine,
					StringUtils.indexOf(logLine, TOTAL_TIME_SPENT));
			LOGGER.info("scheduler with job id :{} {}", schedulerJobId,
					totalTimeSpent);
			populateTotalTimeSpent(
					StringUtils.substring(totalTimeSpent,
							StringUtils.indexOf(totalTimeSpent, "Time taken")),
					stats);
		}
	}

	private void populateCpuTime(String totalCpuTimeSpent, JsonObject stats) {
		String array[] = totalCpuTimeSpent.split(":");
		stats.addProperty(array[0], array[1]);
	}

	private void populateTotalTimeSpent(String totalTimeSpent,
			JsonObject stats) {
		String array[] = totalTimeSpent.split(":");
		stats.addProperty("Total Time taken", array[1]);
	}

	private void populateMapReduceStats(String mapRedStageInfo,
			JsonObject stats) {
		insertUpdateNumberValues(stats, StringUtils.substringBetween(
				mapRedStageInfo, "number of mappers: ", ";"), "Total Mappers");
		insertUpdateNumberValues(stats, StringUtils.substringAfter(
				mapRedStageInfo, "number of reducers: "), "Total Reducers");
	}

	private void insertUpdateNumberValues(JsonObject stats, String value,
			String propertyName) {
		if (value != null) {
			JsonElement oldValue = stats.get(propertyName);
			int newValue = oldValue == null ? Integer.parseInt(value)
					: oldValue.getAsInt() + Integer.parseInt(value);
			stats.addProperty(propertyName, newValue);
		}
	}

	private void logRecordCountPartitionTimezone(String logLine) {
		String recordCount = logValue(logLine, rowCountPattern);
		if (recordCount != null) {
			setRowCountChecked(true);
			addRowCount(Long.parseLong(recordCount));
			partition = logValue(logLine, partitionPattern);
			timezone = logValue(logLine, tzPattern);
		}
	}

	private String logValue(String logLine, Pattern pattern) {
		String matchString = null;
		String value = null;
		Matcher matcher = pattern.matcher(logLine);
		if (matcher.find()) {
			matchString = matcher.group();
			if (matchString != null && !matchString.endsWith("=")) {
				value = matchString.split("=")[1];
			}
		}
		return value;
	}

	public void addRowCount(long rowCount) {
		this.rowCount += rowCount;
	}

	@Override
	public long getRowCount() {
		return rowCount;
	}

	public void setRowCountChecked(boolean isRowCountChecked) {
		this.isRowCountChecked = isRowCountChecked;
	}

	@Override
	public boolean getRowCountStatus() {
		return isRowCountChecked;
	}

	public void killApplication(Configuration conf, String jobId) {
		List<String> applicationIdsList = this.jobIdApplicationIdMap.get(jobId);
		LOGGER.debug("Inside killApplication Application id in map is "
				+ this.jobIdApplicationIdMap.keySet() + "Applicaton values "
				+ this.jobIdApplicationIdMap.values());
		if (null != applicationIdsList) {
			try (YarnClient yarnClient = YarnClient.createYarnClient()) {
				yarnClient.init(conf);
				yarnClient.start();
				Collections.reverse(applicationIdsList);
				for (String applicationId : applicationIdsList) {
					try {
						String[] applicationIdParts = applicationId.split("_");
						// job submittion count
						int id = Integer.parseInt(applicationIdParts[2]);
						// resourcemanager start time
						long clusterTimestamp = Long
								.parseLong(applicationIdParts[1]);
						ApplicationId appId = ApplicationId
								.newInstance(clusterTimestamp, id);
						LOGGER.debug("Application Ids are" + applicationId);
						yarnClient.killApplication(appId);
						jobIdApplicationIdMap.remove(jobId);
						LOGGER.info(
								"Yarn job associated with scheduler job with id : "
										+ jobId + " successfully killed");
					} catch (YarnException e) {
						LOGGER.error(
								"YarnException occured when trying to kill scheduler job with id : "
										+ jobId + " with application id : "
										+ applicationId);
					} catch (IOException e) {
						LOGGER.error(
								"IOException occured when trying to kill scheduler job with id : "
										+ jobId + " with application id : "
										+ applicationId);
					} catch (NumberFormatException e) {
						LOGGER.error(
								"NumberFormatException occured when trying to kill scheduler job with id : "
										+ jobId + " with application id : "
										+ applicationId);
					}
				}
			} catch (IOException exception) {
				LOGGER.error("Error while closing the yarn client", exception);
			}
		} else {
			LOGGER.error(
					"application id is null not able to kill application through abort.");
		}
	}
}

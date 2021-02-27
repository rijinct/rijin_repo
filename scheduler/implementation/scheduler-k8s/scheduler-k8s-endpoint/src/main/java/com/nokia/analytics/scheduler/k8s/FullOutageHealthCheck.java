
package com.rijin.analytics.scheduler.k8s;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.QueryConstants;

@Component
public class FullOutageHealthCheck implements HealthIndicator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(FullOutageHealthCheck.class);

	private static DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss");

	private static final String CONFIGURED_MINUTES_INTERVAL = "FULL_OUTAGE_HEALTH_CHECK_MINUTES_INTERVAL";

	private static final String FOLDER_MODIFICATION_MINUTES_THRESHOLD = "FOLDER_MODIFICATION_MINUTES_THRESHOLD";

	private static final String HIVE_HDFS_ERROR_DESCRIPTIONS_TO_SKIP_RESTART = "HIVE_HDFS_ERROR_DESCRIPTIONS";

	protected static volatile LocalDateTime startTime;

	protected static volatile Map<String, String> boundaryMap;

	protected static final Map<String, String> usageImportDirMap = new HashMap<>();

	protected static boolean restart;

	@Override
	public Health health() {
		try {
			checkForFullOutage();
		} catch (JobExecutionException e) {
			LOGGER.error(
					"Exception occured While reaching postgres, hence skipping Full Outage Health check:{} ",
					e);
		} catch (IOException e) {
			LOGGER.error(
					"Exception occured While reaching HDFS, hence skipping Full Outage Health check:{} ",
					e);
		} catch (Exception e) {
			LOGGER.error(
					"Exception occured, hence skipping the Full Outage Health check:{} ",
					e);
		}
		if (restart) {
			LOGGER.info(
					"scheduler engine is gracefully shutting down as all Usage jobs are not processing");
			return Health.down().withDetail(
					"scheduler engine is shutting down as all Usage jobs are not processing",
					1).build();
		}
		return Health.up().build();
	}

	private static void checkForFullOutage()
			throws IOException, JobExecutionException {
		if (startTime == null) {
			startTime = LocalDateTime.now();
			LOGGER.info("Skip Full outage health check : startup");
			LOGGER.info("Set initial time : {}", startTime.format(formatter));
			boundaryMap = fetchLatestBoundaries();
		} else if (getDiffMinutes(startTime) >= getMinutesInterval()) {
			startTime = LocalDateTime.now();
			isRestartRequired();
			LOGGER.info("Updated time: {}", startTime.format(formatter));
		} else {
			LOGGER.debug(
					"Skip Full outage health check : Interval not breached");
		}
	}

	private static Map<String, String> fetchLatestBoundaries()
			throws JobExecutionException {
		Map<String, String> latestboundaryMap = new HashMap<>();
		List boundaryRows = HealthCheckUtils
				.executeMultiQuery(QueryConstants.USAGE_JOB_BOUNDARY_QUERY);
		if (CollectionUtils.isNotEmpty(boundaryRows)) {
			constructMapFromRows(latestboundaryMap, boundaryRows);
		}
		return latestboundaryMap;
	}

	private static void constructMapFromRows(
			Map<String, String> latestboundaryMap, List boundaryRows) {
		for (Object item : boundaryRows) {
			if (item.getClass().isArray()) {
				Object[] data = (Object[]) item;
				latestboundaryMap.put(data[0].toString(), data[1].toString());
			}
		}
	}

	private static long getMinutesInterval() {
		return Integer.parseInt(System.getenv(CONFIGURED_MINUTES_INTERVAL));
	}

	private static long getDiffMinutes(LocalDateTime dateTime) {
		return Duration.between(dateTime, LocalDateTime.now()).toMinutes();
	}

	private static void isRestartRequired()
			throws IOException, JobExecutionException {
		if (!checkForBoundaryUpdate() && checkForContinuousDataPush()) {
			checkForRestart();
		}
	}

	private static void checkForRestart()
			throws IOException, JobExecutionException {
		List statusList = HealthCheckUtils
				.executeMultiQuery(QueryConstants.USAGE_STATUS_QUERY);
		if (isAllUsageJobsFailed(statusList)) {
			if (!allJobsFailedWithHiveOrHdfsExceptions(statusList)) {
				restart = true;
			}
		} else if (!isUsageProcessing()) {
			restart = true;
		}
	}

	private static boolean isAllUsageJobsFailed(List statusList) {
		boolean check = false;
		if (CollectionUtils.isNotEmpty(statusList)) {
			check = isAllJobsFailed(statusList);
		}
		if (check) {
			LOGGER.info("All usage jobs failed");
		}
		return check;
	}

	private static boolean isAllJobsFailed(List statusList) {
		boolean check = false;
		for (Object item : statusList) {
			Object[] data = (Object[]) item;
			if (StringUtils.equals("E", data[1].toString())) {
				check = true;
			} else {
				check = false;
				break;
			}
		}
		return check;
	}

	private static boolean checkForBoundaryUpdate()
			throws JobExecutionException {
		boolean isBoundaryUpdated = false;
		Map<String, String> currentBoundaryMap = fetchLatestBoundaries();
		if (boundaryMap.size() == currentBoundaryMap.size()
				&& !boundaryMap.equals(currentBoundaryMap)) {
			isBoundaryUpdated = true;
		} else if (boundaryMap.size() != currentBoundaryMap.size()) {
			isBoundaryUpdated = isAnyBoundaryUpdated(currentBoundaryMap);
		}
		if (isBoundaryUpdated) {
			LOGGER.info("Usage job boundaries are updated");
		} else {
			LOGGER.info("All usage job boundaries are not updated");
		}
		return isBoundaryUpdated;
	}

	private static boolean isAnyBoundaryUpdated(
			Map<String, String> currentBoundaryMap) {
		boolean isBoundaryUpdated = false;
		for (Entry<String, String> entry : boundaryMap.entrySet()) {
			if (!entry.getValue()
					.equals(currentBoundaryMap.get(entry.getKey()))) {
				boundaryMap = currentBoundaryMap;
				isBoundaryUpdated = true;
				break;
			}
		}
		return isBoundaryUpdated;
	}

	private static boolean checkForContinuousDataPush()
			throws IOException, JobExecutionException {
		boolean dataLoading = false;
		for (Entry<String, String> entry : boundaryMap.entrySet()) {
			String usageImportDirectory = fetchUsageImportParentDir(
					entry.getKey());
			if (isFolderUpToDate(usageImportDirectory)) {
				dataLoading = true;
				LOGGER.info("Data Push is happening for Usage:{}",
						entry.getKey());
				break;
			}
		}
		if (!dataLoading) {
			LOGGER.info("Data Push is not happening for all Usages");
		}
		return dataLoading;
	}

	private static String fetchUsageImportParentDir(String usageJobName)
			throws JobExecutionException {
		String usageImportDirectory = usageImportDirMap.get(usageJobName);
		if (StringUtils.isEmpty(usageImportDirectory)) {
			usageImportDirectory = executeQuery(QueryConstants.USAGE_IMPORT_DIR
					.replace("$TNES_TYPE", "'" + usageJobName + "'"));
			usageImportDirMap.put(usageJobName, usageImportDirectory);
		}
		return usageImportDirectory;
	}

	private static boolean allJobsFailedWithHiveOrHdfsExceptions(
			List statusList) {
		boolean check = true;
		for (Object item : statusList) {
			Object[] data = (Object[]) item;
			if (isFailedWithHiveOrHdfsException(data[2].toString())) {
				check = true;
				break;
			} else {
				check = false;
			}
		}
		if (check) {
			LOGGER.info(
					"All usage jobs failed with Hive or HDFS Errors, please check Hive or HDFS serives");
		} else {
			LOGGER.info(
					"Unhealthy: All usage job failed with no Hive or HDFS Errors");
		}
		return check;
	}

	private static boolean isFailedWithHiveOrHdfsException(
			String errorDescription) {
		return getHiveHdfsExceptionList().stream()
				.anyMatch(errorDescription::contains);
	}

	private static boolean isUsageProcessing()
			throws IOException, JobExecutionException {
		boolean isProcessing = false;
		for (Entry<String, String> entry : boundaryMap.entrySet()) {
			String usageImportDirectory = fetchUsageImportParentDir(
					entry.getKey());
			String usageWorkDirectory = usageImportDirectory.replace("import",
					"work");
			if (isFolderUpToDate(usageWorkDirectory)) {
				isProcessing = true;
				LOGGER.info("Healthy : Usage loading in progress for : {}",
						entry.getKey());
				break;
			}
		}
		if (!isProcessing) {
			LOGGER.info(" Unhealthy : None of the Usage jobs are processing ");
		}
		return isProcessing;
	}

	private static boolean isFolderUpToDate(String directory)
			throws IOException {
		return getDiffMinutes(LocalDateTime.parse(
				HealthCheckUtils.getFolderModifiedTimestamp(directory),
				formatter)) < getModificationThreshold();
	}

	private static long getModificationThreshold() {
		return Integer
				.parseInt(System.getenv(FOLDER_MODIFICATION_MINUTES_THRESHOLD));
	}

	private static String executeQuery(String query)
			throws JobExecutionException {
		Object[] resultSet = HealthCheckUtils.executeQuery(query);
		return resultSet != null ? String.valueOf(resultSet[0])
				: StringUtils.EMPTY;
	}

	private static List<String> getHiveHdfsExceptionList() {
		return Stream
				.of(System.getenv(HIVE_HDFS_ERROR_DESCRIPTIONS_TO_SKIP_RESTART)
						.split(","))
				.collect(Collectors.toList());
	}
}

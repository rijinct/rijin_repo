
package com.rijin.scheduler.jobexecution.hive.query;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ProcessUtil;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderHelper;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;


public class HiveRowCountUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveRowCountUtil.class);

	public Long getNumberOfRecordsInserted(WorkFlowContext context,
			String appender, ApplicationLoggerUtilInterface applicationLogger)
			throws IOException, InterruptedException {
		Long rowCount = 0L;
		int counter = 1;
		while (!applicationLogger.getRowCountStatus() && counter < 11) {
			LOGGER.debug("Waiting for row count from hive query log..");
			Thread.sleep(getLogReadDelay());
			counter++;
		}
		LOGGER.debug("Is K8S : {}",toBoolean(System.getenv(schedulerConstants.IS_K8S)));
		if ((!applicationLogger.getRowCountStatus()
				|| applicationLogger.getRowCount() == 0 ) &&  !toBoolean(System.getenv(schedulerConstants.IS_K8S))) {
			rowCount = getNumberOfRecords(context, appender, 0);
		} else {
			rowCount = applicationLogger.getRowCount();
		}
		return rowCount;
	}
	
	public long getLogReadDelay() {
		return 1000L;
	}

	private Long getNumberOfRecords(WorkFlowContext context, String appender,
			Integer retryCount) throws IOException, InterruptedException {
		Long numberOfRecords = 0l;
		Process process = null;
		String flag = "true";
		try {
			String schedulerBinDirectory = SDKSystemEnvironment
					.getschedulerBinDirectory();
			String hiveLogDirectory = SDKSystemEnvironment.getHiveLogDir();
			File directory = StringModificationUtil
					.getFileWithCanonicalPath(schedulerBinDirectory);
			String timestampHint = (String) context
					.getProperty(JobExecutionContext.TIMESTAMP_HINT);
			if (context.getProperty(JobExecutionContext.FLAG) != null) {
				flag = (String) context.getProperty(JobExecutionContext.FLAG);
			}
			if (appender != null) {
				timestampHint = (String) context.getProperty(
						appender + JobExecutionContext.TIMESTAMP_HINT);
			}
			if (timestampHint == null) {
				return 0L;
			}
			ProcessBuilderHelper executor = new ProcessBuilderHelper();
			List<String> command = new ArrayList<String>();
			command.add("./get-record-count-main.sh");
			command.add(hiveLogDirectory);
			command.add(timestampHint);
			command.add(flag);
			process = executor.executeCommand(command, directory);
			Integer exitValue = ProcessUtil.getExitValue(process, 120);
			Long negativeConstant = Long.valueOf("-1");
			if (exitValue == 0) {
				List<String> consoleMessages = ProcessUtil
						.getConsoleMessages(process);
				List<String> linesToBeConsidered = new ArrayList<String>();
				for (String line : consoleMessages) {
					if (line.contains("~") && !line.trim().equals("~")) {
						linesToBeConsidered.add(line);
					}
				}
				LOGGER.debug(
						"The lines to be considered for calculating the record count : {}",
						linesToBeConsidered);
				if (!linesToBeConsidered.isEmpty()) {
					String lastLineEntryToTheLogFile = linesToBeConsidered
							.get(linesToBeConsidered.size() - 1);
					if (lastLineEntryToTheLogFile.contains("~")) {
						if (lastLineEntryToTheLogFile.contains(",")) {
							String[] lastEntryToFile = lastLineEntryToTheLogFile
									.split(",");
							for (String entryToFile : lastEntryToFile) {
								if (entryToFile.contains("~")) {
									String[] values = entryToFile.split("~");
									try {
										numberOfRecords += Long
												.parseLong(values[1]);
									} catch (NumberFormatException ex) {
										LOGGER.info(
												"Unable to get the record count from hive logs.",
												ex.getMessage());
										numberOfRecords = negativeConstant;
									}
								}
							}
						} else {
							String[] values = lastLineEntryToTheLogFile
									.split("~");
							try {
								numberOfRecords = Long.parseLong(values[1]);
							} catch (NumberFormatException ex) {
								LOGGER.info(
										"Unable to get the record count from hive logs.",
										ex.getMessage());
								numberOfRecords = negativeConstant;
							}
						}
					}
				}
			} else if (retryCount < 2) {
				LOGGER.info(
						"No response from the process to get count, retrying..");
				numberOfRecords = getNumberOfRecords(context, appender,
						++retryCount);
			} else if (exitValue == -1) {
				LOGGER.info(
						"No response to get the record count from hive logs.");
				numberOfRecords = negativeConstant;
			} else {
				LOGGER.info("Unable to get the record count from hive logs.");
				numberOfRecords = negativeConstant;
				List<String> consoleErrors = ProcessUtil
						.getConsoleError(process);
				if (!consoleErrors.isEmpty()) {
					LOGGER.info("Console messages: {}", consoleErrors);
				}
			}
		} finally {
			if (process != null) {
				process.destroy();
			}
		}
		LOGGER.debug("Number of records inserted :{}", numberOfRecords);
		return numberOfRecords;
	}

}

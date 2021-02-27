
package com.project.rithomas.jobexecution.export.executors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.Arrays;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.sdk.export.base.ExportOperation;
import com.rijin.analytics.sdk.export.model.ExportConfiguration;
import com.project.rithomas.jobexecution.common.util.StringModificationUtil;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;

public class ExportFromHdfsToLocalExecutor extends ExportOperation {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ExportFromHdfsToLocalExecutor.class);

	private static final String FILE_SEP = System.getProperty("file.separator",
			"/");

	private static final String MOVE_FILES_SCRIPT = "/movefiles.sh";

	@Override
	protected void toExecute(ExportConfiguration configuration)
			throws Exception {
		LOGGER.info("Started step : ExportFromHdfsToLocalExecutor");
		createColumnHeaderFile(configuration.getLocalDirectory(),
				configuration.getColumnHeaderNames());
		String temporaryLocationLocation = configuration
				.getTemporaryLocalDirectory();
		LOGGER.debug("temporaryLocationLocation : {}",
				temporaryLocationLocation);
		File temporaryExportLocationFile = FileSystems.getDefault()
				.getPath(temporaryLocationLocation).normalize().toFile();
		createTemporaryLocIfNotExist(temporaryLocationLocation,
				temporaryExportLocationFile);
		String scriptPath = SDKSystemEnvironment.getschedulerBinDirectory()
				+ MOVE_FILES_SCRIPT;
		LOGGER.debug("movefiles scriptPath : {}", scriptPath);
		String[] commandForTemp = { "/bin/bash", scriptPath,
				configuration.getHdfsExportDirectory(),
				temporaryLocationLocation, configuration.getFileName(),
				configuration.getFileExtension(),
				configuration.getColumnHeaderNames(),
				String.valueOf(configuration.shouldMergeFiles()) };
		triggerCopyFiles(commandForTemp);
		LOGGER.info("Ended step : ExportFromHdfsToLocalExecutor");
	}

	@Override
	protected boolean canExecute(ExportConfiguration configuration) {
		return true;
	}

	public void createColumnHeaderFile(String exportLocation,
			String columnNames) {
		LOGGER.debug(
				"creating header file with exportLocation : {}, columnNames : {}",
				exportLocation, columnNames);
		String headerFilePath = addFileSeparator(exportLocation)
				+ ".columnHeaders";
		LOGGER.debug("headerFilePath : {}", headerFilePath);
		File file = StringModificationUtil
				.getFileWithCanonicalPath(headerFilePath);
		if (!file.exists()) {
			try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(file), StandardCharsets.UTF_8))) {
				if (file.createNewFile()) {
					pw.println(columnNames);
				}
			} catch (IOException e) {
				LOGGER.error(" IOException when creating header file ", e);
			}
		}
	}

	private void createTemporaryLocIfNotExist(String temporaryExportLocation,
			File temporaryExportLocationFile) throws IOException {
		LOGGER.debug("temporaryExportLocationFile.exists()? : {}",
				temporaryExportLocationFile.exists());
		if (!temporaryExportLocationFile.exists()) {
			boolean successful = temporaryExportLocationFile.mkdir();
			LOGGER.debug("temp file creation successful? :{}", successful);
			if (successful) {
				LOGGER.debug("Temporary export location is :: {} ",
						temporaryExportLocation);
			} else {
				LOGGER.error("Unable to create temp export location {}",
						temporaryExportLocation);
				throw new IOException("Failed to create temp export location: "
						+ temporaryExportLocation);
			}
		}
	}

	private void triggerCopyFiles(String[] commandForTemp) {
		LOGGER.debug("triggering copy file with details : {}",
				Arrays.toString(commandForTemp));
		Process process = null;
		BufferedReader br = null;
		ProcessBuilder processBuilder = new ProcessBuilder(commandForTemp);
		try {
			process = processBuilder.start();
			br = new BufferedReader(new InputStreamReader(
					process.getInputStream(), StandardCharsets.UTF_8));
			String line;
			while ((line = br.readLine()) != null) {
				LOGGER.debug(line);
			}
		} catch (IOException e) {
			LOGGER.error("Exception during renaming and moving files ", e);
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException e) {
				LOGGER.error("Exception while closing connection ", e);
			}
			if (process != null) {
				process.destroy();
			}
		}
	}

	private String addFileSeparator(String tempExportPath) {
		String newTempExpPath = tempExportPath;
		if (!tempExportPath.endsWith(FILE_SEP)) {
			newTempExpPath = tempExportPath + FILE_SEP;
		}
		return newTempExpPath;
	}
}

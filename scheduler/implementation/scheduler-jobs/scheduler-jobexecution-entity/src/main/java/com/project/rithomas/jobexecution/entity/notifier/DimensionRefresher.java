
package com.project.rithomas.jobexecution.entity.notifier;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.entity.client.ETLRestApiClient;
import com.project.rithomas.jobexecution.common.util.ProcessUtil;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DimensionRefresher {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DimensionRefresher.class);

	private final static String TRIGGER_DIM_REFRESH_SCRIPT = "trigger-dim-refresh.sh";

	private final static String ETL_SCRIPTS_DIR = "/etl/app/bin/";

	private static final String RITHOMAS_HOME_KEY = "RITHOMAS_HOME";

	public void notifyTopicsToRefreshCache(String dimensionName)
			throws InterruptedException, IOException {
		String rithomasHomeEnv = System.getenv(RITHOMAS_HOME_KEY);
		String scriptPath = rithomasHomeEnv + ETL_SCRIPTS_DIR
				+ TRIGGER_DIM_REFRESH_SCRIPT;
		File file = FileSystems.getDefault().getPath(scriptPath).normalize()
				.toFile();
		if (file.exists()) {
			notifyUsingScript(scriptPath, dimensionName);
		} else if (isKubernetesEnvironment()) {
			ETLRestApiClient apiClient = new ETLRestApiClient(dimensionName);
			apiClient.execute();
			LOGGER.info("ETL Dimension refresh complete");
		} else {
			LOGGER.debug("Dimension refresh script not found. Hence skipping.");
		}
	}

	private void notifyUsingScript(String scriptPath, String dimensionName)
			throws InterruptedException, IOException {
		Process process = null;
		try {
			process = new ProcessBuilder(scriptPath, dimensionName).start();
			Integer exitValue = ProcessUtil.getExitValue(process);
			if (exitValue != 0) {
				LOGGER.error("Console error: {}",
						ProcessUtil.getConsoleError(process));
				LOGGER.info("Console messages: {}",
						ProcessUtil.getConsoleMessages(process));
			} else {
				LOGGER.info(
						"Notified for refreshing cache dimensions. Console messages: {}",
						ProcessUtil.getConsoleMessages(process));
			}
		} finally {
			if (process != null) {
				process.destroy();
			}
		}
	}

	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}

	
}

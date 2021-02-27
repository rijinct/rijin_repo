
package com.project.rithomas.jobexecution.common;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;

public class schedulerSparkLauncher {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(schedulerSparkLauncher.class);

	public boolean launchSparkSubmitJob(List<String> args, String resultDirName)
			throws JobExecutionException {
		SparkAppHandle handle;
		String jobStatus;
		String sparkHome = System.getenv(JobExecutionContext.SPARK_HOME);
		if (sparkHome == null) {
			sparkHome = System.getProperty(JobExecutionContext.SPARK_HOME);
		}
		try {
			SparkLauncher launcher = new SparkLauncher();
			setConfWithQueryHints(args, launcher);
			setAllRequiredJars(launcher);
			String[] arguments = args.toArray(new String[0]);
			String resourceString = schedulerSparkLauncher.class
					.getProtectionDomain().getCodeSource().getLocation()
					.toString();
			LOGGER.debug("Classpath of schedulerSparkLauncher: {}",
					resourceString);
			handle = launcher
					.setAppResource(resourceString.substring(
							resourceString.indexOf(":") + 1,
							resourceString.length()))
					.setMainClass(
							"com.project.rithomas.jobexecution.common.SparkMainClass")
					.setMaster("yarn").setDeployMode("cluster")
					.setSparkHome(sparkHome).addAppArgs(arguments)
					.setConf(JobExecutionContext.SPARK_RESULT_DIR,
							resultDirName == null ? "" : resultDirName)
					.startApplication();
			while (!handle.getState().isFinal()) {
				Thread.sleep(1000L);
			}
			jobStatus = handle.getState().toString();
			LOGGER.debug("Status of job launched in Spark: {} ", jobStatus);
		} catch (IOException e) {
			throw new JobExecutionException(
					"Exception while starting spark session", e);
		} catch (InterruptedException e) {
			throw new JobExecutionException("Exception while running spark job",
					e);
		}
		return "FAILED".equals(jobStatus) ? false : true;
	}

	private void setAllRequiredJars(SparkLauncher launcher) {
		File directory = new File("/etc/hive/lib");
		File[] fList = directory.listFiles();
		if (fList != null) {
			for (File file : fList) {
				launcher.addJar(file.getAbsolutePath());
			}
		}
	}

	private void setConfWithQueryHints(List<String> args,
			SparkLauncher launcher) {
		for (String hint : args) {
			if (StringUtils.containsIgnoreCase(hint, "set spark")) {
				String[] hintString = hint.split("=");
				String key = StringUtils
						.removeStartIgnoreCase(hintString[0], "set").trim();
				String value = hintString[1];
				launcher.setConf(key, value);
			}
		}
	}
}

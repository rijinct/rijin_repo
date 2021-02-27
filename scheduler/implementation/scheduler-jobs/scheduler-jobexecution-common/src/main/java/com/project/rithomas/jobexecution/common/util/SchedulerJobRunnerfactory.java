package com.project.rithomas.jobexecution.common.util;

import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class schedulerJobRunnerfactory {

	public static schedulerJobRunner getRunner(String executionEngine,
			boolean shouldConnectToCustomDbUrl) throws Exception {
		schedulerJobRunner runner = null;
		if (schedulerConstants.HIVE_DATABASE.equals(executionEngine)) {
			runner = new HiveJobRunner(shouldConnectToCustomDbUrl);
		} else if (schedulerConstants.SPARK_DATABASE.equals(executionEngine)) {
			runner = new SparkJobRunner();
		}
		return runner;
	}
}
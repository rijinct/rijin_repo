package com.rijin.scheduler.jobexecution.hive;

import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class DbConfiguratorFactory {

	public DbConfigurator getDbConfigurator(String hiveDBURL,
			String executionEngine) {
		DbConfigurator dbConfigurator = null;
		if (schedulerConstants.SPARK_DATABASE
				.equalsIgnoreCase(executionEngine)) {
			dbConfigurator = new SparkConfigurator();
		} else if (hiveDBURL.contains("hive2")) {
			dbConfigurator = new CA4CIHiveHintsConfiguration();
		}
		return dbConfigurator;
	}
}
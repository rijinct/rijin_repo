
package com.rijin.scheduler.jobexecution.hive;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.IOException;

import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class HiveConfigurationFactory {

	private HiveConfigurationFactory() {
	}

	public static HiveConfiguration getConfiguration() throws IOException {
		if (isKubernetesEnvironment()) {
			return new CDLKHiveConfiguration();
		}
		return new CA4CIHiveConfiguration();
	}
	
	public static HiveConfiguration getSparkConfiguration() throws IOException {
		
		if (isKubernetesEnvironment()) {
			return new CDLKHiveConfiguration();
		}
		return new CA4CISparkConfiguration();
	}

	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}
}

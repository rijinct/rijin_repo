
package com.nokia.monitoring.spark;

import java.util.ArrayList;

import com.nokia.analytics.logging.AnalyticsLogger;
import com.nokia.analytics.logging.AnalyticsLoggerFactory;
import com.nsn.cem.sai.jdbc.ConnectionManager;
import com.nsn.cem.sai.jdbc.PoolConfig;

public class SparkConnectionInitialiser {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SparkConnectionInitialiser.class);

	private static final String HIVE_DATABASE = "HIVE";

	public static boolean initialiseConnection(String sparkThriftUrl) {
		boolean isSuccessful = false;
		try {
			LOGGER.info("Initialise Connection");
			SparkConfigurationProvider sparkConfigurationProvider = new SparkConfigurationProvider(
					new CDLKSparkConfiguration(sparkThriftUrl));
			PoolConfig config = sparkConfigurationProvider
					.getPoolConfiguration();
			ConnectionManager.configureConnPool(config, new ArrayList<String>(),
					HIVE_DATABASE);
			isSuccessful = true;
		} catch (Exception ex) {
			LOGGER.error("Exception while configuring poolconfig", ex);
		}
		return isSuccessful;
	}
}


package com.nokia.monitoring.hive;

import java.util.ArrayList;

import com.nokia.analytics.connector.hive.client.HiveConfigurationProvider;
import com.nokia.analytics.logging.AnalyticsLogger;
import com.nokia.analytics.logging.AnalyticsLoggerFactory;
import com.nsn.cem.sai.jdbc.ConnectionManager;
import com.nsn.cem.sai.jdbc.PoolConfig;

public class HiveConnectionInitialiser {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveConnectionInitialiser.class);

	private static final String HIVE_DATABASE = "HIVE";
	
	public static boolean initialiseConnection() {
		boolean isSuccessful = false;
		try {
			LOGGER.info("  initialiseConnection");
			HiveConfigurationProvider hiveConfigurationProvider = new HiveConfigurationProvider(
					HiveConfigurationFactory.getInstance().getConfiguration());
			PoolConfig config = hiveConfigurationProvider
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

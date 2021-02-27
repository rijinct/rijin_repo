
package com.rijin.analytics.scheduler.k8s;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.stereotype.Component;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.cem.sai.jdbc.PoolConfig;

@Component
public class PoolConfigInitializer {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(PoolConfigInitializer.class);

	public void initializePool() throws Exception {
		try {
			LOGGER.debug("Initializing pool");
			Map<String, PoolConfig> poolConfigMap = HiveConfigurationProvider
					.getInstance().getPoolConfig();
			for (Entry<String, PoolConfig> config : poolConfigMap.entrySet()) {
				LOGGER.debug("PoolConfig : {}", config);
				ConnectionManager
						.configureConnPool(config.getValue(),
								HiveConfigurationProvider.getInstance()
										.getQueryHints(null, config.getKey()),
								config.getKey());
				LOGGER.debug("Pool initialized successfully");
			}
		} catch (Exception exception) {
			LOGGER.error("Exception while configuring poolconfig", exception);
			throw exception;
		}
	}
}
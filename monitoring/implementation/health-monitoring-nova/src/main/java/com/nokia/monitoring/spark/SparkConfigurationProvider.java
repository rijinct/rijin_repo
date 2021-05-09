
package com.nokia.monitoring.spark;

import java.io.IOException;

import com.nokia.analytics.logging.AnalyticsLogger;
import com.nokia.analytics.logging.AnalyticsLoggerFactory;
import com.nsn.cem.sai.jdbc.PoolConfig;

public class SparkConfigurationProvider {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SparkConfigurationProvider.class);

	private SparkConfiguration sparkConfiguration;

	public SparkConfigurationProvider(SparkConfiguration sparkConfiguration) {
		this.sparkConfiguration = sparkConfiguration;
	}

	public PoolConfig getPoolConfiguration() throws IOException {
		LOGGER.info("Initializing the pool configuration");
		PoolConfig poolConfig = new PoolConfig();
		poolConfig.setDbDriver(this.sparkConfiguration.getDbDriver());
		poolConfig.setDbUrl(this.sparkConfiguration.getDbUrl());
		poolConfig.setUserName(this.sparkConfiguration.getHiveUserName());
		poolConfig.setEnableConnectionPool(
				this.sparkConfiguration.isConnectionPoolingEnabled());
		poolConfig
				.setMinConnections(this.sparkConfiguration.getMinConnections());
		poolConfig
				.setMaxConnections(this.sparkConfiguration.getMaxConnection());
		poolConfig.setIdleConnTestInMin(
				this.sparkConfiguration.getIdleConnectionTestPeriodInMins());
		poolConfig.setIdleConnTestSql(
				this.sparkConfiguration.getIdleConnectionTestSQL());
		poolConfig.setTestConnection(
				this.sparkConfiguration.isTestConnectionsFlagEnabled());
		poolConfig.setSchema(this.sparkConfiguration.getSchema());
		LOGGER.info("Initialized the pool configuration");
		return poolConfig;
	}
}

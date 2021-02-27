
package com.rijin.scheduler.jobexecution.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.PoolConfig;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class HiveConfigurationProvider {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveConfigurationProvider.class);

	private static final HiveConfigurationProvider INSTANCE = new HiveConfigurationProvider();

	private HiveConfiguration hiveConfiguration;

	private HiveConfiguration sparkConfiguration;

	private volatile Map<String, HiveConfiguration> dbPropertiesMap;

	public static HiveConfigurationProvider getInstance() {
		return INSTANCE;
	}

	private HiveConfigurationProvider() {
	}

	private void initializeHiveConfiguration() throws IOException {
		if (hiveConfiguration == null) {
			hiveConfiguration = HiveConfigurationFactory.getConfiguration();
		}
		if (sparkConfiguration == null) {
			sparkConfiguration = HiveConfigurationFactory
					.getSparkConfiguration();
		}
		if (dbPropertiesMap == null) {
			dbPropertiesMap = new HashMap<String, HiveConfiguration>();
			dbPropertiesMap.put("HIVE", hiveConfiguration);
			dbPropertiesMap.put("SPARK", sparkConfiguration);
		}
	}

	public Map<String, PoolConfig> getPoolConfig() throws IOException {
		initializeHiveConfiguration();
		Map<String, PoolConfig> poolConfigDBMap = new HashMap<String, PoolConfig>();
		for (Entry<String, HiveConfiguration> entry : dbPropertiesMap
				.entrySet()) {
			HiveConfiguration configuration = entry.getValue();
			PoolConfig poolConfig = new PoolConfig();
			poolConfig.setDbDriver(configuration.getDbDriver());
			poolConfig.setDbUrl(configuration.getDbUrl());
			poolConfig.setCustomDBUrl(configuration.getCustomDBUrl());
			poolConfig.setUserName(configuration.getHiveUserName());
			poolConfig.setEnableConnectionPool(
					configuration.isConnectionPoolingEnabled());
			poolConfig.setMinConnections(configuration.getMinConnections());
			poolConfig.setMaxConnections(configuration.getMaxConnection());
			poolConfig.setIdleConnTestInMin(
					configuration.getIdleConnectionTestPeriodInMins());
			poolConfig.setIdleConnTestSql(
					configuration.getIdleConnectionTestSQL());
			poolConfig.setTestConnection(
					configuration.isTestConnectionsFlagEnabled());
			poolConfig.setSchema(configuration.getDbSchema());
			poolConfigDBMap.put(entry.getKey(), poolConfig);
		}
		return poolConfigDBMap;
	}

	public List<String> getQueryHints(String jobId, String dataBase)
			throws WorkFlowExecutionException {
		try {
			return getDbConfigurator(dataBase).getQueryHints(jobId, dataBase);
		} catch (Exception exception) {
			LOGGER.error("Exception encountered while parsing hive query hints",
					exception);
			throw new WorkFlowExecutionException(
					"Exception encountered while parsing hive query hints ",
					exception);
		}
	}

	public boolean shouldConnectToCustomDbUrl(String jobId, String dataBase)
			throws WorkFlowExecutionException {
		boolean isCustomUrl = false;
		try {
			isCustomUrl = getDbConfigurator(dataBase)
					.shouldConnectToCustomDbUrl(jobId, dataBase);
		} catch (Exception exception) {
			LOGGER.error(
					"Exception encountered while parsing hive query hints: "
							+ exception);
			throw new WorkFlowExecutionException(
					"Exception encountered while parsing hive query hints:  "
							+ exception.getMessage(),
					exception);
		}
		LOGGER.info("Should connect to custom db url for job: {} ? : {}", jobId,
				isCustomUrl);
		return isCustomUrl;
	}

	private DbConfigurator getDbConfigurator(String dataBase) {
		DbConfiguratorFactory dbConfiguratorFactory = new DbConfiguratorFactory();
		DbConfigurator configurator = dbConfiguratorFactory.getDbConfigurator(
				dbPropertiesMap.get(dataBase).getDbUrl(), dataBase);
		return configurator;
	}

	public HiveConfiguration getConfiguration() {
		return hiveConfiguration;
	}
}

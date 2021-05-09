
package com.nokia.monitoring.spark;

import org.apache.commons.lang.BooleanUtils;

public class CDLKSparkConfiguration implements SparkConfiguration {

	private String sparkThriftJdbcUrl;

	public CDLKSparkConfiguration(String sparkUrl) {
		this.sparkThriftJdbcUrl = sparkUrl;
	}

	@Override
	public String getDbDriver() {
		return System.getenv("db_driver");
	}

	@Override
	public String getDbUrl() {
		return System.getenv(sparkThriftJdbcUrl);
	}

	@Override
	public String getHiveUserName() {
		return System.getenv("hive_username");
	}

	@Override
	public int getIdleConnectionTestPeriodInMins() {
		return Integer.parseInt(
				System.getenv("IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES"));
	}

	@Override
	public String getIdleConnectionTestSQL() {
		return System.getenv("IDLE_CONNECTION_TEST_SQL");
	}

	@Override
	public int getMaxConnection() {
		return Integer.parseInt(System.getenv("max_connections"));
	}

	@Override
	public int getMinConnections() {
		return Integer.parseInt(System.getenv("min_connections"));
	}

	@Override
	public Boolean isConnectionPoolingEnabled() {
		return Boolean.valueOf(BooleanUtils
				.toBoolean(System.getenv("connection_pooling_enabled")));
	}

	@Override
	public boolean isTestConnectionsFlagEnabled() {
		return false;
	}

	@Override
	public String getSchema() {
		return System.getenv("DB_SCHEMA");
	}
}

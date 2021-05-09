
package com.nokia.monitoring.spark;

public interface SparkConfiguration {

	public static final String HIVE_CONNECTION_POOLING_ENABLED = "connection_pooling_enabled";

	public static final String HIVE_DB_DRIVER = "db_driver";

	public static final String HIVE_DB_URL = "db_url";

	public static final String HIVE_IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES = "IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES";

	public static final String HIVE_IDLE_CONNECTION_TEST_SQL = "IDLE_CONNECTION_TEST_SQL";

	public static final String HIVE_MAX_CONNECTIONS = "max_connections";

	public static final String HIVE_MIN_CONNECTIONS = "min_connections";

	public static final String HIVE_USERNAME = "hive_username";

	public static final String IS_K8S = "IS_K8S";

	public static final String DB_SCHEMA = "DB_SCHEMA";

	public static final String HIVE_TEST_CONNECTIONS_FLAG = "test_connection";

	String getDbDriver();

	String getDbUrl();

	String getHiveUserName();

	int getIdleConnectionTestPeriodInMins();

	String getIdleConnectionTestSQL();

	int getMaxConnection();

	int getMinConnections();

	Boolean isConnectionPoolingEnabled();

	boolean isTestConnectionsFlagEnabled();

	String getSchema();
}


package com.rijin.scheduler.jobexecution.hive;

public interface HiveConfiguration {

	String HIVE_CONNECTION_POOLING_ENABLED = "connection_pooling_enabled";

	String HIVE_DB_DRIVER = "db_driver";

	String HIVE_DB_URL = "db_url";

	String HIVE_IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES = "IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES";

	String HIVE_IDLE_CONNECTION_TEST_SQL = "IDLE_CONNECTION_TEST_SQL";

	String HIVE_MAX_CONNECTIONS = "max_connections";

	String HIVE_MIN_CONNECTIONS = "min_connections";

	String HIVE_USERNAME = "hive_username";
	
	String KERBEROS_PRINCIPAL = "PRINCIPAL";
	
	String KERBEROS_KEYTAB_LOCATION = "KEYTAB_LOCATION";
	
	String CUSTOM_DB_URL = "custom_db_url";

	String getDbDriver();

	String getDbUrl();

	String getHiveUserName();

	int getIdleConnectionTestPeriodInMins();

	String getIdleConnectionTestSQL();

	int getMaxConnection();

	int getMinConnections();

	Boolean isConnectionPoolingEnabled();

	boolean isTestConnectionsFlagEnabled();
	
	String getKerberosPrincipal();
	
	String getKeytabLocation();
	
	String getCustomDBUrl();
	
	String getDbSchema();
}

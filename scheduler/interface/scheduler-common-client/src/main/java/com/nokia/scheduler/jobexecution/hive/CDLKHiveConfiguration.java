
package com.rijin.scheduler.jobexecution.hive;

import static org.apache.commons.lang.BooleanUtils.toBoolean;

import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class CDLKHiveConfiguration implements HiveConfiguration {

	public String getDbDriver() {
		return System.getenv(HIVE_DB_DRIVER);
	}

	public String getDbUrl() {
		return System.getenv(schedulerConstants.HIVE_JDBC_URL);
	}

	public String getHiveUserName() {
		return System.getenv(HIVE_USERNAME);
	}

	public int getIdleConnectionTestPeriodInMins() {
		return Integer.parseInt(
				System.getenv(HIVE_IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES));
	}

	public String getIdleConnectionTestSQL() {
		return System.getenv(HIVE_IDLE_CONNECTION_TEST_SQL);
	}

	public int getMaxConnection() {
		return Integer.parseInt(System.getenv(HIVE_MAX_CONNECTIONS));
	}

	public int getMinConnections() {
		return Integer.parseInt(System.getenv(HIVE_MIN_CONNECTIONS));
	}

	public Boolean isConnectionPoolingEnabled() {
		return toBoolean(System.getenv(HIVE_CONNECTION_POOLING_ENABLED));
	}

	public boolean isTestConnectionsFlagEnabled() {
		return false;
	}

	public String getKerberosPrincipal() {
		return System.getenv(KERBEROS_PRINCIPAL);
	}

	public String getKeytabLocation() {
		return System.getenv(KERBEROS_KEYTAB_LOCATION);
	}

	public String getCustomDBUrl() {
		return System.getenv(CUSTOM_DB_URL);
	}

	@Override
	public String getDbSchema() {
		return System.getenv(schedulerConstants.DB_SCHEMA);
	}
}

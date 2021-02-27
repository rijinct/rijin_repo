
package com.rijin.scheduler.jobexecution.hive;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang.BooleanUtils.toBoolean;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

public class CA4CIHiveConfiguration implements HiveConfiguration {

	private static final String HIVE_CONFIG_FILE = "hive_config.properties";

	private static final String HIVE_TEST_CONNECTIONS_FLAG = "test_connection";

	private static final String RITHOMAS_HOME = "RITHOMAS_HOME";

	private Properties hiveProperties;

	public CA4CIHiveConfiguration() throws IOException {
		this.hiveProperties = new Properties();
		InputStream inputStream = Files.newInputStream(FileUtils
				.getFile(getschedulerConfDirectory() + HIVE_CONFIG_FILE)
				.toPath());
		hiveProperties.load(inputStream);
		inputStream.close();
	}

	public String getschedulerConfDirectory() {
		return System.getenv(RITHOMAS_HOME) + File.separator + "scheduler"
				+ File.separator + "app" + File.separator + "conf"
				+ File.separator;
	}

	public String getDbDriver() {
		return hiveProperties.getProperty(HIVE_DB_DRIVER);
	}

	public String getDbUrl() {
		return hiveProperties.getProperty(HIVE_DB_URL);
	}

	public String getHiveUserName() {
		return hiveProperties.getProperty(HIVE_USERNAME);
	}

	public int getIdleConnectionTestPeriodInMins() {
		return parseInt(hiveProperties
				.getProperty(HIVE_IDLE_CONNECTION_TEST_PERIOD_IN_MINUTES));
	}

	public String getIdleConnectionTestSQL() {
		return hiveProperties.getProperty(HIVE_IDLE_CONNECTION_TEST_SQL);
	}

	public int getMaxConnection() {
		return parseInt(hiveProperties.getProperty(HIVE_MAX_CONNECTIONS));
	}

	public int getMinConnections() {
		return parseInt(hiveProperties.getProperty(HIVE_MIN_CONNECTIONS));
	}

	public Boolean isConnectionPoolingEnabled() {
		return toBoolean(
				hiveProperties.getProperty(HIVE_CONNECTION_POOLING_ENABLED));
	}

	public boolean isTestConnectionsFlagEnabled() {
		return toBoolean(
				hiveProperties.getProperty(HIVE_TEST_CONNECTIONS_FLAG));
	}

	public String getKerberosPrincipal() {
		return hiveProperties.getProperty(KERBEROS_PRINCIPAL);
	}

	public String getKeytabLocation() {
		return hiveProperties.getProperty(KERBEROS_KEYTAB_LOCATION);
	}

	@Override
	public String getCustomDBUrl() {
		return hiveProperties.getProperty(CUSTOM_DB_URL);
	}

	@Override
	public String getDbSchema() {
		return null;
	}
}

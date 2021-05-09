
package com.nokia.monitoring.hive;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.sql.Connection;
import java.sql.SQLException;

import com.nokia.analytics.logging.AnalyticsLogger;
import com.nokia.analytics.logging.AnalyticsLoggerFactory;
import com.nsn.cem.sai.jdbc.ConnectionManager;

public class HiveCommandExecutor {

	private static final String HIVE_DATABASE = "HIVE";

	private static final String IS_K8S = "IS_K8S";

	private static final String USER_PRINCIPAL = "USER_PRINCIPAL";

	private static final String KEYTAB_LOCATION = "KEYTAB_LOCATION";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveCommandExecutor.class);

	public static Connection getConnection() throws Exception {
		if (toBoolean(System.getenv(IS_K8S))) {
			return ConnectionManager.getConnection(HIVE_DATABASE,
					System.getenv(USER_PRINCIPAL),
					System.getenv(KEYTAB_LOCATION));
		}
		return ConnectionManager.getConnection(HIVE_DATABASE);
	}

	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error("SQL Exception occurred in close Connection ", e);
			}
		}
	}
}

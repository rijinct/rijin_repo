
package com.project.rithomas.jobexecution.common.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;

public final class DBConnectionManager {

	public static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DBConnectionManager.class);

	private static DBConnectionManager dbConnectionManager;

	private DBConnectionManager() {
	}

	static {
		dbConnectionManager = new DBConnectionManager();
	}

	public static DBConnectionManager getInstance() {
		return dbConnectionManager;
	}

	public Connection getHiveConnection(String driver, String url,
			String userName, String password)
			throws SQLException, ClassNotFoundException {
		Class.forName(driver);
		return DriverManager.getConnection(url, userName, password);
	}

	public Connection getPostgresConnection(String driver, String url,
			String userName, String password)
			throws SQLException, ClassNotFoundException {
		Class.forName(driver);
		return DriverManager.getConnection(url, userName, password);
	}

	public void closeConnection(ResultSet dbResult, Statement dbStmt,
			Connection dbConnection) throws SQLException {
		if (dbResult != null) {
			try {
				dbResult.close();
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing result set: {}",
						e.getMessage(), e);
			}
		}
		if (dbStmt != null) {
			try {
				dbStmt.close();
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing statement: {}",
						e.getMessage(), e);
			}
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
	}

	public void closeConnection(Statement dbStmt, Connection dbConnection)
			throws SQLException {
		if (dbStmt != null) {
			try {
				dbStmt.close();
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing statement: {}",
						e.getMessage(), e);
			}
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
	}
	public Connection getConnection(String executionEngine,
			boolean shouldConnectToCustomDbUrl) throws Exception {
		HiveConfiguration configuration = HiveConfigurationProvider.getInstance().getConfiguration();
		Connection con = ConnectionManager.getConnection(executionEngine,
				configuration.getKerberosPrincipal(),
				configuration.getKeytabLocation(),
				shouldConnectToCustomDbUrl);
		return con;
	}
}

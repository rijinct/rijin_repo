
package com.project.rithomas.jobexecution.common.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class HiveJobRunner implements schedulerJobRunner {

	public Statement st = null;

	public Connection con = null;

	public PreparedStatement pst = null;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveJobRunner.class);

	public HiveJobRunner(boolean shouldConnectToCustomDbUrl) throws Exception {
		con = DBConnectionManager.getInstance().getConnection(
				schedulerConstants.HIVE_DATABASE, shouldConnectToCustomDbUrl);
		st = con.createStatement();
	}

	@Override
	public void setQueryHint(String queryHint) throws SQLException {
		LOGGER.debug("Executing query hint {} on hive", queryHint);
		st.execute(queryHint);
	}

	@Override
	public void setQueryHints(List<String> queryHints) throws SQLException {
		for (String queryHint : queryHints) {
			LOGGER.debug("Executing query hint {} on hive", queryHint);
			st.execute(queryHint);
		}
	}

	@Override
	public void runQuery(String query) throws SQLException {
		if (pst == null) {
			pst = con.prepareStatement(query);
		}
		LOGGER.debug("Executing query {} ", query);
		pst.execute();
	}

	@Override
	public void closeConnection() {
		if (pst != null) {
			try {
				ConnectionManager.closeStatement(pst);
				pst = null;
			} catch (Exception e) {
				LOGGER.warn("Exception while closing the prepared statement {}",
						e.getMessage());
			}
		}
		if (st != null) {
			try {
				ConnectionManager.closeStatement(st);
				st = null;
			} catch (Exception e) {
				LOGGER.warn("Exception while closing the statement {}",
						e.getMessage());
			}
		}
		try {
			if (con != null) {
				ConnectionManager.releaseConnection(con,
						schedulerConstants.HIVE_DATABASE);
				con = null;
			}
		} catch (Exception e) {
			LOGGER.warn("Exception while closing the connection {}",
					e.getMessage());
		}
	}

	@Override
	public Object runQuery(String query, String outputDirName,
			WorkFlowContext context) throws SQLException {
		ResultSet rs = st.executeQuery(query);
		return rs;
	}

	@Override
	public Statement getPreparedStatement(String query) throws SQLException {
		pst = con.prepareStatement(query);
		return pst;
	}
}

package com.project.rithomas.jobexecution.common.util;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public interface schedulerJobRunner {

	void runQuery(String query)
			throws SQLException, JobExecutionException;

	void setQueryHint(String queryHint) throws SQLException;

	Object runQuery(String query, String outputDirName,
			WorkFlowContext context) throws JobExecutionException, SQLException;

	void closeConnection();

	Statement getPreparedStatement(String query) throws SQLException;

	void setQueryHints(List<String> queryHints) throws SQLException;
}

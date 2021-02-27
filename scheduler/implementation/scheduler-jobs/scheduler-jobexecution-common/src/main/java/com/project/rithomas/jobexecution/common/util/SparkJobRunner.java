package com.project.rithomas.jobexecution.common.util;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.schedulerSparkLauncher;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class SparkJobRunner implements schedulerJobRunner {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SparkJobRunner.class);

	List<String> queryHints = new ArrayList<String>();

	@Override
	public void runQuery(String query) throws JobExecutionException {
		List<String> sparkLuancherArgs = new ArrayList<String>();
		LOGGER.debug("Executing query hints {} on spark for query {}",
				queryHints, query);
		sparkLuancherArgs.addAll(queryHints);
		sparkLuancherArgs.add(query);
		schedulerSparkLauncher sparkLauncher = new schedulerSparkLauncher();
		boolean success = sparkLauncher.launchSparkSubmitJob(sparkLuancherArgs,
				null);
		if (!success) {
			throw new JobExecutionException(
					"Exception while running spark query.Please refer applicaton log for more details");
		}
	}

	@Override
	public void setQueryHint(String queryHint) {
		queryHints.add(queryHint);
	}

	@Override
	public Object runQuery(String query, String outputDirName,
			WorkFlowContext context) throws JobExecutionException {
		List<String> sparkLuancherArgs = new ArrayList<String>();
		sparkLuancherArgs.addAll(queryHints);
		sparkLuancherArgs.add(query);
		schedulerSparkLauncher sparkLauncher = new schedulerSparkLauncher();
		boolean success = sparkLauncher.launchSparkSubmitJob(sparkLuancherArgs,
				outputDirName);
		if (!success) {
			throw new JobExecutionException(
					"Exception while running spark query.Please refer applicaton log for more details");
		}
		List<String> fileContent = JobExecutionUtil.readSparkResultFile(context,
				outputDirName);
		return fileContent;
	}

	@Override
	public void closeConnection() {
	}

	@Override
	public Statement getPreparedStatement(String query) throws SQLException {
		return null;
	}

	@Override
	public void setQueryHints(List<String> jobQueryHints) throws SQLException {
		queryHints.addAll(jobQueryHints);
	}
}


package com.project.rithomas.jobexecution.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.ThreadContext;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.common.cache.service.CacheServiceException;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DQICalculatorUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
public class DQICalculator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DQICalculator.class);


	private String executionEngine;
	private double dqi = 0;
	private QueryExecutor queryExecutor = null;

	@SuppressWarnings({ "unchecked" })
	public void calculateDqi(WorkFlowContext context, Long lb, Long nextLb)
			throws WorkFlowExecutionException, SQLException, JobExecutionException, ETLException {
		Connection con = null;
		Statement st = null;
		Statement dqiStatement = null;
		String regionId = null;
		DQICalculatorUtil util = new DQICalculatorUtil();
		queryExecutor = util.getQueryExecutor();
		try {
			String jobName = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			LOGGER.info("Calculating dqi values..");
			executionEngine = JobExecutionUtil.getExecutionEngine(context);
			con = DBConnectionManager.getInstance()
					.getConnection(executionEngine, (boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			st = con.createStatement();
			dqiStatement = con.createStatement();
			// disabling the hive compression as the select result wasn't got
			// disabling hive auto convert join as hive query used to fail if
			// this is enabled.
			executeHiveQueryHints(jobName, st);
			executeHiveQueryHints(jobName, dqiStatement);
			String adaptationPLevelStr = jobName.replace("Perf_", "")
					.replace("_AggregateJob", "")
					.replace("_ReaggregateJob", "");
			if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			regionId = ThreadContext.get(JobExecutionContext.TZ_REGION_VAL);
			}
			boolean isReagg = context
					.getProperty(JobExecutionContext.REAGG_LAST_LB) != null;
			boolean valid = true;
			if (isReagg) {
				valid = util.checkIfDQIAlreadyCalculated(context, lb, regionId,
						adaptationPLevelStr);
			}
			if (valid) {
				startDQICalculation(context, lb, nextLb, st, dqiStatement,
						regionId, util, jobName, adaptationPLevelStr);
			} else {
				LOGGER.info(
						"Skipping dqi calculation as no previous dqi values are found to be updated for re-aggregation.");
			}
		} catch (Exception e) {
			throw new JobExecutionException(e.getMessage(), e);
		} finally {
			ConnectionManager.closeStatement(st);
			ConnectionManager.closeStatement(dqiStatement);
			ConnectionManager.releaseConnection(con, executionEngine);
		}
	}
	private void startDQICalculation(WorkFlowContext context, Long lb,
			Long nextLb, Statement st, Statement dqiStatement, String regionId,
			DQICalculatorUtil util, String jobName, String adaptationPLevelStr)
			throws JobExecutionException, CacheServiceException, SQLException,
			ETLException {
		String sourceJobType = (String) context
					.getProperty(JobExecutionContext.SOURCEJOBTYPE);
		String adaptationId = (String) context
					.getProperty(JobExecutionContext.ADAPTATION_ID);
		String adaptationVersion = (String) context
					.getProperty(JobExecutionContext.ADAPTATION_VERSION);
				LOGGER.debug("Calculating dqi for region: {}", regionId);
				// Calculating the max time till which the date can go, as for
				// an
				// interval it should only consider value till x:59.59 and not
				// x:00:00 time.
				List<String> sourceNameList = new ArrayList<String>();
		String description = null;
		if (JobTypeDictionary.USAGE_JOB_TYPE.equalsIgnoreCase(sourceJobType)) {
			UsageDQICalculator calculator = new UsageDQICalculator(lb, nextLb,
					regionId, adaptationId, adaptationVersion);
			dqi = calculator.calculateDQIForSrcUsage(context, dqiStatement,
					util, sourceNameList, queryExecutor);
			description = getDescription(sourceNameList, dqi);
			} else if (JobTypeDictionary.PERF_JOB_TYPE
					.equalsIgnoreCase(sourceJobType)
					|| JobTypeDictionary.PERF_REAGG_JOB_TYPE
							.equalsIgnoreCase(sourceJobType)) {
			AggregationDQICalculator calculator = new AggregationDQICalculator(
					lb, nextLb, regionId, adaptationId, adaptationVersion);
			dqi = calculator.calculateDQIForSrcAgg(context, st, jobName,
					queryExecutor);
			description = (String) context
					.getProperty(JobExecutionContext.DQI_DESCRIPTION);
			}
		if (context.getProperty(JobExecutionContext.PERF_JOB_NAME) != null) {
				jobName = (String) context
						.getProperty(JobExecutionContext.PERF_JOB_NAME);
						}
			LOGGER.debug("value written to table : {}", dqi);
			util.writeToDQITable(context, st, lb, dqi, regionId, description,
					jobName, adaptationPLevelStr);

							// non SGSN case..

			// For LTE and HTTP 4G, report time is already stored as
	}





	private void executeHiveQueryHints(String jobId, Statement st)
			throws WorkFlowExecutionException, SQLException {
		List<String> hiveSettings = HiveConfigurationProvider.getInstance().getQueryHints(jobId,
				executionEngine);
		for (String queryHint : hiveSettings) {
			LOGGER.debug("Executing query hint:{}", queryHint);
			st.execute(queryHint);
		}


	}

	private String getDescription(List<String> sourceNameList,
			Double dqiValue) {
		String description = null;
		LOGGER.debug(
				"sourcename list : {}, dqi value : {} to construct description",
				sourceNameList, dqiValue);
		if (sourceNameList.isEmpty()) {
			if (dqiValue == 0.0) {
				description = "No data sources are configured";
			} else {
				description = "Data available for all the sources";
			}
		} else {
			StringBuilder builder = new StringBuilder();
			if (!sourceNameList.isEmpty()) {
				Collections.sort(sourceNameList);
				builder.append("No data found for ");
				for (String sourceName : sourceNameList) {
					builder.append(sourceName);
					builder.append(" ");
				}
				builder.append("source(s).");
			}
			description = builder.toString();
		}
		return description;
	}

		// If say the source tables has only 1 PS but another is ES for lookup
		// and source job list has only PS job name.
		// Then even the source table name should be only PS name.
}

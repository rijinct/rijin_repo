package com.project.rithomas.jobexecution.common;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.AggregationDQICalculatorUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class AggregationDQICalculator {

	private static final String AGG_DEFAULT_DESC = "DQI value is populated from previous aggregation level";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AggregationDQICalculator.class);

	private final static boolean DEBUGMODE = LOGGER.isDebugEnabled();

	private List<String> sourceJobList = null;

	private double dqi = 0;

	private Long lb = null;

	private Long nextLb = null;

	private String regionId = null;

	private String adaptationId = null;

	private String adaptationVersion = null;

	public AggregationDQICalculator(Long lbValue, Long nextLbValue,
			String regionIdValue, String adaptationIdValue,
			String adaptationVersionVal) {
		lb = lbValue;
		nextLb = nextLbValue;
		regionId = regionIdValue;
		adaptationId = adaptationIdValue;
		adaptationVersion = adaptationVersionVal;
	}

	public double calculateDQIForSrcAgg(WorkFlowContext context, Statement st,
			String jobName, QueryExecutor queryExecutor)
			throws JobExecutionException, SQLException {
		sourceJobList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		boolean multipleSources = AggregationDQICalculatorUtil
				.isMultipleSource(context);
		String sourceTableNameList = AggregationDQICalculatorUtil
				.getSourceTableNameList(context, queryExecutor, sourceJobList);
		if (!multipleSources) {
			calculateDQIValue(context, st, sourceTableNameList);
		} else {
			calculateDQIValueForMultiSource(context, st, jobName, queryExecutor,
					sourceTableNameList);
		}
		return dqi;
	}

	private void calculateDQIValue(WorkFlowContext context, Statement st,
			String sourceTableNameList) throws SQLException {
		ResultSet aggResultSet = null;
		try {
			aggResultSet = executeQueryForSourceTableName(sourceTableNameList,
					sourceJobList, lb, nextLb, regionId, context, st);
			calculateDQIFromPreviousDQIValues(context, aggResultSet, regionId);
		} finally {
			ConnectionManager.closeResultSet(aggResultSet);
		}
	}

	private void calculateDQIValueForMultiSource(WorkFlowContext context,
			Statement st, String jobName, QueryExecutor queryExecutor,
			String sourceTableNameList)
			throws JobExecutionException, SQLException {
		String queryToGetSource = AggregationDQICalculatorUtil
				.getQueryToGetSource(regionId, jobName);
		List<Object> resultList = queryExecutor
				.executeMetadatasqlQueryMultiple(queryToGetSource, context);
		double dqiSum = 0;
		int dqiTotalWeight = 0;
		if (resultList == null || resultList.size() < 2) {
			calculateDQIValue(context, st, sourceTableNameList);
		} else {
			calculateDQIValueMultiSource(context, st, queryExecutor, resultList,
					dqiSum, dqiTotalWeight);
		}
	}

	private void calculateDQIValueMultiSource(WorkFlowContext context,
			Statement st, QueryExecutor queryExecutor, List<Object> resultList,
			double dqiSum, int dqiTotalWeight)
			throws SQLException, JobExecutionException {
		String sourceTableName;
		for (Object result : resultList) {
			Object[] resultIt = (Object[]) result;
			sourceTableName = (String) resultIt[0];
			String sourceAdapName = (String) resultIt[1];
			calculateDQIValue(context, st, sourceTableName);
			String weightsQuery = "select ds_weight from saidata.es_data_source_name_1 where source_enabled = 'YES' and adaptation_id='"
					+ adaptationId + "' and adaptation_version='"
					+ adaptationVersion + "' and ds_name='" + sourceAdapName
					+ "' and ds_version!='" + JobExecutionContext.OVERALL_STR
					+ "'";
			List<Object> aggResultSetList = queryExecutor
					.executeMetadatasqlQueryMultiple(weightsQuery, context);
			if (aggResultSetList != null && !aggResultSetList.isEmpty()) {
				for (Object aggResult : aggResultSetList) {
					Integer weight = Integer.parseInt(aggResult.toString());
					dqiSum = (dqi * weight) + dqiSum;
					dqiTotalWeight = dqiTotalWeight + weight;
				}
			}
		}
		if (dqiTotalWeight != 0) {
			dqi = dqiSum / dqiTotalWeight;
		} else {
			LOGGER.info(
					"Weights not configured for any of the enabled sources for this job. Hence marking dqi value as 0.");
			dqi = 0;
		}
	}

	private void calculateDQIFromPreviousDQIValues(WorkFlowContext context,
			ResultSet aggResultSet, String regionId) throws SQLException {
		double dqiSum = 0;
		int count = 0;
		while (aggResultSet.next()) {
			if ((TimeZoneUtil.isTimeZoneEnabled(context)
					&& (regionId.equals(aggResultSet.getString(2))))
					|| !TimeZoneUtil.isTimeZoneEnabled(context)) {
				dqiSum = aggResultSet.getDouble(1) + dqiSum;
				count++;
			}
		}
		if (count != 0) {

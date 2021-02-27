package com.project.rithomas.jobexecution.common;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.common.cache.service.CacheServiceException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DQICalculatorUtil;
import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UsageDQICalculatorUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class UsageDQICalculator {

	private String adaptationId = null;

	private String adaptationVersion = null;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UsageDQICalculator.class);

	private QueryExecutor usageQueryExecutor = null;

	private List<String> sourceJobList = null;

	private boolean dataSourcesConfigured = true;

	private double dqi = 0;

	List<Double> dqiValueList = new ArrayList<Double>();

	private Long lb = null;

	private Long nextLb = null;

	private String regionId = null;

	private boolean isReagg = false;

	private Integer thresholdValue = null;

	private Long maxValue = null;

	private List<String> sourceNameList = null;

	private DQICalculatorUtil util = null;

	private String postgreSql = null;

	private String usageSpecName = null;

	private String usageSpecVersion = null;

	private String usageSpecId = null;

	private String sourceTableName = null;

	private int overallDQIWeight = 0;
	
	private DataAvailabilityCacheUtil dataAvailabilityCacheService;

	public UsageDQICalculator(Long lbValue, Long nextLbValue,
			String regionIdVal, String adaptationIdVal,
			String adaptationVersionVal) {
		lb = lbValue;
		nextLb = nextLbValue;
		regionId = regionIdVal;
		adaptationId = adaptationIdVal;
		adaptationVersion = adaptationVersionVal;
	}

	public double calculateDQIForSrcUsage(WorkFlowContext context,
			Statement dqiStatement, DQICalculatorUtil dqiCalcUtil,
			List<String> sourceNameListValues, QueryExecutor queryExecutor)
			throws JobExecutionException, CacheServiceException, SQLException {
		overallDQIWeight = 0;
		usageQueryExecutor = queryExecutor;
		sourceJobList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		isReagg = context
				.getProperty(JobExecutionContext.REAGG_LAST_LB) != null;
		thresholdValue = UsageDQICalculatorUtil.getThresholdValue(context);
		LOGGER.debug("DQI threshold configured is: {}", thresholdValue);
		maxValue = UsageDQICalculatorUtil.getMaxValue(nextLb, thresholdValue);
		sourceNameList = sourceNameListValues;
		util = dqiCalcUtil;
		try {
			startUsageDQICalculation(context, dqiStatement, queryExecutor);
			dqi = UsageDQICalculatorUtil.calculateFinalDQIValue(
					overallDQIWeight, dqiValueList, dqi);
			LOGGER.debug("Final value for usage : {}", dqi);
		} catch (CacheServiceException e) {
			throw new JobExecutionException(e.getMessage(), e);
		} 
		return dqi;
	}

	private void startUsageDQICalculation(WorkFlowContext context,
			Statement dqiStatement, QueryExecutor queryExecutor)
			throws JobExecutionException, CacheServiceException, SQLException {
		for (String sourceJobName : sourceJobList) {
			String srcTableNameQuery = "select table_name from job_list where jobid ='"
					+ sourceJobName + "'";
			Object[] srcTableNameArr = queryExecutor
					.executeMetadatasqlQuery(srcTableNameQuery, context);
			sourceTableName = (String) srcTableNameArr[0];
			String sourcePartColumn = UsageDQICalculatorUtil
					.getSourcePartColumn(context, sourceJobName);
			setAdaptationDetails(context, sourceJobName);
			LOGGER.debug("adaptation id : {}, adaptation version : {}",
					adaptationId, adaptationVersion);
			usageSpecName = sourceJobName.replace("Usage_", "")
					.replace("_LoadJob", "");
			usageSpecVersion = usageSpecName
					.split("_")[usageSpecName.split("_").length - 1];
			usageSpecId = usageSpecName.replace("_" + usageSpecVersion, "");
			String dataSourceColumn = UsageDQICalculatorUtil
					.getDataSourceColumn(context);
			LOGGER.debug("Data Source column : {}", dataSourceColumn);
			String unixReportTime = "unix_timestamp(" + sourcePartColumn + ")";
			// For LTE and HTTP 4G, report time is already stored as
			// unix time-stamp.
			setPostgreSql(context);
			doDQICalculation(context, dqiStatement, queryExecutor,
					sourceJobName, unixReportTime);
		}
	}

	private void doDQICalculation(WorkFlowContext context,
			Statement dqiStatement, QueryExecutor queryExecutor,
			String sourceJobName, String unixReportTime)
			throws JobExecutionException, CacheServiceException, SQLException {
		List<Object[]> resultSet = UsageDQICalculatorUtil
				.getDataSourceMappingSGSNCase(context, queryExecutor,
						adaptationId, adaptationVersion);
		if (resultSet != null && !resultSet.isEmpty()) {
			startDQICalculationSGSNCase(context, dqiStatement, sourceJobName,
					resultSet, unixReportTime);
		} else {
			startDQICalculationNONSGSNCase(context, dqiStatement,
					unixReportTime);
		}
	}

	private void setPostgreSql(WorkFlowContext context) {
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			postgreSql = UsageDQICalculatorUtil.setPostgresSQLTZEnabled(
					regionId, usageSpecVersion, usageSpecId, adaptationId,
					adaptationVersion);
		} else {
			postgreSql = UsageDQICalculatorUtil.setPostgresSql(usageSpecVersion,
					usageSpecId, adaptationId, adaptationVersion);
		}
	}

	private void setAdaptationDetails(WorkFlowContext context,
			String sourceJobName) {
		if (context.getProperty(sourceJobName + "_"
				+ JobExecutionContext.ADAPTATION_ID) != null) {
			adaptationId = (String) context.getProperty(
					sourceJobName + "_" + JobExecutionContext.ADAPTATION_ID);
			adaptationVersion = (String) context.getProperty(sourceJobName + "_"
					+ JobExecutionContext.ADAPTATION_VERSION);
		}
	}

	private void startDQICalculationNONSGSNCase(WorkFlowContext context,
			Statement dqiStatement, String unixReportTime)
			throws JobExecutionException, CacheServiceException, SQLException {
		double dqiSum = 0;
		int dqiTotalWeight = 0;
		double dqiVal = 0;
		String overallWeightQuery = String.format(
				"select ds_weight from saidata.es_data_source_name_1 where adaptation_id='%s' and adaptation_version='%s' and usage_specification_id='%s' and usage_specification_version='%s' and ds_version='%s'",
				adaptationId, adaptationVersion, usageSpecId, usageSpecVersion,
				JobExecutionContext.OVERALL_STR);
		Object[] overallWeightQueryResultSetList = null;
		if (sourceJobList.size() != 1) {
			overallWeightQueryResultSetList = usageQueryExecutor
					.executeMetadatasqlQuery(overallWeightQuery, context);
		}
		if (sourceJobList.size() == 1
				|| (overallWeightQueryResultSetList != null)) {
			dqiVal = calculateDQIValueNonSGSNCase(context, dqiStatement,
					unixReportTime, dqiSum, dqiTotalWeight, dqiVal);
			LOGGER.debug("DQI Value : {}", dqiVal);
			setFinalDQIValueInfo(dqiVal, overallWeightQueryResultSetList);
		}
	}

	private double calculateDQIValueNonSGSNCase(WorkFlowContext context,
			Statement dqiStatement, String unixReportTime, double dqiSum,
			int dqiTotalWeight, double dqiVal)
			throws JobExecutionException, CacheServiceException, SQLException {
		if (!isReagg) {
			LOGGER.debug(
					"SQL query for getting sources when job type is Loading is : {}",
					postgreSql);
			List<Object[]> resultSetList = usageQueryExecutor
					.executeMetadatasqlQueryMultiple(postgreSql, context);
			if (resultSetList != null && !resultSetList.isEmpty()) {
				dqiVal = calculateDQIValue(dqiSum, dqiTotalWeight, dqiVal,
						resultSetList);
			}
		} else {
			dqiVal = calculateDQIValue(dqiStatement, dqiSum, dqiTotalWeight,
					dqiVal, context, unixReportTime);
		}
		return dqiVal;
	}

	private void setFinalDQIValueInfo(double dqiVal,
			Object[] overallWeightQueryResultSetList) {
		if (sourceJobList.size() == 1) {
			dqi = dqiVal;
		} else {
			LOGGER.debug("non SGSN weight : {}, dqi_value: {}",
					Long.parseLong(
							overallWeightQueryResultSetList[0].toString()),
					dqiVal);
			overallDQIWeight = UsageDQICalculatorUtil
					.addToDQIListAndGetOverallDQIWeight(overallDQIWeight,
							dqiVal, overallWeightQueryResultSetList,
							dqiValueList);
		}
	}

	private void startDQICalculationSGSNCase(WorkFlowContext context,
			Statement dqiStatement, String sourceJobName,
			List<Object[]> resultSet, String unixReportTime)
			throws JobExecutionException, CacheServiceException, SQLException {
		double dqiSum = 0;
		int dqiTotalWeight = 0;
		boolean isDataAvailable = false;
		double dqiVal = 0;
		isDataAvailable = UsageDQICalculatorUtil.checkIfDataAvailable(context,
				resultSet, isDataAvailable, usageQueryExecutor);
		if (isDataAvailable) {
			LOGGER.debug("data available1");
			String overallWeightQuery = String.format(
					"select ds_weight from saidata.es_data_source_name_1 where adaptation_id='%s' and adaptation_version='%s' and ds_version='%s'",
					adaptationId, adaptationVersion,
					JobExecutionContext.OVERALL_STR);
			Object[] overallWeightQueryResultSetList = usageQueryExecutor
					.executeMetadatasqlQuery(overallWeightQuery, context);
			if (overallWeightQueryResultSetList != null) {
				dataSourcesConfigured = true;
				dqiVal = calculateDQIValueSGSNCase(context, dqiStatement,
						unixReportTime, dqiSum, dqiTotalWeight, dqiVal);
				setFinalDQIValueInfoSGSNCase(sourceJobName, dqiVal,
						overallWeightQueryResultSetList);
			}
		}
	}

	private double calculateDQIValueSGSNCase(WorkFlowContext context,
			Statement dqiStatement, String unixReportTime, double dqiSum,
			int dqiTotalWeight, double dqiVal)
			throws JobExecutionException, CacheServiceException, SQLException {
		if (!isReagg) {
			LOGGER.debug(
					"SQL query for getting sources when job type is Loading is : {}",
					postgreSql);
			List<Object[]> resultSetList = usageQueryExecutor
					.executeMetadatasqlQueryMultiple(postgreSql, context);
			if (resultSetList != null && !resultSetList.isEmpty()) {
				dqiVal = calculateDQIValue(dqiSum, dqiTotalWeight, dqiVal,
						resultSetList);
			} else {
				dataSourcesConfigured = false;
			}
		} else {
			dqiVal = calculateDQIValue(dqiStatement, dqiSum, dqiTotalWeight,
					dqiVal, context, unixReportTime);
		}
		return dqiVal;
	}

	private void setFinalDQIValueInfoSGSNCase(String sourceJobName,
			double dqiVal, Object[] overallWeightQueryResultSetList) {
		if (dataSourcesConfigured || regionId == null) {
			if (!dataSourcesConfigured) {
				LOGGER.info(
						"No data source configured for the job: {}, hence DQI value will be 0",
						sourceJobName);
			}
			LOGGER.debug("DQI Value : {}", dqiVal);
			LOGGER.debug("weight : {}, dqi_value : {}",
					overallWeightQueryResultSetList[0].toString(), dqiVal);
			overallDQIWeight = UsageDQICalculatorUtil
					.addToDQIListAndGetOverallDQIWeight(overallDQIWeight,
							dqiVal, overallWeightQueryResultSetList,
							dqiValueList);
		} else {
			LOGGER.info(
					"No data source configured for the job: {} and region: {} hence not including for dqi calculation.",
					sourceJobName, regionId);
		}
	}

	private double calculateDQIValue(double dqiSum, int dqiTotalWeight, double dqiVal, List<Object[]> resultSetList)
			throws CacheServiceException {
		String key = util.getCacheKey(regionId, adaptationId, adaptationVersion, usageSpecName);
		dataAvailabilityCacheService = new DataAvailabilityCacheUtil();
		try {
			for (Object[] resultSetArray : resultSetList) {
				dqiSum += getDQISum(resultSetArray, key, sourceNameList, thresholdValue, lb,
						nextLb);
				Long weight = resultSetArray[2] != null ? Long.parseLong(resultSetArray[2].toString()) : null;
				if (weight != null) {
					dqiTotalWeight += weight;
				}
				LOGGER.debug("DQI Total Weight : {}", dqiTotalWeight);
			}
		} finally {
			dataAvailabilityCacheService.closeConnection();
		}
		if (dqiTotalWeight != 0) {
			dqiVal = dqiSum * 100 / dqiTotalWeight;
		}
		return dqiVal;
	}

	private double getDQISum(Object[] resultSetArray,String key,
			List<String> sourceNameList, Integer thresholdValue, Long lb,
			Long nextLb) throws CacheServiceException {
		double dqi = 0;
		String dataSourceId = resultSetArray[0] != null
				? resultSetArray[0].toString() : "";
		String dataSourceName = resultSetArray[1] != null
				? resultSetArray[1].toString() : null;
		Long weight = resultSetArray[2] != null
				? Long.parseLong(resultSetArray[2].toString()) : null;
		key = key.replace(JobExecutionContext.STAGE_TYPE, dataSourceId);
		String ltValue = getLtValue(key, lb);
		String utValue = getUtValue(key, thresholdValue, nextLb);
		LOGGER.debug("LT value: {}, UT value: {}", ltValue, utValue);
		int sourceCheck = 1;
		if (ltValue == null || utValue == null) {
			sourceNameList.add(dataSourceName);
			sourceCheck = 0;
		}
		LOGGER.debug("The weight which is configured is : {}", weight);
		if (weight != null) {
			dqi = (weight * sourceCheck);
		}
		LOGGER.debug("DQI Sum : {}", dqi);
		return dqi;
	}

	private String getUtValue(String key, Integer thresholdValue, Long nextLb)
			throws CacheServiceException {
		Long upperThreshold = nextLb - (thresholdValue * 60 * 1000L);
		String upperThresholdKey = key.replace(JobExecutionContext.LOWER_BOUND,
				upperThreshold.toString());
		return dataAvailabilityCacheService.getValue(upperThresholdKey);
	}

	private String getLtValue(String key, Long lb) throws CacheServiceException {
		String lowerThresholdKey = key.replace(JobExecutionContext.LOWER_BOUND,
				lb.toString());
		return dataAvailabilityCacheService.getValue(lowerThresholdKey);
	}

	private double getDQISum(ResultSet dqiResultSet, Integer thresholdValue,
			List<String> sourceNameList, long lb, long nextLb, long maxValue)
			throws SQLException {
		int sourceCheck = 0;
		double dqiVal = 0;
		Object dqiObject4 = dqiResultSet.getObject(4);
		Object dqiObject5 = dqiResultSet.getObject(5);
		if (dqiObject4 != null && dqiObject5 != null) {
			Long minReportTime = dqiResultSet.getLong(4);
			Long maxReportTime = dqiResultSet.getLong(5);
			Long maxValueWithThreshold = (maxReportTime + (thresholdValue * 60))
					* 1000;
			Long minValueWithThreshold = (minReportTime - (thresholdValue * 60))
					* 1000;
			if ((minValueWithThreshold < lb)
					&& (maxValueWithThreshold >= nextLb)
					&& (maxValueWithThreshold <= maxValue)) {
				sourceCheck = 1;
			} else {
				sourceNameList.add(dqiResultSet.getString(2));
				sourceCheck = 0;
			}
			LOGGER.debug("The weight which is configured is : {}",
					dqiResultSet.getInt(3));
			dqiVal = dqiResultSet.getInt(3) * sourceCheck;
			LOGGER.debug("DQI Value : {}", dqiVal);
		} else {
			sourceNameList.add(dqiResultSet.getString(2));
		}
		return dqiVal;
	}

	private double calculateDQIValue(Statement dqiStatement, double dqiSum,
			int dqiTotalWeight, double dqiVal, WorkFlowContext context,
			String unixReportTime) throws SQLException {
		ResultSet dqiResultSet = null;
		String sql = null;
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			sql = new StringBuilder(
					"select ds_id,ds_name,ds_weight,min_report_time,max_report_time from ")
							.append("(select data_source_id FROM es_data_src_rgn_info_1 where region_id='")
							.append(regionId).append("') ")
							.append("dsr join (select * from es_data_source_name_1 where adaptation_id='")
							.append(adaptationId)
							.append("' and adaptation_version='")
							.append(adaptationVersion)
							.append("' and usage_specification_id='")
							.append(usageSpecId)
							.append("' and usage_specification_version='")
							.append(usageSpecVersion)
							.append("' and source_enabled='YES' and ds_version!='")
							.append(JobExecutionContext.OVERALL_STR)
							.append("')  es_data_source_name_1 on es_data_source_name_1.ds_id=dsr.data_source_id left outer join ")
							.append("(select source_id, min(")
							.append(unixReportTime)
							.append(") min_report_time, max(")
							.append(unixReportTime).append(") max_report_time ")
							.append("from ").append(sourceTableName)
							.append(" where dt>='").append(lb)
							.append("' and dt<'").append(nextLb)
							.append("' group by source_id) ")
							.append(sourceTableName)
							.append(" on(es_data_source_name_1.ds_id=")
							.append(sourceTableName).append(".source_id)")
							.toString();
			context.setProperty(
					JobExecutionContext.HIVE_SESSION_QUERY + "_" + regionId,
					sql);
		} else {
			sql = new StringBuilder(
					"select ds_id,ds_name,ds_weight,min_report_time,max_report_time from ")
							.append("(select * from es_data_source_name_1 where adaptation_id='")
							.append(adaptationId)
							.append("' and adaptation_version='")
							.append(adaptationVersion)
							.append("' and usage_specification_id='")
							.append(usageSpecId)
							.append("' and usage_specification_version='")
							.append(usageSpecVersion)
							.append("' and source_enabled='YES' ")
							.append("and ds_version!='")
							.append(JobExecutionContext.OVERALL_STR)
							.append("')  es_data_source_name_1 left outer join ")
							.append("(select source_id, min(")
							.append(unixReportTime)
							.append(") min_report_time, max(")
							.append(unixReportTime).append(") ")
							.append("max_report_time from ")
							.append(sourceTableName).append(" where dt>='")
							.append(lb).append("' and dt<'").append(nextLb)
							.append("' group by source_id) ")
							.append(sourceTableName)
							.append(" on(es_data_source_name_1.ds_id=")
							.append(sourceTableName).append(".source_id)")
							.toString();
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, sql);
		}
		LOGGER.debug(
				"SQL query for dqi calulation when job type is Loading is : {}",
				sql);
		try {
			dqiResultSet = dqiStatement.executeQuery(sql);
			if (dqiResultSet.next()) {
				do {
					dqiSum += getDQISum(dqiResultSet, thresholdValue,
							sourceNameList, lb, nextLb, maxValue);
					LOGGER.debug("DQI Sum : {}", dqiSum);
					dqiTotalWeight += dqiResultSet.getInt(3);
					LOGGER.debug("DQI Total Weight : {}", dqiTotalWeight);
				} while (dqiResultSet.next());
			} else {
				dataSourcesConfigured = false;
			}
		} finally {
			ConnectionManager.closeResultSet(dqiResultSet);
		}
		if (dqiTotalWeight != 0) {
			dqiVal = dqiSum * 100 / dqiTotalWeight;
		}
		return dqiVal;
	}
}

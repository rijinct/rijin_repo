package com.project.rithomas.jobexecution.common.util;


import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class UsageDQICalculatorUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UsageDQICalculatorUtil.class);

	public static double calculateFinalDQIValue(int overallDQIWeight, List<Double> dqiValueList, double dqi) {
		if (dqiValueList != null && !dqiValueList.isEmpty()) {
			if (dqiValueList.size() == 1) {
				dqi = dqiValueList.get(0) / overallDQIWeight;
				LOGGER.debug("final DQI value : {}", dqi);
			} else {
				double overallDQIValue = 0;
				for (double dqiValObj : dqiValueList) {
					overallDQIValue += dqiValObj;
				}
				dqi = overallDQIValue / overallDQIWeight;
				LOGGER.debug("overall DQI value : {} with DQI weight: {} and DQI value : {}",
						new Object[] { overallDQIValue, overallDQIWeight, dqi });
			}
		}
		return dqi;
	}

	public static String getDataSourceColumn(WorkFlowContext context) {
		String dataSourceColumn = "source_id";
		if (context.getProperty(JobExecutionContext.DATA_SOURCE_COLUMN) != null
				&& !context.getProperty(JobExecutionContext.DATA_SOURCE_COLUMN).toString().equalsIgnoreCase("NA")) {
			dataSourceColumn = (String) context.getProperty(JobExecutionContext.DATA_SOURCE_COLUMN);
		}
		return dataSourceColumn;
	}

	public static String getSourcePartColumn(WorkFlowContext context, String sourceJobName) {
		String sourcePartColumn = "event_time";
		LOGGER.debug("sourceJobName value : {}", sourceJobName);
		if (context.getProperty(sourceJobName + "_" + JobExecutionContext.PARTITION_COLUMN) != null) {
			sourcePartColumn = (String) context.getProperty(sourceJobName + "_" + JobExecutionContext.PARTITION_COLUMN);
		}
		LOGGER.debug("Source Partition Column : {}", sourcePartColumn);
		if (sourcePartColumn.contains(";")) {
			sourcePartColumn = sourcePartColumn.substring(sourcePartColumn.indexOf(';') + 1);
		}
		return sourcePartColumn;
	}

	public static String setPostgresSql(String usageSpecVersion, String usageSpecId, String adaptationId,
			String adaptationVersion) {
		String postgreSql;
		postgreSql = "(select ds_id, ds_name, ds_weight from saidata.es_data_source_name_1 where adaptation_id='"
				+ adaptationId + "' and adaptation_version='" + adaptationVersion + "' and usage_specification_id='"
				+ usageSpecId + "' and usage_specification_version='" + usageSpecVersion + "' and source_enabled='YES' "
				+ "and ds_version!='" + JobExecutionContext.OVERALL_STR + "')";
		return postgreSql;
	}

	public static String setPostgresSQLTZEnabled(String regionId, String usageSpecVersion, String usageSpecId,
			String adaptationId, String adaptationVersion) {
		String postgreSql = null;
		postgreSql = "select dsr.data_source_id, es_data_source_name_1.ds_name, es_data_source_name_1.ds_weight from "
				+ "(select data_source_id FROM saidata.es_data_src_rgn_info_1 where region_id='" + regionId + "') "
				+ "dsr join (select * from saidata.es_data_source_name_1 where adaptation_id='" + adaptationId
				+ "' and adaptation_version='" + adaptationVersion + "' and usage_specification_id='" + usageSpecId
				+ "' and usage_specification_version='" + usageSpecVersion
				+ "' and source_enabled='YES' and ds_version!='" + JobExecutionContext.OVERALL_STR
				+ "')  es_data_source_name_1 on es_data_source_name_1.ds_id=dsr.data_source_id";
		return postgreSql;
	}

	public static List<Object[]> getDataSourceMappingSGSNCase(WorkFlowContext context, QueryExecutor queryExecutor,
			String adaptationId, String adaptationVersion) throws JobExecutionException {
		String getDataSourceSQL = "select source, type, adaptation_id, adaptation_version from "
				+ "rithomas.data_source_mapping where adaptation_id='" + adaptationId + "' and adaptation_version='"
				+ adaptationVersion + "'";
		return (List<Object[]>) queryExecutor.executeMetadatasqlQueryMultiple(getDataSourceSQL, context);
	}

	public static int addToDQIListAndGetOverallDQIWeight(int overallDQIWeight, double dqiVal,
			Object[] overallWeightQueryResultSetList, List<Double> dqiValueList) {
		dqiValueList.add(Long.parseLong(overallWeightQueryResultSetList[0].toString()) * dqiVal);
		overallDQIWeight += Long.parseLong(overallWeightQueryResultSetList[0].toString());
		return overallDQIWeight;
	}

	public static boolean checkIfDataAvailable(WorkFlowContext context, List<Object[]> resultSet,
			boolean isDataAvailable, QueryExecutor usageQueryExecutor) throws JobExecutionException {
		for (Object[] resultVal : resultSet) {
			String dataAvailableQuery = "select available from saidata.es_sgsn_available_data_1 where source='"
					+ resultVal[0] + "' and type='" + resultVal[1] + "'";
			List<Object> availableDataResultSet = usageQueryExecutor.executeMetadatasqlQueryMultiple(dataAvailableQuery,
					context);
			if (availableDataResultSet != null && !availableDataResultSet.isEmpty()) {
				for (Object availableDataResult : availableDataResultSet) {
					if ("YES".equalsIgnoreCase(availableDataResult.toString())) {
						isDataAvailable = true;
						LOGGER.debug("Data is available for entity : {}", resultVal[0]);
					}
				}
			}
		}
		return isDataAvailable;
	}

	public static Long getMaxValue(Long nextLb, Integer thresholdValue) {
		// Calculating the max time till which the date can go, as for
		// an
		// interval it should only consider value till x:59.59 and not
		// x:00:00 time.
		return ((nextLb / 1000) + ((thresholdValue - 1) * 60) + 59) * 1000;
	}

	public static Integer getThresholdValue(WorkFlowContext context) {
		Integer thresholdValue = 5;
		if (context.getProperty(JobExecutionContext.DQI_THRESHOLD) != null) {
			thresholdValue = Integer.parseInt(context.getProperty(JobExecutionContext.DQI_THRESHOLD).toString());
		}
		return thresholdValue;
	}
}

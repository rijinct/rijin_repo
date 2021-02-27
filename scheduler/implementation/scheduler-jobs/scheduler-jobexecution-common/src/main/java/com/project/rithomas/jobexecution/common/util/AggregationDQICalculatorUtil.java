package com.project.rithomas.jobexecution.common.util;


import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class AggregationDQICalculatorUtil {

	private static final String REAGG_SUFFIX = "_ReaggregateJob";

	private static final String AGG_SUFFIX = "_AggregateJob";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AggregationDQICalculatorUtil.class);

	public static String getQueryToGetSource(String regionId, String jobName) {
		String aggJobName = jobName.replace(REAGG_SUFFIX, AGG_SUFFIX);
		String queryToGetSource = QueryConstants.GET_SOURCE_TABLE_NAME_ADAP
				.replace(QueryConstants.JOB_ID, aggJobName);
		if (regionId != null) {
			queryToGetSource = QueryConstants.GET_SOURCE_TABLE_NAME_ADAP_TZ
					.replace(QueryConstants.JOB_ID, aggJobName)
					.replace(QueryConstants.REGION_ID, regionId);
		}
		return queryToGetSource;
	}

	public static String getSourceTableNameList(WorkFlowContext context,
			QueryExecutor queryExecutor, List<String> sourceJobList)
			throws JobExecutionException {
		StringBuffer sourceTableNameListStrBuf = new StringBuffer("");
		int i = 1;
		for (String sourceJobName : sourceJobList) {
			String srcTableNameQuery = "select table_name from job_list where jobid ='"
					+ sourceJobName + "'";
			Object[] srcTableNameArr = queryExecutor
					.executeMetadatasqlQuery(srcTableNameQuery, context);
			sourceTableNameListStrBuf = sourceTableNameListStrBuf
					.append((String) srcTableNameArr[0]);
			if (i < sourceJobList.size()) {
				sourceTableNameListStrBuf = sourceTableNameListStrBuf
						.append(",");
			}
			i++;
		}
		LOGGER.debug("source table name list for the aggregation : {}",
				sourceTableNameListStrBuf.toString());
		return sourceTableNameListStrBuf.toString();
	}

	public static boolean isMultipleSource(WorkFlowContext context) {
		boolean multipleSources = false;
		String sourceTableNames = (String) context
				.getProperty(JobExecutionContext.SOURCE);
		LOGGER.debug("Source Table Name : {}", sourceTableNames);
		if (sourceTableNames.contains(",")) {
			multipleSources = true;
		}
		return multipleSources;
	}
}

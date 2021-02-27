
package com.project.rithomas.jobexecution.common.util;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class UsageAggregationStatusUtil {

	public Long getAggregatedRecordCount(WorkFlowContext context,
			String usageJobId, String perfJobId, String reportTime,
			String upperBound, String regionId) throws JobExecutionException {
		return getRecordCount(context, usageJobId, perfJobId, reportTime,
				upperBound, regionId, "true");
	}

	public Long getNonAggregatedRecordCount(WorkFlowContext context,
			String usageJobId, String perfJobId, String reportTime,
			String upperBound, String regionId) throws JobExecutionException {
		return getRecordCount(context, usageJobId, perfJobId, reportTime,
				upperBound, regionId, "false");
	}
	
	private Long getRecordCount(WorkFlowContext context,
			String usageJobId, String perfJobId, String reportTime,
			String upperBound, String regionId, String isAggregated) throws JobExecutionException {
		Long recordCount = 0l;
		String query = null;
		if (regionId != null) {
			query = QueryConstants.GET_AGGREGATED_RECORD_COUNT_TZ
					.replace(QueryConstants.REGION_ID, regionId);
		} else {
			query = QueryConstants.GET_AGGREGATED_RECORD_COUNT;
		}
		query = query.replace(QueryConstants.USAGE_JOB_ID, usageJobId)
				.replace(QueryConstants.PERF_JOB_ID, perfJobId)
				.replace(QueryConstants.REPORT_TIME, reportTime)
				.replace(QueryConstants.UPPER_BOUND, upperBound)
				.replace(QueryConstants.STATUS, isAggregated);
		QueryExecutor queryExecutor = new QueryExecutor();
		Object[] data = queryExecutor.executeMetadatasqlQuery(query, context);
		if (data != null && data.length > 0) {
			recordCount = Long.parseLong(data[0].toString());
		}
		return recordCount;
	}

	public void insertIntoUsageAggList(WorkFlowContext context,
			String usageJobId, String perfJobId, String reportTime,
			Long numberOfRecords, String regionId)
			throws JobExecutionException {
		String query = null;
		if (regionId != null) {
			query = QueryConstants.INSERT_INTO_USAGE_AGG_STATUS_WITH_REGION
					.replace(QueryConstants.REGION_ID, regionId);
		} else {
			query = QueryConstants.INSERT_INTO_USAGE_AGG_STATUS;
		}
		query = query.replace(QueryConstants.USAGE_JOB_ID, usageJobId)
				.replace(QueryConstants.PERF_JOB_ID, perfJobId)
				.replace(QueryConstants.REPORT_TIME, reportTime)
				.replace(QueryConstants.NUMBER_OF_RECORDS,
						numberOfRecords.toString());
		QueryExecutor queryExecutor = new QueryExecutor();
		queryExecutor.executePostgresqlUpdate(query, context);
	}

	public void updateUsageAggList(WorkFlowContext context, String perfJobId,
			String reportTime, String upperBound, String regionId)
			throws JobExecutionException {
		if (JobTypeDictionary.USAGE_JOB_TYPE.equals(
				context.getProperty(JobExecutionContext.SOURCEJOBTYPE))) {
			String query = null;
			if (regionId != null) {
				query = QueryConstants.UPDATE_USAGE_AGG_STATUS_TZ
						.replace(QueryConstants.REGION_ID, regionId);
			} else {
				query = QueryConstants.UPDATE_USAGE_AGG_STATUS;
			}
			query = query.replace(QueryConstants.PERF_JOB_ID, perfJobId)
					.replace(QueryConstants.REPORT_TIME, reportTime)
					.replace(QueryConstants.UPPER_BOUND, upperBound);
			QueryExecutor queryExecutor = new QueryExecutor();
			queryExecutor.executePostgresqlUpdate(query, context);
		}
	}
}


package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ReaggregationListUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggregationListUtil.class);

	public boolean recordExists(WorkFlowContext context,
			String requestorJobName, String providerJobName, String reportTime,
			String regionId) throws JobExecutionException {
		boolean recordExists = false;
		String queryToCheck = null;
		QueryExecutor queryExecutor = new QueryExecutor();
		if (regionId != null && !regionId.isEmpty()) {
			queryToCheck = QueryConstants.CHECK_REAGG_LIST_WITH_REGION
					.replace(QueryConstants.REGION_ID, regionId);
		} else {
			queryToCheck = QueryConstants.CHECK_REAGG_LIST;
		}
		queryToCheck = queryToCheck
				.replace(QueryConstants.REQUESTOR_JOB, requestorJobName)
				.replace(QueryConstants.PROVIDER_JOB, providerJobName)
				.replace(QueryConstants.REPORT_TIME, reportTime);
		LOGGER.debug("Query to check if entry is there in reagg list table: {}",
				queryToCheck);
		Object[] data = queryExecutor.executeMetadatasqlQuery(queryToCheck,
				context);
		if (data != null && data.length > 0) {
			recordExists = true;
		}
		return recordExists;
	}

	private boolean updateReaggregationList(WorkFlowContext context,
			String targetJobName, String sourceJobNames, String reportTime,
			String newStatus, String oldStatuses) throws JobExecutionException {
		String updateSql = null;
		if (reportTime == null) {
			updateSql = QueryConstants.REAGG_LIST_UPDATE_PREV_STATUS
					.replace(QueryConstants.OLD_STATUS, oldStatuses);
		} else {
			updateSql = QueryConstants.REAGG_LIST_UPDATE
					.replace(QueryConstants.REPORT_TIME, reportTime)
					.replace(QueryConstants.OLD_STATUS, oldStatuses);
		}
		updateSql = updateSql.replace(QueryConstants.STATUS, newStatus)
				.replace(QueryConstants.SOURCE_JOB_NAMES, sourceJobNames)
				.replace(QueryConstants.PROVIDER_JOB, targetJobName);
		LOGGER.debug("Reaggregation list update query: {}", updateSql);
		QueryExecutor queryExecutor = new QueryExecutor();
		queryExecutor.executePostgresqlUpdate(updateSql, context);
		return true;
	}

	public boolean updateReaggregationList(WorkFlowContext context,
			String targetJobName, String sourceJobNames, String reportTime,
			String newStatus, String oldStatuses, String regionId)
			throws JobExecutionException {
		String oldStatusesWithQuote = "'"+oldStatuses+"'";
		if (regionId == null) {
			return updateReaggregationList(context, targetJobName,
					sourceJobNames, reportTime, newStatus, oldStatusesWithQuote);
		}
		String updateSql = null;
		if (reportTime == null) {
			updateSql = QueryConstants.REAGG_LIST_UPDATE_PREV_STATUS_TZ
					.replace(QueryConstants.OLD_STATUS, oldStatusesWithQuote);
		} else {
			updateSql = QueryConstants.REAGG_LIST_UPDATE_TZ
					.replace(QueryConstants.REPORT_TIME, reportTime)
					.replace(QueryConstants.OLD_STATUS, oldStatusesWithQuote);
		}
		updateSql = updateSql.replace(QueryConstants.STATUS, newStatus)
				.replace(QueryConstants.SOURCE_JOB_NAMES, sourceJobNames)
				.replace(QueryConstants.PROVIDER_JOB, targetJobName)
				.replace(QueryConstants.REGION_ID, regionId);
		LOGGER.debug("Reaggregation list update query: {}", updateSql);
		QueryExecutor queryExecutor = new QueryExecutor();
		queryExecutor.executePostgresqlUpdate(updateSql, context);
		return true;
	}

	public boolean insertIntoReaggList(WorkFlowContext context, String loadTime,
			String reportTime, String providerJobAggLevel, String providerJob,
			String requestorJob, String regionId, String status)
			throws JobExecutionException {
		String insertQuery = null;
		LOGGER.debug(
				"Inserting into reagg list table with report_time: {}, agg level: {}, provider: {}, requestor: {}",
				reportTime, providerJobAggLevel, providerJob, requestorJob);
		if (regionId != null && !regionId.isEmpty()) {
			insertQuery = QueryConstants.INSERT_INTO_REAGG_LIST_WITH_REGION
					.replace(QueryConstants.REGION_ID, regionId);
		} else {
			insertQuery = QueryConstants.INSERT_INTO_REAGG_LIST;
		}
		insertQuery = insertQuery.replace(QueryConstants.LOAD_TIME, loadTime)
				.replace(QueryConstants.REPORT_TIME, reportTime)
				.replace(QueryConstants.ALEVEL, providerJobAggLevel)
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.REQUESTOR_JOB, requestorJob)
				.replace(QueryConstants.STATUS, status);
		QueryExecutor queryExecutor = new QueryExecutor();
		queryExecutor.executePostgresqlUpdate(insertQuery, context);
		return true;
	}

	public List<String> getReaggregatedReportTimes(WorkFlowContext context,
			String providerJob, String sourceJobNames)
			throws JobExecutionException {
		List<String> reportTimeList = new ArrayList<String>();
		String partCompletedQuery = QueryConstants.GET_REPORT_TIMES
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.SOURCE_JOB_NAMES, sourceJobNames);
		LOGGER.debug("Reaggregation list report time query: {}",
				partCompletedQuery);
		QueryExecutor queryExecutor = new QueryExecutor();
		List data = queryExecutor
				.executeMetadatasqlQueryMultiple(partCompletedQuery, context);
		if (data != null && !data.isEmpty()) {
			for (Object dataItr : data) {
				reportTimeList.add(dataItr.toString());
			}
		}
		return reportTimeList;
	}

	public List getPendingReportTimes(WorkFlowContext context, String jobId,
			String truncatedReportTime, String nextReportTime)
			throws JobExecutionException {
		String pendingStatusQuery = QueryConstants.GET_PENDING_REAGG_LIST
				.replace(QueryConstants.JOB_ID, jobId)
				.replace(QueryConstants.LOWER_BOUND, truncatedReportTime)
				.replace(QueryConstants.UPPER_BOUND, nextReportTime);
		LOGGER.debug("Reaggregation list pending/initialized status query: {}",
				pendingStatusQuery);
		QueryExecutor queryExecutor = new QueryExecutor();
		return queryExecutor.executeMetadatasqlQueryMultiple(pendingStatusQuery,
				context);
	}

	public boolean isAnyReaggDependentJobsActive(WorkFlowContext context,
			int numberOfDays) throws JobExecutionException {
		LOGGER.debug(
				"Checking for active dependent re-aggregation jobs beyond retention..");
		boolean isAnyReaggDependentJobsActive = true;
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String reportTime = DateFunctionTransformation.getInstance()
				.getFormattedDate(DateFunctionTransformation
						.getDateBySubtractingDays(numberOfDays, context)
						.getTime());
		String sql = String.format(QueryConstants.ACTIVE_DEPENDENT_REAGG_JOBS,
				jobName, reportTime);
		LOGGER.debug("Dependent Reagg job query: {}",sql);
		QueryExecutor executor = new QueryExecutor();
		List data = executor.executeMetadatasqlQueryMultiple(sql, context);
		if(CollectionUtils.isEmpty(data)) {
			isAnyReaggDependentJobsActive=false;
		}
		LOGGER.info("isAnyReaggDependentJobsActive: {}",isAnyReaggDependentJobsActive);
		return isAnyReaggDependentJobsActive;
	}

	public boolean isJobDisabled(WorkFlowContext context, String providerAggJob,
			String providerJob) throws JobExecutionException {
		boolean isJobDisabled = false;
		String jobsEnableQuery = QueryConstants.GET_JOB_ENABLED_QUERY
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.PROVIDER_AGG_JOB, providerAggJob);
		LOGGER.debug(
				"Query to check whether the aggregation/Re-aggregation jobs are disabled or not : {}",
				jobsEnableQuery);
		QueryExecutor queryExecutor = new QueryExecutor();
		List data = queryExecutor
				.executeMetadatasqlQueryMultiple(jobsEnableQuery, context);
		if (CollectionUtils.isNotEmpty(data)) {
			for (Object record : data) {
				Object[] recordValues = (Object[]) record;
				String paramVal = recordValues[1] != null
						? recordValues[1].toString() : "";
				if ("NO".equalsIgnoreCase(paramVal)) {
					isJobDisabled = true;
					break;
				}
			}
		}
		LOGGER.info("Is {} disabled : {}", providerJob, isJobDisabled);
		return isJobDisabled;
	}
}

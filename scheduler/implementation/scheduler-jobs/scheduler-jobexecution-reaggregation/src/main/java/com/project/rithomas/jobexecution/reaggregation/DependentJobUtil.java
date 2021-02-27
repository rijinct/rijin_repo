
package com.project.rithomas.jobexecution.reaggregation;

import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class DependentJobUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DependentJobUtil.class);

	@SuppressWarnings("rawtypes")
	public static void checkForDependentJobs(String dependentJobQuery,
			String sourceJobsInQuotes, WorkFlowContext context)
			throws JobExecutionException, WorkFlowExecutionException {
		LOGGER.debug("Dependent job query single: {}", dependentJobQuery);
		QueryExecutor queryExecutor = new QueryExecutor();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		List dependentJobs = queryExecutor
				.executeMetadatasqlQueryMultiple(dependentJobQuery, context);
		if (CollectionUtils.isNotEmpty(dependentJobs)) {
			for (Object itObj : dependentJobs) {
				Object[] data = (Object[]) itObj;
				String maxValue = data[2] != null ? data[2].toString() : null;
				String distinctReaggQuery = QueryConstants.GET_DISTINCT_REAGG_LIST
						.replace(QueryConstants.PROVIDER_JOB, jobId)
						.replace(QueryConstants.SOURCE_JOB_NAMES,
								sourceJobsInQuotes)
						.replace(QueryConstants.LOAD_TIME,
								context.getProperty(
										JobExecutionContext.MIN_LOAD_TIME)
										.toString());
				LOGGER.debug("Distinct reagg list query: {}",
						distinctReaggQuery);
				List distinctReaggList = queryExecutor
						.executeMetadatasqlQueryMultiple(distinctReaggQuery,
								context);
				if (maxValue != null
						&& CollectionUtils.isNotEmpty(distinctReaggList)) {
					updateReaggListTable(context, maxValue, data,
							distinctReaggList);
				} else {
					LOGGER.debug(
							"Either max value is null or distinct reagg list is empty");
				}
			}
		} else {
			LOGGER.info(
					"No dependent re-aggregation jobs to be registered in reagg-list");
		}
	}

	private static void updateReaggListTable(WorkFlowContext context,
			String maxValue, Object[] data, List distinctReaggList)
			throws WorkFlowExecutionException, JobExecutionException {
		ReaggregationListUtil reaggListUtil = new ReaggregationListUtil();
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String providerJob = data[0].toString();
		String providerJobAggLevel = data[1].toString();
		String providerAggJob = data[3] != null ? data[3].toString() : null;
		LOGGER.debug("Max value: {}, Agg level: {}", maxValue,
				providerJobAggLevel);
		for (Object reaggListIt : distinctReaggList) {
			Object[] reaggListObj = (Object[]) reaggListIt;
			Long reportTimeInMillis = null;
			String reportTime = reaggListObj[2] != null
					? reaggListObj[2].toString() : null;
			String regionId = null;
			if (reaggListObj[3] != null) {
				boolean isTimeZoneAgnostic = TimeZoneUtil.isTimeZoneAgnostic(
						context, providerJobAggLevel, providerAggJob);
				regionId = isTimeZoneAgnostic
						? JobExecutionContext.DEFAULT_TIMEZONE
						: reaggListObj[3].toString();
			}
			Long maxValueInMillis = dateTransformer.getDate(maxValue).getTime();
			String truncatedReportTime = null;
			String nextReportTime = null;
			if (reportTime != null) {
				reportTimeInMillis = dateTransformer.getTrunc(
						dateTransformer.getDate(reportTime).getTime(),
						providerJobAggLevel, (String) context.getProperty(
								JobExecutionContext.WEEK_START_DAY));
				truncatedReportTime = dateTransformer
						.getFormattedDate(reportTimeInMillis);
				nextReportTime = dateTransformer.getFormattedDate(
						dateTransformer.getNextBound(reportTimeInMillis,
								providerJobAggLevel).getTime());
			}
			List pendingStatusList = reaggListUtil.getPendingReportTimes(
					context, jobId, truncatedReportTime, nextReportTime);
			LOGGER.debug("Pending status list: {}", pendingStatusList);
			Long retentionDayValue = ReaggCommonUtil
					.getRetentionDayValue(context);
			LOGGER.debug("Report time from distinct reagg list: {}",
					reportTime);
			LOGGER.debug("Report time calculated: {}, retention day value: {}",
					truncatedReportTime, retentionDayValue);
			if (reportTimeInMillis != null
					&& reportTimeInMillis <= maxValueInMillis
					&& CollectionUtils.isEmpty(pendingStatusList)) {
				if (reportTimeInMillis >= retentionDayValue
						&& !reaggListUtil.recordExists(context, jobId,
								providerJob, truncatedReportTime, regionId)
						&& !reaggListUtil.isJobDisabled(context, providerAggJob,
								providerJob)) {
					reaggListUtil.insertIntoReaggList(context,
							dateTransformer.getFormattedDate(new Date()),
							truncatedReportTime, providerJobAggLevel,
							providerJob, jobId, regionId,
							CommonConstants.INITIALIZED_STATUS);
				} else {
					reaggListUtil.updateReaggregationList(context, providerJob,
							"'" + jobId + "'", truncatedReportTime,
							CommonConstants.INITIALIZED_STATUS,
							CommonConstants.PENDING_STATUS, regionId);
				}
			}
		}
	}
}

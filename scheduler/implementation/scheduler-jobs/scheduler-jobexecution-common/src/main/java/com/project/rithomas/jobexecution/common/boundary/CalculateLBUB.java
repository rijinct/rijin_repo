
package com.project.rithomas.jobexecution.common.boundary;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.TimeZoneConverter;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class CalculateLBUB extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CalculateLBUB.class);

	private JobPropertyRetriever jobPropertyRetriever = null;

	private JobDetails jobDetails = null;

	private static final String EITHER_OF_BOUNDARY_VALUE_NULL = "Either of the boundary value is null.Hence not aggregating further";

	protected String sourcePlevel = null;

	private int numOfRegionsWithNullBoundary = 0;

	private Map<String, Long> leastReportTimeRegionMap = new HashMap<>();
	
	private TimeZoneConverter timeZoneConverter = new TimeZoneConverter();

	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		jobPropertyRetriever = new JobPropertyRetriever(context);
		jobDetails = new JobDetails(context);
		boolean isSuccess = false;
		final BoundaryQuery boundaryQuery = new BoundaryQuery();
		BoundaryCalculator boundaryCalculator = BoundaryCalculatorFactory
				.getBoundaryCalculatorInstance(context);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS,
				jobPropertyRetriever.getAggRetentionDays());
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		Timestamp maxValue = null;
		if (TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)) {
			context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 0);
			List<String> timeZoneRegions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			numOfRegionsWithNullBoundary = 0;
			isSuccess = calculateBoundaryForEachRegion(context, boundaryQuery,
					boundaryCalculator, jobName, updateJobStatus,
					timeZoneRegions);
			if (numOfRegionsWithNullBoundary == timeZoneRegions.size()) {
				context.setProperty(JobExecutionContext.RETURN, 1);
				context.setProperty(JobExecutionContext.DESCRIPTION,
						EITHER_OF_BOUNDARY_VALUE_NULL);
			}
		} else {
			maxValue = boundaryCalculator.getMaxValueOfJob(boundaryQuery,
					jobName, null);
			List<Boundary> srcBoundaryList = new ArrayList<>();
			for (String sourceJob : jobDetails.getSourceJobList(null)) {
				srcBoundaryList
						.addAll(boundaryQuery.retrieveByJobId(sourceJob));
			}
			jobDetails.setMaxValue(null, maxValue);
			try {
				isSuccess = setBoundaryValuesAndCheckForDST(context,
						boundaryCalculator, null, srcBoundaryList);
			} catch (WorkFlowExecutionException e) {
				setErrorInContextAndThrowException(context, e);
			} finally {
				if (!isSuccess) {
					updateJobStatus.updateFinalJobStatus(context);
				}
			}
		}
		return isSuccess;
	}

	private boolean calculateBoundaryForEachRegion(WorkFlowContext context,
			final BoundaryQuery boundaryQuery,
			BoundaryCalculator boundaryCalculator, String jobName,
			UpdateJobStatus updateJobStatus, List<String> timeZoneRegions)
			throws WorkFlowExecutionException {
		boolean isSuccess = false;
		Timestamp maxValue;
		for (String region : timeZoneRegions) {
			boundaryCalculator.setFirstRun(false);
			maxValue = boundaryCalculator.getMaxValueOfJob(boundaryQuery,
					jobName, region);
			jobDetails.setMaxValue(region, maxValue);
			try {
				LOGGER.info("Calculating LB UB for region: {}", region);
				final List<Boundary> srcBoundaryList = new ArrayList<>();
				List<String> modifiedSrcJobList = modifySourceJobListForRegion(
						context, region);
				for (String sourceJob : modifiedSrcJobList) {
					LOGGER.debug("source job:{}", sourceJob);
					List<Boundary> boundaryWithRgn = boundaryQuery
							.retrieveByJobIdAndRegionId(sourceJob, region);
					if (CollectionUtils.isNotEmpty(boundaryWithRgn)) {
						srcBoundaryList.add(boundaryWithRgn.get(0));
					} else {
						boundaryCalculator
								.setValidForAggregationForMultipleSrc(false);
						break;
					}
				}
				jobDetails.setSourceJobList(region, modifiedSrcJobList);
				isSuccess = setBoundaryValuesAndCheckForDST(context,
						boundaryCalculator, region, srcBoundaryList);
			} catch (WorkFlowExecutionException e) {
				setErrorInContextAndThrowException(context, e);
			} finally {
				if (!isSuccess) {
					updateJobStatus.updateFinalJobStatus(context);
				}
			}
		}
		return isSuccess;
	}

	private boolean setBoundaryValuesAndCheckForDST(WorkFlowContext context,
			BoundaryCalculator boundaryCalculator, String region,
			final List<Boundary> srcBoundaryList)
			throws WorkFlowExecutionException {
		try {
			jobDetails.setSourceBoundList(region, srcBoundaryList);
			Long finalLb = getFinalLB(context, region,
					boundaryCalculator.calculateLB(region));
			updateForFirstTimeExecution(context, finalLb, region);
			logBoundaryValuesAndCheckForDST(context, boundaryCalculator,
					finalLb, boundaryCalculator.calculateUB(region), region);
		} catch (JobExecutionException | WorkFlowExecutionException e) {
			setErrorInContextAndThrowException(context, e);
		}
		return true;
	}

	private void setErrorInContextAndThrowException(WorkFlowContext context,
			Exception e) throws WorkFlowExecutionException {
		LOGGER.error(" Exception {}", e);
		context.setProperty(JobExecutionContext.STATUS, "E");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				e.getMessage());
		throw new WorkFlowExecutionException("Exception: " + e.getMessage(), e);
	}

	private Long getFinalLB(WorkFlowContext context, String region,
			Calendar calendarLB) throws WorkFlowExecutionException {
		Long finalLb = DateFunctionTransformation.getInstance().getTrunc(
				calendarLB.getTimeInMillis(),
				(String) context.getProperty(JobExecutionContext.ALEVEL),
				(String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY),
				TimeZoneUtil.getZoneId(context, region));
		setLBInContext(context, region, finalLb, JobExecutionContext.LB);
		return finalLb;
	}

	private void setLBInContext(WorkFlowContext context, String region,
			Long finalLb, String keyForLB) throws WorkFlowExecutionException {
		String propertyLB = TimeZoneUtil.isTimeZoneEnabled(context)
				&& !TimeZoneUtil.isTimeZoneAgnostic(context)
						? new StringBuilder(keyForLB).append("_")
								.append(region).toString()
						: keyForLB;
		context.setProperty(propertyLB, finalLb);
	}

	private void updateForFirstTimeExecution(WorkFlowContext context,
			Long finalLb, String region) {
		if (!"YES".equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC))) {
			if (region != null) {
				leastReportTimeRegionMap.put(region, finalLb);
				context.setProperty(
						JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION,
						leastReportTimeRegionMap);
			}
			jobDetails.setLBINIT(region, finalLb);
		}
	}

	private void logBoundaryValuesAndCheckForDST(WorkFlowContext context,
			BoundaryCalculator boundaryCalculator, Long finalLb,
			Calendar calendarUB, String region)
			throws JobExecutionException, WorkFlowExecutionException {
		boolean conditionToEvaluate = false;
		if (region != null) {
			conditionToEvaluate = !boundaryCalculator.getFirstRun()
					&& boundaryCalculator.isValidForAggregationForMultipleSrc();
		} else {
			conditionToEvaluate = !boundaryCalculator.getFirstRun();
		}
		if (conditionToEvaluate) {
			if (!jobDetails.isSourceJobTypeUsage()) {
				calendarUB = DateFunctionTransformation.getInstance()
						.getNextBound(calendarUB.getTimeInMillis(),
								boundaryCalculator.getSourcePlevel());
			}
			Calendar calendarLB = new GregorianCalendar();
			calendarLB.setTimeInMillis(finalLb);
			Long finalUb = calendarUB.getTimeInMillis();
			String calendarLBLocalTime = "";
			String calendarUBLocalTime = "";
			if (TimeZoneUtil.isSystemInUTC()) {
				String finalUBLtz = timeZoneConverter
						.dateTime(getFormattedDate(calendarUB.getTime()))
						.from(TimeZoneConverter.UTC)
						.to(TimeZoneUtil.getZoneId(context, region)).convert();
				String finalLBLtz = timeZoneConverter
						.dateTime(getFormattedDate(calendarLB.getTime()))
						.from(TimeZoneConverter.UTC)
						.to(TimeZoneUtil.getZoneId(context, region)).convert();
				LOGGER.debug("calendarUB:{} and finalUBLocalTime:{} ", finalUb,
						finalUBLtz);
				calendarLBLocalTime = "and in localTime is:" + finalLBLtz;
				calendarUBLocalTime = "and in localTime is:" + finalUBLtz;
			}
			if (region != null) {
				boundaryCalculator.getMaxReportTimeRegionMap().put(region,
						finalUb);
				context.setProperty(
						JobExecutionContext.MAX_REPORT_TIME_FOR_REGION,
						boundaryCalculator.getMaxReportTimeRegionMap());
			}
			jobDetails.setUpperBound(region, finalUb);
			LOGGER.debug("Checking if UB is falling in DST or not");
			isUpperBoundFallsInDST(context, calendarUB, region);
			Calendar calendarNB = DateFunctionTransformation.getInstance()
					.getNextBound(finalLb, (String) context
							.getProperty(JobExecutionContext.PLEVEL));
			Long nextLb = calendarNB.getTimeInMillis();
			jobDetails.setNextLB(region, nextLb);
			LOGGER.info(
					"LowerBound value is :{}  {} ",
					getFormattedDate(calendarLB.getTime()),calendarLBLocalTime);
			LOGGER.info("UpperBound value is : {} {}",
					getFormattedDate(calendarUB.getTime()), calendarUBLocalTime);
			LOGGER.info("Nextlowerbound value is : {}",
					getFormattedDate(calendarNB.getTime()));
			context.setProperty(JobExecutionContext.RETURN, 0);
			LBUBUtil.lbubChecks(context, region);
		} else {
			if (region != null) {
				LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL + " for region {}",
						region);
				numOfRegionsWithNullBoundary++;
			} else {
				LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL);
				context.setProperty(JobExecutionContext.RETURN, 1);
				context.setProperty(JobExecutionContext.DESCRIPTION,
						EITHER_OF_BOUNDARY_VALUE_NULL);
			}
		}
	}

	private String getFormattedDate(Date dateTime) {
		return DateFunctionTransformation.getInstance()
				.getFormattedDate(dateTime);
	}

	private void isUpperBoundFallsInDST(WorkFlowContext context,
			Calendar calendarUB, String region) throws JobExecutionException {
		Long calendarUBForDST = null;
		if (jobPropertyRetriever.isPlevelHourOr15Min() && !TimeZoneUtil.isSystemInUTC()) {
			calendarUBForDST = DateFunctionTransformation.getInstance()
					.getTrunc(calendarUB.getTimeInMillis(),
							JobExecutionContext.HOUR);
			DayLightSavingUtil dayLightSavingUtil = new DayLightSavingUtil();
			Calendar truncCalUBForDST = new GregorianCalendar();
			truncCalUBForDST.setTimeInMillis(calendarUBForDST);
			int ubDSTCnt = dayLightSavingUtil
					.getCntIfUBInTimeZoneDST(truncCalUBForDST, region, context);
			jobDetails.setUbDstCount(region, ubDSTCnt);
		}
	}

	private List<String> modifySourceJobListForRegion(WorkFlowContext context,
			String region) throws WorkFlowExecutionException {
		List<String> sourceJobIdList = jobDetails.getSourceJobList(null);
		List<String> modifiedSrcJobList = sourceJobIdList;
		if (jobDetails.getSourceJobType()
				.equalsIgnoreCase(JobTypeDictionary.PERF_JOB_TYPE)) {
			ModifySourceJobList modifySourceJobList = new ModifySourceJobList(
					context, region, sourceJobIdList);
			modifiedSrcJobList = new ArrayList<>(
					modifySourceJobList.modifySourceJobListForAgg());
			LOGGER.info("Source joblist for region {} is: {}", region,
					modifiedSrcJobList);
		}
		return modifiedSrcJobList;
	}
}


package com.project.rithomas.jobexecution.common.boundary;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.PerfInterval;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.utils.DateCopyProvider;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public abstract class BoundaryCalculator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(BoundaryCalculator.class);

	protected boolean validForAggregationForMultipleSrc = true;

	private Timestamp maxValue = null;

	private Map<String, Long> maxReportTimeRegionMap = new HashMap<>();

	protected static final String PLEVEL = "_PLEVEL";

	private static final String MIN_15 = "MIN_15";

	protected WorkFlowContext context;

	protected Map<String, String> plevelMap = new HashMap<>();

	protected String sourcePlevel = null;

	protected boolean firstRun = false;

	protected JobPropertyRetriever jobPropertyRetriever;

	protected RuntimePropertyRetriever runTimePropertyRetriever;

	protected JobDetails jobDetails;

	public BoundaryCalculator(WorkFlowContext workFlowContext) {
		this.context = workFlowContext;
		jobPropertyRetriever = new JobPropertyRetriever(this.context);
		runTimePropertyRetriever = new RuntimePropertyRetriever(this.context);
		jobDetails = new JobDetails(workFlowContext);
	}

	public abstract Calendar calculateUB(String region)
			throws WorkFlowExecutionException, JobExecutionException;

	public Timestamp getMaxValueOfJob(BoundaryQuery boundaryQuery,
			String jobName, String regionId) {
		List<Boundary> boundaryList = null;
		if (StringUtils.isNotEmpty(regionId)) {
			boundaryList = boundaryQuery.retrieveByJobIdAndRegionId(jobName,
					regionId);
		} else {
			boundaryList = boundaryQuery.retrieveByJobId(jobName);
		}
		LOGGER.debug("Max Value of the Job: {} is: {}", jobName, boundaryList);
		Boundary boundary = boundaryList.get(0);
		return boundary.getMaxValue();
	}

	public Calendar calculateLB(String regionId)
			throws WorkFlowExecutionException {
		Calendar calendar = new GregorianCalendar();
		Calendar calendarLB = new GregorianCalendar();
		int retDays = jobPropertyRetriever.getAggregationRetentionDays();
		calendar.add(Calendar.DAY_OF_MONTH, -retDays);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendarLB = LBUBUtil.getCalendarLb(context,
				getMaxValueString(regionId), calendar, calendarLB);
		calendarLB = DateFunctionTransformation.getInstance().getNextBound(
				calendarLB.getTimeInMillis(),
				jobPropertyRetriever.getAggregationLevel());
		if (calendar.getTimeInMillis() > calendarLB.getTimeInMillis()) {
			LOGGER.info(
					"Boundary Value : {}, is lesser than the boundary calculated based on retention days({}) : {}. "
							+ "So taking Lower bound based on Retention days configured.",
					calendarLB.getTimeInMillis(), retDays,
					calendar.getTimeInMillis());
			calendarLB.setTimeInMillis(calendar.getTimeInMillis());
		}
		return calendarLB;
	}

	private String getMaxValueString(String regionId)
			throws WorkFlowExecutionException {
		StringBuilder maxValue = new StringBuilder(
				JobExecutionContext.MAXVALUE);
		if (TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)) {
			maxValue = maxValue.append("_").append(regionId);
		}
		return maxValue.toString();
	}

	protected Calendar getSourceBoundaryValue(String regionId)
			throws WorkFlowExecutionException {
		Calendar calendarUB = null;
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		List<Boundary> boundaryList = null;
		if (TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)) {
			boundaryList = boundaryQuery.retrieveByJobIdAndRegionId(
					jobDetails.getSourceJobName(), regionId);
		} else {
			boundaryList = retrieveBoundaryOfSrcJob(context, boundaryQuery);
		}
		if (CollectionUtils.isNotEmpty(boundaryList)) {
			calendarUB = getSourceMaxValue(context, boundaryList);
		} else {
			calendarUB = calculateBoundaryIfBoundaryNullForTz(context,
					boundaryQuery);
		}
		return calendarUB;
	}

	protected boolean isJobRunningForFirstTime(WorkFlowContext context) {
		if (!TimeZoneUtil.isTimeZoneEnabled(context)) {
			context.setProperty(JobExecutionContext.RETURN, 1);
		}
		LOGGER.info("All available sources are not sending data for Job");
		return true;
	}

	protected List<Boundary> retrieveBoundaryOfSrcJob(WorkFlowContext context,
			BoundaryQuery boundaryQuery) {
		return boundaryQuery.retrieveByJobId((String) context
				.getProperty(JobExecutionContext.SOURCE_JOB_NAME));
	}

	private Calendar getSourceMaxValue(WorkFlowContext context,
			List<Boundary> boundaryList) {
		Calendar sourceMaxValue = new GregorianCalendar();
		Boundary boundary = boundaryList.get(0);
		LOGGER.debug("job id {} region {} maxvalue is: {}",
				new Object[] { boundary.getJobId(), boundary.getRegionId(),
						boundary.getMaxValue() });
		if (boundary.getMaxValue() != null) {
			if (boundary.getMaxValue().getTime() <= sourceMaxValue
					.getTimeInMillis()) {
				sourceMaxValue.setTime(boundary.getMaxValue());
				sourcePlevel = (String) context
						.getProperty(boundary.getJobId() + PLEVEL);
			}
		} else {
			sourceMaxValue = null;
		}
		return sourceMaxValue;
	}

	private Calendar calculateBoundaryIfBoundaryNullForTz(
			WorkFlowContext context, BoundaryQuery boundaryQuery)
			throws WorkFlowExecutionException {
		Calendar calendarUB = null;
		if (TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)) {
			List<Boundary> boundaryList = retrieveBoundaryOfSrcJob(context,
					boundaryQuery);
			if (CollectionUtils.isNotEmpty(boundaryList)) {
				calendarUB = getSourceMaxValue(context, boundaryList);
			} else {
				calendarUB = null;
			}
		}
		return calendarUB;
	}

	protected List<Long> getListOfAllIntervals(Long lowerBound,
			Long maxUpperBound, String pLevel) {
		List<Long> listOfUbVal = new ArrayList<>();
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		String finalPLevel = JobExecutionContext.MIN_15.equalsIgnoreCase(pLevel)
				? MIN_15 : pLevel;
		while (lowerBound <= maxUpperBound
				&& lowerBound <= dateTransformer.getSystemTime()) {
			listOfUbVal.add(lowerBound);
			lowerBound = lowerBound + PerfInterval
					.valueOf(finalPLevel.toUpperCase()).convertToSeconds();
		}
		return listOfUbVal;
	}

	protected Long getLB(String regionId) {
		return StringUtils.isNotEmpty(regionId)
				? (Long) context.getProperty(
						new StringBuilder(JobExecutionContext.LB).append("_")
								.append(regionId).toString())
				: (Long) context.getProperty(JobExecutionContext.LB);
	}

	private void checkIfSourceReportTimeIsLesserThanTarget(
			Calendar calendarUBForEachStageType, Calendar calendarUBTarget,
			String eachStageType) {
		if (calendarUBForEachStageType.getTimeInMillis() < calendarUBTarget
				.getTimeInMillis()) {
			LOGGER.info("No data available for {} ", eachStageType);
		}
	}

	private Calendar getUBForJob(Long finalLb, String plevel) {
		return DateFunctionTransformation.getInstance().getNextBound(finalLb,
				plevel);
	}

	protected Calendar getFinalUBOfSrcs(String regionId,
			List<String> listOfAllAvailableStageTypes,
			Map<String, Long> leastReportTimeMap) {
		Entry<String, Long> maxEntry = jobDetails
				.getLeastMaxValueOfSrcs(leastReportTimeMap);
		Calendar leastReportTimeCal = new GregorianCalendar();
		if (maxEntry != null) {
			Long ubValue = maxEntry.getValue();
			Calendar calendarUbVal = new GregorianCalendar();
			calendarUbVal.setTimeInMillis(ubValue);
			LOGGER.info(" Max UB  is {}", DateFunctionTransformation
					.getInstance().getFormattedDate(calendarUbVal.getTime()));
			Long upperThresholdCutOff = ubValue
					- runTimePropertyRetriever.getWaitTimeToProceedAgg();
			ubValue = checkLeastUB(context, regionId,
					listOfAllAvailableStageTypes, upperThresholdCutOff,
					leastReportTimeMap, ubValue);
			leastReportTimeCal.setTimeInMillis(ubValue);
			LOGGER.info(" Least UB Value for all types {}",
					DateFunctionTransformation.getInstance()
							.getFormattedDate(leastReportTimeCal.getTime()));
			Long truncDate = DateFunctionTransformation.getInstance().getTrunc(
					leastReportTimeCal.getTimeInMillis(),
					(String) context.getProperty(JobExecutionContext.PLEVEL),
					(String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY),
					TimeZoneUtil.getZoneId(context, regionId));
			leastReportTimeCal.setTimeInMillis(truncDate);
		} else {
			firstRun = isJobRunningForFirstTime(context);
		}
		return leastReportTimeCal;
	}

	private Long checkLeastUB(WorkFlowContext context, String regionId,
			List<String> listOfAllAvailableStageTypes,
			Long upperThresholdCutOff,
			Map<String, Long> reportTimeMapForAllStages,
			Long leastReportTimeOfSrc) {
		Long ubValue = leastReportTimeOfSrc;
		Calendar calendarUBForEachStageType = new GregorianCalendar();
		Calendar calendarUBTarget = getUBForJob(getLB(regionId),
				jobPropertyRetriever.getPartitionLevel());
		for (String eachStageType : listOfAllAvailableStageTypes) {
			if (reportTimeMapForAllStages.get(eachStageType) != null) {
				Long reportTimeForEachStageType = reportTimeMapForAllStages
						.get(eachStageType);
				calendarUBForEachStageType
						.setTimeInMillis(reportTimeForEachStageType);
				if (!JobTypeDictionary.USAGE_JOB_TYPE
						.equalsIgnoreCase((String) context.getProperty(
								JobExecutionContext.SOURCEJOBTYPE))) {
					calendarUBForEachStageType = getUBForJob(
							reportTimeForEachStageType,
							plevelMap.get(eachStageType));
				}
				LOGGER.info("Max Value for {} : {}", eachStageType,
						DateFunctionTransformation.getInstance()
								.getFormattedDate(
										calendarUBForEachStageType.getTime()));
				if (!isPlevelHourOr15Min(context)
						|| reportTimeForEachStageType >= upperThresholdCutOff) {
					if (reportTimeForEachStageType < ubValue) {
						ubValue = reportTimeForEachStageType;
					}
					checkIfSourceReportTimeIsLesserThanTarget(
							calendarUBForEachStageType, calendarUBTarget,
							eachStageType);
				} else {
					LOGGER.info(
							"{} sources have not loaded data beyond {}, hence not considered for UB calculation",
							eachStageType, new Date(upperThresholdCutOff));
				}
			} else {
				LOGGER.info("{} source is not sending data", eachStageType);
			}
		}
		return ubValue;
	}

	protected boolean isPlevelHourOr15Min(WorkFlowContext context) {
		String performanceLevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		return JobExecutionContext.MIN_15.equalsIgnoreCase(performanceLevel)
				|| JobExecutionContext.HOUR.equalsIgnoreCase(performanceLevel);
	}

	public boolean getFirstRun() {
		return firstRun;
	}

	public void setFirstRun(boolean firstRun) {
		this.firstRun = firstRun;
	}

	public String getSourcePlevel() {
		return sourcePlevel;
	}

	public Map<String, Long> getMaxReportTimeRegionMap() {
		return maxReportTimeRegionMap;
	}

	public void setMaxReportTimeRegionMap(
			Map<String, Long> maxReportTimeRegionMap) {
		this.maxReportTimeRegionMap = maxReportTimeRegionMap;
	}

	public boolean isValidForAggregationForMultipleSrc() {
		return validForAggregationForMultipleSrc;
	}

	public void setValidForAggregationForMultipleSrc(
			boolean validForAggregationForMultipleSrc) {
		this.validForAggregationForMultipleSrc = validForAggregationForMultipleSrc;
	}

	public Timestamp getMaxValue() {
		return DateCopyProvider.copyOfTimeStamp(maxValue);
	}

	public void setMaxValue(Timestamp maxValue) {
		this.maxValue = DateCopyProvider.copyOfTimeStamp(maxValue);
	}
}

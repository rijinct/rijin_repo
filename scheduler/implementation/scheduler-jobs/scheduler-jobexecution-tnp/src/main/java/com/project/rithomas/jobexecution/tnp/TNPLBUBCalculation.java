
package com.project.rithomas.jobexecution.tnp;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.boundary.CalculateLBUB;
import com.project.rithomas.jobexecution.common.boundary.JobPropertyRetriever;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class TNPLBUBCalculation extends CalculateLBUB {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(TNPLBUBCalculation.class);

	private static final String UNCHECKED = "unchecked";

	private static final Boolean DEBUG_MODE = LOGGER.isDebugEnabled();

	private Map<String, Long> lbRegionMap = new HashMap<String, Long>();

	private Map<String, Long> ubRegionMap = new HashMap<String, Long>();

	@SuppressWarnings(UNCHECKED)
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.info("Calculating LB and UB..");
		List<String> sourceJobIdList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		// Timezone consideration for boundary calculation
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			List<String> timeZoneRegions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			int totalRgnCnt = timeZoneRegions.size();
			LOGGER.debug("TimeZone regions: {}", timeZoneRegions);
			LOGGER.debug("total region count: {}", totalRgnCnt);
			for (String region : timeZoneRegions) {
				LOGGER.info("Setting boundary and max value for region: {}",
						region);
				final BoundaryQuery boundaryQuery = new BoundaryQuery();
				final List<Boundary> srcBoundaryList = new ArrayList<Boundary>();
				List<Boundary> boundaryList = boundaryQuery
						.retrieveByJobIdAndRegionId(jobName, region);
				if (JobTypeDictionary.TNP_PERF_JOB_TYPE
						.equalsIgnoreCase(jobType)) {
					boundaryList = boundaryQuery.retrieveByJobId(jobName);
				}
				Boundary boundary = boundaryList.get(0);
				context.setProperty("MAXVALUE_" + region,
						boundary.getMaxValue());
				for (String sourceJob : sourceJobIdList) {
					LOGGER.debug("source job:" + sourceJob);
					List<Boundary> boundaryWithRgn = boundaryQuery
							.retrieveByJobIdAndRegionId(sourceJob, region);
					if (!boundaryWithRgn.isEmpty()) {
						srcBoundaryList.add(boundaryWithRgn.get(0));
					}
				}
				context.setProperty("SOURCE_BOUND_LIST_" + region,
						srcBoundaryList);
			}
		} else {
			final BoundaryQuery boundaryQuery = new BoundaryQuery();
			final List<Boundary> srcBoundaryList = new ArrayList<Boundary>();
			for (String sourceJob : sourceJobIdList) {
				LOGGER.debug("source job:" + sourceJob);
				srcBoundaryList
						.addAll(boundaryQuery.retrieveByJobId(sourceJob));
			}
			context.setProperty(JobExecutionContext.SOURCE_BOUND_LIST,
					srcBoundaryList);
		}
		// Set retentiondays
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(
				context);
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS,
				jobPropertyRetriever.getAggRetentionDays());
		this.calculateLB(context);
		this.calculateUB(context);
		this.calculateNLB(context);
		context.setProperty(JobExecutionContext.RETURN, 0);
		this.lbubChecks(context);
		return true;
	}

	@SuppressWarnings(UNCHECKED)
	private void calculateLB(WorkFlowContext context) {
		Calendar calendar = TNPLBUBCalculationHelper.getCalendar(context);
		Calendar calendarLB = new GregorianCalendar();
		// setting the retention based value as LB (will be used when the source
		// table doesn't have any data)
		calendarLB.setTimeInMillis(calendar.getTimeInMillis());
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			List<String> timeZoneRegions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			for (String region : timeZoneRegions) {
				List<Boundary> srcBoundaryList = (List<Boundary>) context
						.getProperty("SOURCE_BOUND_LIST_" + region);
				Timestamp maxValue = (Timestamp) context
						.getProperty("MAXVALUE_" + region);
				this.calculateLB(context, srcBoundaryList, maxValue, calendarLB,
						region);
			}
		} else {
			List<Boundary> srcBoundaryList = (List<Boundary>) context
					.getProperty(JobExecutionContext.SOURCE_BOUND_LIST);
			Timestamp maxValue = (Timestamp) context
					.getProperty(JobExecutionContext.MAXVALUE);
			this.calculateLB(context, srcBoundaryList, maxValue, calendarLB,
					null);
		}
	}

	private void calculateLB(WorkFlowContext context,
			List<Boundary> srcBoundaryList, Timestamp maxValue,
			Calendar calendarLB, String region) {
		Calendar calendar = TNPLBUBCalculationHelper.getCalendar(context);
		Calendar calLBValue = calendarLB;
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		if (maxValue == null) {
			TNPLBUBCalculationHelper.calculateLB(context, srcBoundaryList,
					jobType, calLBValue);
		} else {
			calLBValue = DateFunctionTransformation.getInstance().getNextBound(
					maxValue.getTime(),
					(String) context.getProperty(JobExecutionContext.PLEVEL));
		}
		LOGGER.debug("Boundary based on retention: {}", calendar.getTime());
		LOGGER.debug("Boundary based on table: {}", calLBValue.getTime());
		if (calendar.getTimeInMillis() > calLBValue.getTimeInMillis()) {
			calLBValue.setTimeInMillis(calendar.getTimeInMillis());
		}
		Long finalLb = DateFunctionTransformation.getInstance().getTrunc(
				calLBValue.getTimeInMillis(),
				(String) context.getProperty(JobExecutionContext.PLEVEL),
				(String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY));
		calculateLBforRegion(context, region, finalLb);
	}

	private void calculateLBforRegion(WorkFlowContext context, String region,
			Long finalLb) {
		if (region == null) {
			context.setProperty(JobExecutionContext.LB, finalLb);
			LOGGER.info("LowerBound value is : {}",
					TNPLBUBCalculationHelper.getDateFromMilliSeconds(finalLb));
		} else {
			context.setProperty("LB_" + region, finalLb);
			this.lbRegionMap.put(region, finalLb);
			context.setProperty(
					JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION,
					this.lbRegionMap);
			LOGGER.info("LowerBound value for region {} is : {}", region,
					TNPLBUBCalculationHelper.getDateFromMilliSeconds(finalLb));
		}
	}

	@SuppressWarnings(UNCHECKED)
	private void calculateUB(WorkFlowContext context) {
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			List<String> timeZoneRegions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			for (String region : timeZoneRegions) {
				List<Boundary> srcBoundaryList = (List<Boundary>) context
						.getProperty("SOURCE_BOUND_LIST_" + region);
				this.calculateUB(context, srcBoundaryList, region);
			}
		} else {
			List<Boundary> srcBoundaryList = (List<Boundary>) context
					.getProperty(JobExecutionContext.SOURCE_BOUND_LIST);
			this.calculateUB(context, srcBoundaryList, null);
		}
	}

	private void calculateUB(WorkFlowContext context,
			List<Boundary> srcBoundaryList, String region) {
		Calendar calendarUB = new GregorianCalendar();
		boolean firstTime = true;
		String sourcePlevel = null;
		// To check if the source job type is Usage or not.
		LOGGER.debug("Calculating UB from source boundary list");
		for (Boundary srcBoundary : srcBoundaryList) {
			LOGGER.debug("for source job: {}, maxvalue is: {}",
					srcBoundary.getJobId(), srcBoundary.getMaxValue());
			sourcePlevel = (String) context
					.getProperty(srcBoundary.getJobId() + "_PLEVEL");
			if (srcBoundary.getMaxValue() != null) {
				firstTime = TNPLBUBCalculationHelper.setUBValue(calendarUB,
						firstTime, srcBoundary);
			} else {
				calendarUB = null;
				LOGGER.debug("UB value is: null");
				break;
			}
		}
		Long finalUb = null;
		if (calendarUB != null) {
			finalUb = TNPLBUBCalculationHelper.calculateFinalUBValue(calendarUB,
					sourcePlevel, context);
			setUBinAccordanceWithRegion(context, region, finalUb);
		} else {
			LOGGER.info("UpperBound value is : {}", finalUb);
		}
	}

	private void setUBinAccordanceWithRegion(WorkFlowContext context,
			String region, Long finalUb) {
		if (region == null) {
			context.setProperty(JobExecutionContext.UB, finalUb);
			LOGGER.info("UpperBound value is : {}",
					TNPLBUBCalculationHelper.getDateFromMilliSeconds(finalUb));
		} else {
			context.setProperty("UB_" + region, finalUb);
			this.ubRegionMap.put(region, finalUb);
			context.setProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION,
					this.ubRegionMap);
			LOGGER.info("UpperBound value for region {} is : {}", region,
					TNPLBUBCalculationHelper.getDateFromMilliSeconds(finalUb));
		}
	}

	private void calculateNLB(WorkFlowContext context) {
		if (!TimeZoneUtil.isTimeZoneEnabled(context)) {
			Long finalLb = (Long) context.getProperty(JobExecutionContext.LB);
			Calendar calendarNLB = DateFunctionTransformation.getInstance()
					.getNextBound(finalLb, (String) context
							.getProperty(JobExecutionContext.PLEVEL));
			Long nextLb = calendarNLB.getTimeInMillis();
			context.setProperty(JobExecutionContext.NEXT_LB, nextLb);
			LOGGER.info("Nextlowerbound value is : {}",
					TNPLBUBCalculationHelper.getDateFromMilliSeconds(nextLb));
		}
	}

	@SuppressWarnings("unchecked")
	private void lbubChecks(WorkFlowContext context)
			throws WorkFlowExecutionException {
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			List<String> regions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			Map<String, Long> regionOffsetMap = (Map<String, Long>) context
					.getProperty(
							JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP);
			int rgnCntToAggregate = 0;
			for (String regionId : regions) {
				Long offset = getOffset(regionOffsetMap, regionId);
				Long finalLb = (Long) context.getProperty("LB_" + regionId);
				Long finalUb = null;
				Long tempUb = (Long) context.getProperty("UB_" + regionId);
				if (null != tempUb) {
					finalUb = tempUb - offset;
				}
				Long nextLb = DateFunctionTransformation.getInstance()
						.getNextBound(finalLb,
								(String) context.getProperty(
										JobExecutionContext.PLEVEL))
						.getTimeInMillis();
				LOGGER.info("Nextlowerbound value for region {} is : {}",
						regionId, TNPLBUBCalculationHelper
								.getDateFromMilliSeconds(nextLb));
				rgnCntToAggregate = TNPLBUBCalculationHelper
						.finalLBUBRegionCheck(context, rgnCntToAggregate,
								regionId, finalLb, finalUb, nextLb);
				if (rgnCntToAggregate == -1) {
					break;
				}
			}
			if (rgnCntToAggregate > 0) {
				context.setProperty(JobExecutionContext.RETURN, 0);
			} else {
				context.setProperty(JobExecutionContext.RETURN, 1);
			}
		} else {
			TNPLBUBCalculationHelper.finalLBUBCheck(context);
		}
	}

	private Long getOffset(Map<String, Long> regionOffsetMap, String regionId)
			throws WorkFlowExecutionException {
		Long offset = regionOffsetMap.get(regionId);
		if (offset == null) {
			throw new WorkFlowExecutionException(
					"The time_difference not set for region ".concat(regionId)
							.concat(" in saidata.es_rgn_tz_info_1 table. Please load time_difference for all configured regions for the job to run"));
		}
		return offset;
	}
}

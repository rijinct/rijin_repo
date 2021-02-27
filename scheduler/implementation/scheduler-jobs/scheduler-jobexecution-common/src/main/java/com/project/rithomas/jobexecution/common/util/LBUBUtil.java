
package com.project.rithomas.jobexecution.common.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class LBUBUtil {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(LBUBUtil.class);

	private static final String EITHER_OF_BOUNDARY_VALUE_NULL = "Either of the boundary value is null.Hence not aggregating/exporting further";

	private static final String NO_COMPLETE_DATA = "No Complete Data to Export/Aggregate further";

	private static final String KPI_JOB = "_1_KPIJob";

	private static final String PROFILE_JOB = "_1_ProfileJob";

	private static final String KPI_JOB_LIKE = "%_KPIJob";

	private static final String PROFILE_JOB_LIKE = "%_ProfileJob";

	private static final String UNDERSCORE = "_";

	public static void lbubChecks(WorkFlowContext context, String regionId)
			throws WorkFlowExecutionException {
		Long finalLb = null;
		Long finalUb = null;
		Long nextLb = null;
		if (("YES".equalsIgnoreCase(
				(String) context.getProperty("TIME_ZONE_SUPPORT")))
				&& !(TimeZoneUtil.isTimeZoneAgnostic(context))) {
			int rgnCntToAggregate = (Integer) context
					.getProperty(JobExecutionContext.REGION_COUNT_TO_AGG);
			finalLb = (Long) context.getProperty("LB_" + regionId);
			finalUb = (Long) context.getProperty("UB_" + regionId);
			nextLb = (Long) context.getProperty("NEXT_LB_" + regionId);
			if (finalLb != null && finalUb != null && finalLb.equals(finalUb)) {
				LOGGER.info(NO_COMPLETE_DATA + " for region " + regionId);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context,
						NO_COMPLETE_DATA + " for region " + regionId);
			} else if (finalLb != null && finalUb != null && nextLb > finalUb) {
				LOGGER.info(NO_COMPLETE_DATA + " for region " + regionId);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context,
						NO_COMPLETE_DATA + " for region " + regionId);
			} else if (finalUb == null || finalLb == null) {
				LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL + " for region "
						+ regionId);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context, EITHER_OF_BOUNDARY_VALUE_NULL
						+ " for region " + regionId);
			} else {
				// if data is there to aggregate
				rgnCntToAggregate++;
				context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG,
						rgnCntToAggregate);
			}
			if (rgnCntToAggregate > 0) {
				// atleast one region has data to aggregate. So moving to next
				// step
				context.setProperty(JobExecutionContext.RETURN, 0);
			} else {
				// no data available to aggregate for any region so exiting the
				// job
				context.setProperty(JobExecutionContext.RETURN, 1);
			}
		} else {
			finalLb = (Long) context.getProperty(JobExecutionContext.LB);
			finalUb = (Long) context.getProperty(JobExecutionContext.UB);
			nextLb = (Long) context.getProperty(JobExecutionContext.NEXT_LB);
			if (finalUb == null || finalLb == null) {
				LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context, EITHER_OF_BOUNDARY_VALUE_NULL);
			} else if (finalLb.equals(finalUb)) {
				LOGGER.info(NO_COMPLETE_DATA);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context, NO_COMPLETE_DATA);
			} else if (nextLb > finalUb) {
				LOGGER.info(NO_COMPLETE_DATA);
				context.setProperty(JobExecutionContext.RETURN, 1);
				setDescription(context, NO_COMPLETE_DATA);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public List<Boundary> getSourceBoundaryList(WorkFlowContext context) {
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		Timestamp maxValue = (Timestamp) context
				.getProperty(JobExecutionContext.MAXVALUE);
		LOGGER.debug("MAXVAL in context:{}", maxValue);
		final BoundaryQuery boundaryQuery = new BoundaryQuery();
		List<String> sourceJobIdList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		List<Boundary> srcBoundaryList = new ArrayList<Boundary>();
		// getting the maxvalue from boundary for job
		List<Boundary> boundaryList = boundaryQuery.retrieveByJobId(jobName);
		Boundary boundary = boundaryList.get(0);
		maxValue = boundary.getMaxValue();
		LOGGER.debug("boundary for current job{}", maxValue);
		context.setProperty(JobExecutionContext.MAXVALUE, maxValue);
		if (sourceJobIdList.get(0).contains(KPI_JOB)) {
			String tempJobId = sourceJobIdList.get(0).replace(KPI_JOB,
					KPI_JOB_LIKE);
			srcBoundaryList = boundaryQuery
					.retrieveByJobIdLike(tempJobId.toUpperCase());
		} else if (sourceJobIdList.get(0).contains(PROFILE_JOB)) {
			String tempJobId = sourceJobIdList.get(0).replace(PROFILE_JOB,
					PROFILE_JOB_LIKE);
			srcBoundaryList = boundaryQuery
					.retrieveByJobIdLike(tempJobId.toUpperCase());
		} else {
			for (String sourceJob : sourceJobIdList) {
				LOGGER.debug("source job:" + sourceJob);
				srcBoundaryList
						.add(boundaryQuery.retrieveByJobId(sourceJob).get(0));
			}
		}
		return srcBoundaryList;
	}

	private static void setDescription(WorkFlowContext context,
			String description) {
		if (!(("YES").equals(context
				.getProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC))))
			context.setProperty(JobExecutionContext.DESCRIPTION, description);
	}

	public static List<String> getSpecIDVerFromJobName(String jobName,
			String jobType) {
		String specIDVersion = "";
		if (isAggTypeJob(jobType)) {
			String specIDVersionInterval = jobName.substring(
					jobName.indexOf(UNDERSCORE) + 1,
					jobName.lastIndexOf(UNDERSCORE));
			specIDVersion = specIDVersionInterval.substring(0,
					specIDVersionInterval.lastIndexOf(UNDERSCORE));
		} else if (isUsageTypeJob(jobType)) {
			specIDVersion = jobName.substring(jobName.indexOf(UNDERSCORE) + 1,
					jobName.lastIndexOf(UNDERSCORE));
		}
		String specID = specIDVersion.substring(0,
				specIDVersion.lastIndexOf(UNDERSCORE));
		String specVersion = specIDVersion
				.substring(specIDVersion.lastIndexOf(UNDERSCORE) + 1);
		List<String> strList = new ArrayList<String>();
		strList.add(specID);
		strList.add(specVersion);
		LOGGER.debug("Spec ID:{}, Spec Version:{}", specID, specVersion);
		return strList;
	}

	public static boolean isAggTypeJob(String jobType) {
		return JobTypeDictionary.PERF_JOB_TYPE.equals(jobType);
	}

	public static boolean isUsageTypeJob(String jobType) {
		return JobTypeDictionary.USAGE_JOB_TYPE.equals(jobType);
	}

	public static Calendar getCalendarLb(WorkFlowContext context,
			String maxValue, Calendar calendar, Calendar calendarLB) {
		if ((Timestamp) context.getProperty(maxValue) == null) {
			calendarLB = DateFunctionTransformation.getInstance()
					.getPreviousBound(calendar.getTimeInMillis(),
							(String) context
									.getProperty(JobExecutionContext.ALEVEL));
		} else {
			calendarLB.setTime((Timestamp) context.getProperty(maxValue));
		}
		return calendarLB;
	}

	public static void setCalendarLBForAPN(WorkFlowContext context,
			String maxValue, Calendar calendar, Calendar calendarLB)
			throws WorkFlowExecutionException {
		if ((Timestamp) context.getProperty(maxValue) == null) {
			try {
				QueryExecutor queryExecutor = new QueryExecutor();
				Object[] result = queryExecutor.executeMetadatasqlQuery(
						QueryConstants.LB_APN_EXPORT, context);
				if (result == null || result.length == 0) {
					LOGGER.error(
							"No max value retrieved for LB from APN Export Job.So using the current date");
					calendarLB.setTimeInMillis(calendar.getTimeInMillis());
				} else {
					calendarLB.setTime((Timestamp) result[0]);
					LOGGER.debug("LB value fetched from APN Export job is {}",
							calendarLB.getTimeInMillis());
				}
			} catch (JobExecutionException e) {
				LOGGER.error(
						"Exception while executing query {}" + e.getMessage());
				throw new WorkFlowExecutionException(
						"Exception while gettting the lb value for APN Export ");
			}
		} else {
			calendarLB.setTime((Timestamp) context.getProperty(maxValue));
		}
	}
}
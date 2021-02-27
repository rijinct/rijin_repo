
package com.project.rithomas.jobexecution.common.boundary;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class SingleSource extends BoundaryCalculator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SingleSource.class);

	public SingleSource(WorkFlowContext workFlowContext) {
		super(workFlowContext);
	}

	@Override
	public Calendar calculateUB(String regionId)
			throws WorkFlowExecutionException {
		LOGGER.debug("Calculating upper bound for single source");
		Calendar calendarUB = new GregorianCalendar();
		List<Boundary> srcBoundaryList = null;
		List<String> skipRegionList = null;
		if (TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)) {
			srcBoundaryList = (List<Boundary>) context
					.getProperty("SOURCE_BOUND_LIST_" + regionId);
		} else {
			skipRegionList = getSkipRegionListForTimezoneAgnostic();
			srcBoundaryList = jobDetails.getSourceBoundList(regionId);
		}
		if (CollectionUtils.isNotEmpty(srcBoundaryList)) {
			for (Boundary srcBoundary : srcBoundaryList) {
				String srcRegionId = srcBoundary.getRegionId();
				if (srcRegionId == null
						|| CollectionUtils.isEmpty(skipRegionList)
						|| !skipRegionList.contains(srcRegionId)) {
					String regionInfo = (srcRegionId != null)
							? " for region " + srcRegionId : "";
					LOGGER.info("Boundary value for the job: {} {} is: {}",
							srcBoundary.getJobId(), regionInfo,
							srcBoundary.getMaxValue());
					if (srcBoundary.getMaxValue() != null) {
						LOGGER.debug(
								"When max value is not null for source: {}, the current upper bound is : {}",
								srcBoundary.getMaxValue().getTime(),
								calendarUB.getTimeInMillis());
						if (srcBoundary.getMaxValue().getTime() <= calendarUB
								.getTimeInMillis()) {
							calendarUB.setTime(srcBoundary.getMaxValue());
							sourcePlevel = (String) context.getProperty(
									srcBoundary.getJobId() + PLEVEL);
						}
					} else {
						firstRun = true;
						calendarUB = null;
						break;
					}
				}
			}
		} else {
			firstRun = true;
			calendarUB = null;
		}
		LOGGER.debug("calendarUB: {}", calendarUB);
		return calendarUB;
	}

	private List<String> getSkipRegionListForTimezoneAgnostic()
			throws WorkFlowExecutionException {
		List<String> skipRegionList = new ArrayList<>();
		if (TimeZoneUtil.isTimeZoneEnabled(context) && !TimeZoneUtil
				.isTimeZoneAgnostic(context, sourcePlevel, null)) {
			ModifySourceJobList modifySourceJobList = new ModifySourceJobList(
					context);
			skipRegionList = modifySourceJobList
					.getListOfSkipTimeZoneForAgnosticJobs();
		}
		return skipRegionList;
	}
}

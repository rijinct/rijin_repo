
package com.project.rithomas.jobexecution.common.boundary;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.text.StrSubstitutor;

import com.google.common.collect.ImmutableMap;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class MultiSourceWithoutCache extends BoundaryCalculator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MultiSourceWithoutCache.class);

	public MultiSourceWithoutCache(WorkFlowContext workFlowContext) {
		super(workFlowContext);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Calendar calculateUB(String regionId)
			throws WorkFlowExecutionException {
		if (regionId != null) {
			validForAggregationForMultipleSrc = true;
		}
		List<String> listOfAllAvailableStageTypes = new ArrayList<>();
		Map<String, Long> leastReportTimeMap = new HashMap<>();
		List<Object> resultSet = null;
		QueryExecutor queryExecutor = new QueryExecutor();
		List<String> sourceJobs = jobDetails.getSourceJobList(regionId);
		String srcJobType = jobDetails.getSourceJobType();
		try {
			for (String eachSrcJob : sourceJobs) {
				final String availabletype = getAvailableType(srcJobType,
						LBUBUtil.getSpecIDVerFromJobName(eachSrcJob,
								srcJobType));
				resultSet = queryExecutor.executeMetadatasqlQueryMultiple(
						getAvailDataSqlQuery(availabletype), context);
				if (CollectionUtils.isEmpty(resultSet)) {
					Long truncDate = getMaxValueForAvailableType(regionId,
							eachSrcJob, availabletype);
					listOfAllAvailableStageTypes.add(availabletype);
					leastReportTimeMap.put(availabletype, truncDate);
				} else {
					LOGGER.info(
							"{} is configured as No, so not considering this source.",
							availabletype);
				}
			}
		} catch (JobExecutionException e) {
			throw new WorkFlowExecutionException(e.getMessage(), e);
		}
		return getFinalUBOfSrcs(regionId, listOfAllAvailableStageTypes,
				leastReportTimeMap);
	}

	private String getAvailableType(String srcJobType,
			final List<String> specIdVersionList) {
		return StrSubstitutor.replace(getEntityType(srcJobType),
				ImmutableMap.of("SpecID",
						specIdVersionList.get(0).toUpperCase(), "SpecVersion",
						specIdVersionList.get(1).toUpperCase()));
	}

	private String getEntityType(String srcJobType) {
		String entityType = null;
		if (LBUBUtil.isAggTypeJob(srcJobType)) {
			entityType = "PS_${SpecID}_${SpecVersion}";
		} else if (LBUBUtil.isUsageTypeJob(srcJobType)) {
			entityType = "US_${SpecID}_${SpecVersion}";
		}
		return entityType;
	}

	private String getAvailDataSqlQuery(final String availabletype) {
		return StrSubstitutor.replace(
				QueryConstants.AVAILABLE_DATA_ON_TYPE_AVAIL,
				ImmutableMap.of(QueryConstants.AVAILABLE_DATA_COL_TYPE,
						availabletype,
						QueryConstants.AVAILABLE_DATA_COL_AVAILABLE, "No"));
	}

	private Long getMaxValueForAvailableType(String regionId, String eachSrcJob,
			final String availabletype) throws WorkFlowExecutionException {
		Long truncDate = null;
		context.setProperty(JobExecutionContext.SOURCE_JOB_NAME, eachSrcJob);
		Calendar calendarmaxValue = TimeZoneUtil.isTimeZoneEnabled(context)
				? getSourceBoundaryValue(regionId)
				: getSourceBoundaryValue(null);
		plevelMap.put(availabletype,
				(String) context.getProperty(eachSrcJob + PLEVEL));
		if (calendarmaxValue != null) {
			LOGGER.debug("UB value for {} is {}", availabletype,
					DateFunctionTransformation.getInstance()
							.getFormattedDate(calendarmaxValue.getTime()));
			truncDate = DateFunctionTransformation.getInstance().getTrunc(
					calendarmaxValue.getTimeInMillis(),
					jobPropertyRetriever.getPartitionLevel(),
					(String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY),
					TimeZoneUtil.getZoneId(context, regionId));
		} else {
			LOGGER.info("No Data Available for type {}", availabletype);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					new StringBuilder("No Data Available for type ")
							.append(availabletype).toString());
		}
		return truncDate;
	}
}


package com.project.rithomas.jobexecution.common.boundary;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.common.cache.service.CacheServiceException;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class MultiSourceWithCache extends BoundaryCalculator {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MultiSourceWithCache.class);

	public MultiSourceWithCache(WorkFlowContext workFlowContext) {
		super(workFlowContext);
	}

	@Override
	public Calendar calculateUB(String regionId)
			throws WorkFlowExecutionException {
		LOGGER.debug("Calculating Upper bound with Cache");
		if (regionId != null) {
			validForAggregationForMultipleSrc = true;
		}
		Long finalLB = getLB(regionId);
		Calendar calendarUBValue = null;
		List<Boundary> sourceBoundaryList = jobDetails
				.getSourceBoundList(regionId);
		try {
			calendarUBValue = fetchUB(regionId, finalLB, sourceBoundaryList);
		} catch (JobExecutionException e) {
			LOGGER.error(
					"Exception while calculating upper bound with cache : {}",
					e);
			throw new WorkFlowExecutionException(e.getMessage(), e);
		}
		return calendarUBValue;
	}

	private Calendar fetchUB(String regionId, Long finalLB,
			List<Boundary> sourceBoundaryList)
			throws JobExecutionException, WorkFlowExecutionException {
		List<String> listOfAllAvailableStageTypes = new ArrayList<>();
		Map<String, Long> reportTimeMapForAllStages = new HashMap<>();
		List<String> excludedStageTypes = jobPropertyRetriever
				.getExcludedStageTypes();
		try {
			for (Boundary eachSourceBoundary : sourceBoundaryList) {
				if (eachSourceBoundary.getMaxValue() != null) {
					String sourceJobId = eachSourceBoundary.getJobId();
					List<Long> listOfUbVal = getListOfAllIntervals(finalLB,
							(Long) eachSourceBoundary.getMaxValue().getTime(),
							jobPropertyRetriever.getPartitionLevel());
					Collections.reverse(listOfUbVal);
					LOGGER.debug(
							"List of all the UB values for job : {} are : {}",
							sourceJobId, listOfUbVal);
					populateMapForAvailableStageTypes(regionId, sourceJobId,
							listOfAllAvailableStageTypes,
							reportTimeMapForAllStages, listOfUbVal,
							excludedStageTypes);
				}
			}
		} catch (JobExecutionException | CacheServiceException e) {
			throw new JobExecutionException(e.getMessage(), e);
		}
		return getFinalUBOfSrcs(regionId, listOfAllAvailableStageTypes,
				reportTimeMapForAllStages);
	}

	@SuppressWarnings("unchecked")
	private void populateMapForAvailableStageTypes(String regionId,
			String sourceJobId, List<String> listOfAllAvailableStageTypes,
			Map<String, Long> reportTimeMapForAllStages, List<Long> listOfUbVal,
			List<String> excludedStageTypes)
			throws JobExecutionException, CacheServiceException {
		String usageSpecId = jobDetails.getUsageSpecId(sourceJobId);
		QueryExecutor queryExecutor = new QueryExecutor();
		List<Object> enabledStageTypeList = queryExecutor
				.executeMetadatasqlQueryMultiple(String.format(
						QueryConstants.SGSN_AVAILABLE_DATA_TYPE_QUERY,
						usageSpecId), context);
		if (CollectionUtils.isNotEmpty(enabledStageTypeList)) {
			for (Object eachStageType : enabledStageTypeList) {
				String trimmedStageType = eachStageType.toString().trim();
				if (CollectionUtils.isEmpty(excludedStageTypes)
						|| !excludedStageTypes.contains(trimmedStageType)) {
					listOfAllAvailableStageTypes.add(trimmedStageType);
					String keyTemplate = getKeyTemplateForJob(sourceJobId,
							regionId, trimmedStageType);
					reportTimeMapForAllStages.put(trimmedStageType,
							getMaxLowerBoundForStageType(listOfUbVal,
									trimmedStageType, keyTemplate));
				} else {
					LOGGER.info(
							"Excluding the stage type:{} for boundary calculation",
							trimmedStageType);
				}
			}
		}
	}

	private Long getMaxLowerBoundForStageType(List<Long> listOfUbVal,
			String stageTypeToCheck, String keyTemplate)
			throws CacheServiceException {
		DataAvailabilityCacheUtil dataAvailabilityCacheService = new DataAvailabilityCacheUtil();
		Long lowerBoundForStageType = null;
		try {
			for (Long lowerBound : listOfUbVal) {
				String key = keyTemplate.replace(
						JobExecutionContext.LOWER_BOUND, lowerBound.toString());
				String value = dataAvailabilityCacheService.getValue(key);
				LOGGER.debug("key: {}, value:{}", key, value);
				if (value != null) {
					LOGGER.debug(
							"Data available for StageType : {} for the interval {}",
							stageTypeToCheck, lowerBound);
					lowerBoundForStageType = lowerBound;
					break;
				} else {
					LOGGER.info(
							"Data not available for {} for this interval {}",
							stageTypeToCheck, lowerBound);
				}
			}
		} finally {
			dataAvailabilityCacheService.closeConnection();
		}
		LOGGER.debug("Lower bound for the stage type : {} is : {}",
				stageTypeToCheck, lowerBoundForStageType);
		return lowerBoundForStageType;
	}

	private String getKeyTemplateForJob(String jobId, String regionId,
			String stageType) {
		String keyTemplate = TimeZoneUtil.isTimeZoneEnabled(context)
				? JobExecutionContext.DATA_AVBL_CACHE_KEY_TIMEZONE
				: JobExecutionContext.DATA_AVBL_CACHE_KEY;
		String adapIdVersion = jobDetails.getAdaptationFromJobName(jobId);
		String specIdVersion = jobDetails.getSpecIdVersion(jobId);
		keyTemplate = keyTemplate
				.replace(JobExecutionContext.ADAPTATION_ID_VERSION,
						adapIdVersion)
				.replace(JobExecutionContext.USAGE_SPECIFICATION_ID_VERSION,
						specIdVersion);
		keyTemplate = keyTemplate.replace(JobExecutionContext.STAGE_TYPE,
				stageType);
		if (isNotEmpty(regionId)) {
			keyTemplate = keyTemplate
					.replace(JobExecutionContext.TIMEZONE_REGION, regionId);
		}
		return keyTemplate;
	}
}

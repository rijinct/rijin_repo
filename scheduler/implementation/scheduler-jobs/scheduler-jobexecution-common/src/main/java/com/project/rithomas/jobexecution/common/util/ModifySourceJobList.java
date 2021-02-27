
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ModifySourceJobList {

	private WorkFlowContext workFlowContext;

	private String region;

	private List<String> sourceJobIdList;

	private Set<String> sourceJobIdListTZEnabledSet;

	private String usageSpecID;

	private String serviceIDSQLWithAvailableSrc;
	
	private String serviceIDSQL;

	private QueryExecutor queryExecutor = new QueryExecutor();

	private String sourceJob;

	private Set<String> serviceIDSet;

	private String skippedTimezoneSQL;

	private String skippedTimezoneService;

	private List<String> skipRegionList;

	private Map<String, Set<String>> skipTimezoneMap;

	private Map<String, List<String>> baseSourceJobsMap;
	
	private List<String> baseUsageJobs = new ArrayList<>();

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ModifySourceJobList.class);

	public ModifySourceJobList(WorkFlowContext workFlowContext, String region,
			List<String> sourceJobIdList) {
		this.workFlowContext = workFlowContext;
		this.region = region;
		this.sourceJobIdList = sourceJobIdList;
		this.baseSourceJobsMap = getBaseSourceJobsMap();
	}

	public ModifySourceJobList(WorkFlowContext workFlowContext) {
		this.workFlowContext = workFlowContext;
		sourceJobIdList = getBaseSourceJobs(getBaseSourceJobsMap());
	}

	private List<String> getBaseSourceJobs(
			Map<String, List<String>> baseSourceJobsMap) {
		List<String> baseSourceJobList = new ArrayList<>();
		for (List<String> baseSourceJobs : baseSourceJobsMap.values()) {
			baseSourceJobList.addAll(baseSourceJobs);
		}
		return baseSourceJobList;
	}

	private Map<String, List<String>> getBaseSourceJobsMap() {
		return (Map<String, List<String>>) workFlowContext
				.getProperty(JobExecutionContext.BASE_SOURCE_JOB_MAP);
	}

	public List<String> getListOfSkipTimeZoneForAgnosticJobs()
			throws WorkFlowExecutionException {
		sourceJobIdListTZEnabledSet = new HashSet<String>();
		skipRegionList = new ArrayList<String>();
		boolean isFirstSource = true;
		for (String sourceJob : sourceJobIdList) {
			try {
				this.sourceJob = sourceJob;
				getUsageSpecID(sourceJob);
				getServiceIDSQL();
				getServiceIDSet();
				executeAndloadSkipTimezoneIntoMap();
				getSkipRegionList(isFirstSource);
				if (skipRegionList.isEmpty()) {
					break;
				}
				isFirstSource = false;
			} catch (JobExecutionException e) {
				LOGGER.error(
						"Exception while executing query {}" + e.getMessage());
				throw new WorkFlowExecutionException(
						"Exception while generating skipRegionList for Agnostic jobs ");
			}
		}
		LOGGER.info("Time-zone regions to be skipped: {}", skipRegionList);
		return skipRegionList;
	}

	private void getSkipRegionList(boolean isFirstSource) {
		if (serviceIDSet.isEmpty()) {
			return;
		}
		for (Entry<String, Set<String>> timeZone : skipTimezoneMap.entrySet()) {
			int serviceCount = 0;
			for (String serviceID : serviceIDSet) {
				if (timeZone.getValue().contains(serviceID.toUpperCase())) {
					serviceCount++;
				}
			}
			if (serviceCount == serviceIDSet.size()) {
				LOGGER.debug(
						"No sources are sending data, hence skipping for {}",
						timeZone.getKey());
				if (isFirstSource) {
					skipRegionList.add(timeZone.getKey());
				}
			} else {
				skipRegionList.removeIf(str -> str.contains(timeZone.getKey()));
			}
		}
	}

	private void executeAndloadSkipTimezoneIntoMap()
			throws JobExecutionException {
		List<Object[]> skipTimezoneObjectList = queryExecutor
				.executeMetadatasqlQueryMultiple(
						QueryConstants.SKIPPED_SERVICE_TZ_QUERY,
						workFlowContext);
		skipTimezoneMap = new HashMap<>();
		if (CollectionUtils.isNotEmpty(skipTimezoneObjectList)) {
			for (Object[] object : skipTimezoneObjectList) {
				if (object[0] != null && object[1] != null) {
					String[] skipServices = StringUtils
							.split(object[1].toString().toUpperCase(), ',');
					Set<String> skipServicesSet = new HashSet<>(
							Arrays.asList(skipServices));
					skipTimezoneMap.put(object[0].toString(), skipServicesSet);
				}
			}
		}
	}

	public Set<String> modifySourceJobListForAgg()
			throws WorkFlowExecutionException {
		sourceJobIdListTZEnabledSet = new HashSet<String>();
		for (String sourceJob : sourceJobIdList) {
			try {
				LOGGER.debug("Source job : {}", sourceJob);
				this.sourceJob = sourceJob;
				String usageSources = getBaseUsageSources(sourceJob);
				serviceIDSQLWithAvailableSrc = getSQLToFetchServices(true, usageSources);
				serviceIDSQL = getSQLToFetchServices(false, usageSources);
				getServiceIDSet();
				getSkippedTimezoneSQL();
				getSkippedTimezoneServices();
				updateSourceJobSetBasedOnTimezoneServices();
			} catch (JobExecutionException e) {
				LOGGER.error(
						"Exception while executing query {}" + e.getMessage());
				throw new WorkFlowExecutionException(
						"Exception while generating source job list  for timezone ");
			}
		}
		return sourceJobIdListTZEnabledSet;
	}

	private void updateSourceJobSetBasedOnTimezoneServices() {
		StringTokenizer skippedTZTokens = new StringTokenizer(
				String.valueOf(skippedTimezoneService), ",");
		int serviceCount = 0;
		LOGGER.debug("The number of interfaces for a given service is {}",
				serviceIDSet.size());
		while (skippedTZTokens.hasMoreTokens()) {
			String serviceSkipped = skippedTZTokens.nextToken();
			if (serviceIDSet.contains(serviceSkipped.toUpperCase())) {
				LOGGER.info("The skipped service configured is {}",
						serviceSkipped);
				serviceCount++;
			}
		}
		LOGGER.info(
				"The number of interfaces for which  timezone is skipped  {}",
				serviceCount);
		if (serviceCount != serviceIDSet.size()) {
			LOGGER.debug(" Region {} has {} as another source", region,
					sourceJob);
			sourceJobIdListTZEnabledSet.add(sourceJob);
		}
	}

	private void getSkippedTimezoneServices() throws JobExecutionException {
		Object[] skippedTimezoneArray = queryExecutor
				.executeMetadatasqlQuery(skippedTimezoneSQL, workFlowContext);
		if (skippedTimezoneArray != null && skippedTimezoneArray[0] != null) {
			skippedTimezoneService = String.valueOf(skippedTimezoneArray[0]);
			LOGGER.info(
					" The services that are configured to be skipped for this time-zone are : {}",
					skippedTimezoneService);
		} else {
			sourceJobIdListTZEnabledSet.add(sourceJob);
		}
	}

	private void getSkippedTimezoneSQL() {
		skippedTimezoneSQL = QueryConstants.SKIPPED_TZ_QUERY
				.replace(JobExecutionContext.TZ_REGION_VAL, region);
		LOGGER.debug("The query to be executed for skipped time zone : {}",
				skippedTimezoneSQL);
	}

	private void getServiceIDSet() throws JobExecutionException {
		List<Object> serviceIDList = queryExecutor
				.executeMetadatasqlQueryMultiple(serviceIDSQL, workFlowContext);
		serviceIDSet = new HashSet<String>();
		if (CollectionUtils.isNotEmpty(serviceIDList)) {
			List<Object> serviceIDListWithAvailableSrc = queryExecutor
					.executeMetadatasqlQueryMultiple(serviceIDSQLWithAvailableSrc, workFlowContext);
			if (CollectionUtils.isNotEmpty(serviceIDListWithAvailableSrc)) {
				for (Object string : serviceIDList) {
					if (string != null) {
						serviceIDSet.add(String.valueOf(string).toUpperCase());
					}
				}
			} else {
				sourceJobIdListTZEnabledSet.add(sourceJob);
			}
		} else {
			if (StringUtils.isEmpty(usageSpecID)) {
				for (String baseJob : baseUsageJobs) {
					String usageSpecIdBaseJob = getUsageSpecificationID(baseJob);
					serviceIDSet.add(usageSpecIdBaseJob);
				}
			} else {
				serviceIDSet.add(usageSpecID);
			}

		}
	}

	private void getServiceIDSQL() {
		serviceIDSQLWithAvailableSrc = QueryConstants.SERVICE_ID_QUERY_AVAILABLE_SRC
				.replace(JobExecutionContext.USAGE_SPEC_ID, usageSpecID);
		serviceIDSQL = QueryConstants.SERVICE_ID_QUERY
				.replace(JobExecutionContext.USAGE_SPEC_ID, usageSpecID);
		LOGGER.debug("fetch serviceID: {}", serviceIDSQLWithAvailableSrc);
	}

	private String getSQLToFetchServices(boolean srcAvailable,  String usageIds) {
		return (srcAvailable) ? QueryConstants.SERVICE_ID_FOR_USAGE_QUERY_AVAILABLE_SRC
				.replace(JobExecutionContext.USAGE_ID_LIST, usageIds) : QueryConstants.SERVICE_ID_FOR_USAGE_QUERY
						.replace(JobExecutionContext.USAGE_ID_LIST, usageIds);
	}

	private String getBaseUsageSources(String sourceJob) {
		baseUsageJobs = baseSourceJobsMap.get(sourceJob);
		List<String> baseUsageSources = new ArrayList<>();
		for (String baseUsageJob : baseUsageJobs) {
			baseUsageSources.add(encloseUsageSpecInQuotes(baseUsageJob));
		}
		LOGGER.info("For the source job: {}, base usage sources are: {}",
				sourceJob, baseUsageSources);
		return Strings.join(baseUsageSources, ',');
	}

	private String encloseUsageSpecInQuotes(String baseUsageJob) {
		return "'" + getUsageSpecificationID(baseUsageJob) + "'";
	}

	private String getUsageSpecificationID(String usageJob) {
		return StringUtils.substringBetween(usageJob, "Usage_", "_1_LoadJob");
	}

	private void getUsageSpecID(String source) {
		usageSpecID = getUsageSpecificationID(source);
		LOGGER.debug("UsageSpecId is: {}", usageSpecID);
	}
}

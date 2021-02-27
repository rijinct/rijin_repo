
package com.project.rithomas.jobexecution.common.util;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class TimeZoneUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(TimeZoneUtil.class);

	private static final String POSTGRES_REGION_TZ_INFO_TABLE = "saidata.es_rgn_tz_info_1";

	private static final String DEFAULT_TZ_SUB_PARTITION = "DEFAULT_TZ_SUB_PARTITION";
	
	private static final Pattern UTC_PATTERN = Pattern.compile(".*(UTC|Zulu|Universal)");

	public static boolean isTimeZoneEnabled(WorkFlowContext context) {
		boolean timeZoneEnabled = "YES".equalsIgnoreCase((String) context
				.getProperty(JobExecutionContext.TIME_ZONE_SUPPORT));
		if (JobTypeDictionary.ARCHIVING_JOB_TYPE.equalsIgnoreCase(
				(String) context.getProperty(JobExecutionContext.JOBTYPE))) {
			String sourceJob = (String) context
					.getProperty(JobExecutionContext.SOURCEJOBID);
			LOGGER.debug("source job: {}", sourceJob);
			timeZoneEnabled = "YES"
					.equalsIgnoreCase((String) context.getProperty(sourceJob
							+ "_" + JobExecutionContext.TIME_ZONE_SUPPORT));
			LOGGER.debug("Time zone enabled for source job: {}", timeZoneEnabled);
		}
		return timeZoneEnabled;
	}

	@SuppressWarnings({ "unchecked" })
	public static void setRegionTimeDiffOffset(WorkFlowContext context)
			throws JobExecutionException {
		QueryExecutor queryExecutor = new QueryExecutor();
		String dataSrcRgnOffsetQuery = QueryConstants.REGION_OFFSET_QUERY
				.replace(QueryConstants.TABLE_NAME,
						POSTGRES_REGION_TZ_INFO_TABLE);
		List<Object[]> resultSet = queryExecutor
				.executeMetadatasqlQueryMultiple(dataSrcRgnOffsetQuery,
						context);
		Map<String, Long> regionOffsetMap = new HashMap<String, Long>();
		if (resultSet != null && !resultSet.isEmpty()) {
			for (Object[] result : resultSet) {
				String paramName = result[0].toString();
				Long paramValue = 0L;
				if (result[1] != null) {
					paramValue = (long) (Double
							.parseDouble(result[1].toString()) * 3600 * 1000L);
				}
				LOGGER.debug("Region: {}, Offset: {}", paramName, paramValue);
				regionOffsetMap.put(paramName, paramValue);
			}
		}
		context.setProperty(JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP,
				regionOffsetMap);
	}

	public static Map<String, String> getRegionZoneIdMapping(
			WorkFlowContext context) throws JobExecutionException {
		Map<String, String> regionZoneIdMapping = new HashMap<>();
		if (isSystemInUTC()) {
			QueryExecutor queryExecutor = new QueryExecutor();
			String dataSrcZoneIdQuery = QueryConstants.REGION_ZONEID_QUERY
					.replace(QueryConstants.TABLE_NAME,
							POSTGRES_REGION_TZ_INFO_TABLE);
			List<Object[]> resultSet = queryExecutor
					.executeMetadatasqlQueryMultiple(dataSrcZoneIdQuery,
							context);
			if (CollectionUtils.isNotEmpty(resultSet)) {
				regionZoneIdMapping.put(JobExecutionContext.DEFAULT_TIMEZONE,
						getValue(resultSet.get(0)));
				for (Object[] result : resultSet) {
					String paramName = result[0].toString();
					String paramValue = getValue(result);
					LOGGER.debug("Region: {}, ZoneId: {}", paramName,
							paramValue);
					regionZoneIdMapping.put(paramName, paramValue);
				}
			}
			if (MapUtils.isEmpty(regionZoneIdMapping)) {
				regionZoneIdMapping.put(JobExecutionContext.DEFAULT_TIMEZONE,
						TimeZoneConverter.UTC);
			}
		}
		return regionZoneIdMapping;
	}

	private static String getValue(Object[] result) {
		return result[1] != null ? result[1].toString()
				: null;
	}

	public static boolean isSystemInUTC() {
		Matcher matcher = UTC_PATTERN.matcher(ZoneId.systemDefault().getId());
		return matcher.matches();
	}
	
	public static String getZoneId(WorkFlowContext context, String region) {
		if (isSystemInUTC()) {
			String tz = region != null ? region
					: JobExecutionContext.DEFAULT_TIMEZONE;
			Map<String, String> regionZoneIdMapping = (Map<String, String>) context
					.getProperty(JobExecutionContext.REGION_ZONEID_MAPPING);
			String zoneId = null;
			if (MapUtils.isNotEmpty(regionZoneIdMapping)
					&& regionZoneIdMapping.containsKey(tz)) {
				zoneId = regionZoneIdMapping.get(tz);
			} else {
				zoneId = TimeZoneConverter.UTC;
			}
			LOGGER.debug("Region: {}, ZoneId: {}", tz, zoneId);
			return zoneId;
		} else {
			return null;
		}
	}

	public static boolean isDefaultTZSubPartition(WorkFlowContext context) {
		boolean isDefaultTZSubPartition = false;
		String prop = (String) context.getProperty(DEFAULT_TZ_SUB_PARTITION);
		isDefaultTZSubPartition = JobExecutionContext.YES
				.equalsIgnoreCase(prop);
		LOGGER.debug("isDefaultTZSubPartition:" + isDefaultTZSubPartition);
		return isDefaultTZSubPartition;
	}

	public static boolean isTimeZoneAgnostic(WorkFlowContext context,
			String plevel, String jobName) throws WorkFlowExecutionException {
		boolean isTimeZoneAgnostic = false;
		try {
			isTimeZoneAgnostic = getTimeZoneAgnosticPlevels(context)
					.contains(plevel)
					|| getTimeZoneAgnosticJobs(context).contains(jobName);
		} catch (JobExecutionException ex) {
			throw new WorkFlowExecutionException(ex.getMessage(), ex);
		}
		return isTimeZoneAgnostic;
	}

	public static boolean isTimeZoneAgnostic(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String plevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		if (JobTypeDictionary.ARCHIVING_JOB_TYPE.equalsIgnoreCase(
				(String) context.getProperty(JobExecutionContext.JOBTYPE))) {
			jobName = (String) context
					.getProperty(JobExecutionContext.SOURCEJOBID);
			plevel = (String) context
					.getProperty(jobName + "_" + JobExecutionContext.PLEVEL);
		}
		return isTimeZoneAgnostic(context, plevel,
				jobName);
	}

	private static List<String> pLevels = new ArrayList<String>();

	public static List<String> getTimeZoneAgnosticPlevels(
			WorkFlowContext context) throws JobExecutionException {
		if (pLevels.isEmpty()) {
			populateListWithQueryResult(
					QueryConstants.TIMEZONE_AGNOSTIC_PLEVELS, pLevels, context);
			if (pLevels.isEmpty()) {
				pLevels.add("WEEK");
				pLevels.add("MONTH");
			}
		}
		return pLevels;
	}

	private static List<String> timeZoneAgnosticJobs = new ArrayList<String>();

	public static List<String> getTimeZoneAgnosticJobs(WorkFlowContext context)
			throws JobExecutionException {
		if (timeZoneAgnosticJobs.isEmpty()) {
			populateListWithQueryResult(QueryConstants.TIMEZONE_AGNOSTIC_JOBS,
					timeZoneAgnosticJobs, context);
			if (timeZoneAgnosticJobs.isEmpty()) {
				timeZoneAgnosticJobs
						.add("Perf_CDOMHTTPSTATS_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_CSUBDOMHTTPSTAT_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_DUSRSPAPPUSGSTA_1_DAY_AggregateJob");
				timeZoneAgnosticJobs.add("Perf_EMAILSTATS_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_CAT_DEV_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_CAT_USG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_DEVICE_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_DEV_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_USAGE_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_APP_USG_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_CAT_DEV_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_CAT_USG_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs.add("Perf_OTT_DEVICE_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_DEVICE_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEGACY_CAT_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEG_APP_CC_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEG_APP_USR_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEG_CAT_CC_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEG_CAT_USR_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_LEG_USG_MAX_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_MOST_USED_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_TOT_SUBS_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_TOT_SUB_SEG_1_DAY_AggregateJob");
				timeZoneAgnosticJobs
						.add("Perf_OTT_WEBSITES_1_DAY_AggregateJob");
				timeZoneAgnosticJobs.add("Perf_POP3STATS_1_DAY_AggregateJob");
			}
		}
		return timeZoneAgnosticJobs;
	}

	private static void populateListWithQueryResult(String query,
			List<String> list, WorkFlowContext context)
			throws JobExecutionException {
		QueryExecutor executor = new QueryExecutor();
		List resultList = executor.executeMetadatasqlQueryMultiple(query,
				context);
		if (resultList != null && !resultList.isEmpty()) {
			for (Object result : resultList) {
				if (result != null) {
					list.add(result.toString());
				}
			}
		}
	}
		
	public static boolean isTimezoneEnabledAndNotAgnostic(WorkFlowContext context)
			throws WorkFlowExecutionException {
		return TimeZoneUtil.isTimeZoneEnabled(context)
				&& !(TimeZoneUtil.isTimeZoneAgnostic(context));
	}
}


package com.rijin.scheduler.jobexecution.hive;

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.containsNone;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.settings.GlobalSettings;
import com.rijin.scheduler.jobexecution.hive.settings.ILoadQueryHints;
import com.rijin.scheduler.jobexecution.hive.settings.Job;
import com.rijin.scheduler.jobexecution.hive.settings.QueryHints;
import com.rijin.scheduler.jobexecution.hive.settings.QuerySettings;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;

public abstract class DbConfigurator implements ILoadQueryHints {

	private static final String HADOOP_FILEREADER = System.getenv("HADOOP_FILEREADER");

	private static final String FILE_READER_CLASS = "FILE_READER_CLASS";

	protected static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DbConfigurator.class);

	protected static final Map<String, String> queryHintFileMap = new HashMap<String, String>();
	static {
		queryHintFileMap.put("HIVE", "hive_settings.xml");
		queryHintFileMap.put("SPARK", "spark_settings.xml");
	}

	protected static final Map<String, Long> fileTimeStampMap = new HashMap<String, Long>();

	protected static final Map<String, Map<String, QueryHints>> queryHintMap = new LinkedHashMap<String, Map<String, QueryHints>>();

	protected static final Map<String, Map<String, QueryHints>> queryHintPatternMap = new LinkedHashMap<String, Map<String, QueryHints>>();

	protected static final Map<String, Map<String, QueryHints>> patternMap = new LinkedHashMap<String, Map<String, QueryHints>>();

	private static final Map<String, Map<String, Boolean>> connectionUrlMap = new LinkedHashMap<String, Map<String, Boolean>>();

	protected Set<String> hiveSettings = new LinkedHashSet<String>();

	protected XStream xstream = new XStream();

	FileReader fileReader;

	public Map<String, Map<String, QueryHints>> getQueryhintmap() {
		return queryHintMap;
	}

	public Map<String, Map<String, QueryHints>> getQueryhintpatternmap() {
		return queryHintPatternMap;
	}

	public Map<String, Map<String, QueryHints>> getPatternmap() {
		return patternMap;
	}

	public List<String> getQueryHints(String jobId, String dbName)
			throws WorkFlowExecutionException {
		initializeHiveSettings(dbName);
		if (queryHintMap.get(dbName) != null) {
	private QueryHints updateQueryHints(Job jobName,
			Map<String, QueryHints> map, String keyValue) {
		List<String> updatedHint = new ArrayList<String>();
		QueryHints queryHints = new QueryHints();
		if (map != null && map.containsKey(keyValue)
				&& jobName.getQueryHints().getHint() != null) {
			if (map.get(keyValue).getHint() != null) {
				updatedHint = map.get(keyValue).getHint();
			}
			updatedHint.addAll(jobName.getQueryHints().getHint());
			queryHints.setHint(updatedHint);
		} else {
			queryHints = jobName.getQueryHints();
		}
		return queryHints;
	}

	private void updateDbConnectionURL(Job job, String dbName) {
		String jobName = isNotEmpty(job.getPattern()) ? job.getPattern()
				: job.getName();
		Map<String, Boolean> urlMap = connectionUrlMap.get(dbName);
		if (job.isCustomDbUrl() != null) {
			if (urlMap == null) {
				urlMap = new LinkedHashMap<String, Boolean>();
			}
			urlMap.put(jobName, job.isCustomDbUrl());
			connectionUrlMap.put(dbName, urlMap);
		}
		LOGGER.debug("connectionUrlMap: {}", connectionUrlMap);
	}

	@Override
	public Set<String> addHiveSettings(String jobName,
			Map<String, QueryHints> queryhintmap, Set<String> hiveSettings) {
		if (queryhintmap.get(jobName) != null
				&& queryhintmap.get(jobName).getHint() != null) {
			for (String queryHints : queryhintmap.get(jobName).getHint()) {
				updateHiveSettings(hiveSettings, queryHints);
			}
		}
		return hiveSettings;
	}

	private void updateHiveSettings(Set<String> hiveSettings,
			String queryHints) {
		if (!hiveSettings.contains(queryHints)) {
			hiveSettings.add(queryHints);
		} else {
			hiveSettings.remove(queryHints);
			hiveSettings.add(queryHints);
		}
	}

	protected static void handleXStreamExceptions(Exception e)
			throws WorkFlowExecutionException {
		if (DbConfigurator.queryHintMap.isEmpty()) {
			throw new WorkFlowExecutionException(e.getMessage(), e);
		} else {
			LOGGER.warn(
					"Modified hivesettings.xml not in proper foramt. Hence going ahead with previous hiveSettings.xml configurations "
							+ e.getMessage());
		}
	}

	protected static boolean isHiveSettingsModified(String dbName) {
		return (fileTimeStampMap.get(dbName) != getFileTimeStamp(dbName));
	}

	protected Set<String> setPatternMap(String jobId, Set<String> hiveSettings,
			String dbName) {
		if (queryHintPatternMap.get(dbName) != null) {
			for (Entry<String, QueryHints> pattern : queryHintPatternMap
					.get(dbName).entrySet()) {
				if (jobId.matches(pattern.getKey())) {
					// If a pattern Map already exists for the jobId, then add
					// in
					// hints for all the other pattern which the jobID matches.
					if (patternMap.get(dbName) != null
							&& patternMap.get(dbName).containsKey(jobId)
							&& patternMap.get(dbName).get(jobId)
									.getHint() != null) {
						QueryHints queryHintsMap = patternMap.get(dbName)
								.get(jobId);
						QueryHints finalQueryHint = new QueryHints();
						List<String> existingHints = new ArrayList<String>(
								queryHintsMap.getHint());
						if (pattern.getValue().getHint() != null) {
							for (String patternHintsObj : pattern.getValue()
									.getHint()) {
								if (!existingHints.contains(patternHintsObj)) {
									existingHints.add(patternHintsObj);
								}
							}
						}
						finalQueryHint.setHint(existingHints);
						patternMap.get(dbName).put(jobId, finalQueryHint);
					} else {
						if (patternMap.get(dbName) == null) {
							HashMap<String, QueryHints> finalqueryHintMap = new HashMap<String, QueryHints>();
							finalqueryHintMap.put(jobId, pattern.getValue());
							patternMap.put(dbName, finalqueryHintMap);
						} else {
							patternMap.get(dbName).put(jobId,
									pattern.getValue());
						}
					}
				}
			}
		}
		return addHiveSettings(jobId, patternMap.get(dbName) == null
				? new HashMap<String, QueryHints>() : patternMap.get(dbName),
				hiveSettings);
	}
	
	protected FluentIterable<String> replace(Collection<String> source, String key, String value) {
		Function<String, String> function = new Function<String, String>() {
			@Override
			public String apply(String input) {
				return containsNone(input, key) ? input : input.replace(key, value);
			}
		};
		return FluentIterable.from(source).transform(function);
	}
		
}

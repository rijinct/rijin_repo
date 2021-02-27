
package com.rijin.scheduler.jobexecution.hive.settings;

import java.util.Map;
import java.util.Set;

public interface ILoadQueryHints {

	Set<String> addHiveSettings(String jobName,
			Map<String, QueryHints> hintsMap, Set<String> hiveSettings);
}
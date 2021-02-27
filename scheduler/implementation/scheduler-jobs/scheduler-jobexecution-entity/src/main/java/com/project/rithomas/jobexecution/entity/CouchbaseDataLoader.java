package com.project.rithomas.jobexecution.entity;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.io.IOException;

import com.rijin.analytics.cacheservice.command.HiveToCouchbaseDataExporter;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.entity.client.CacheServiceRestClient;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class CouchbaseDataLoader {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(CouchbaseDataLoader.class);

	private String jobName;

	private String hiveTableName;

	private String parallelThreadCount;

	public CouchbaseDataLoader(String jobName, String hiveTableName, String parallelThreadCount) {
		this.jobName = jobName;
		this.hiveTableName = hiveTableName;
		this.parallelThreadCount = parallelThreadCount;
	}

	public boolean load() throws WorkFlowExecutionException {
		boolean jobExecuted = false;
		LOGGER.info(
				"Loading data from {} table to couchbase using {} job with {} parallel threads.",
				hiveTableName, jobName,
				parallelThreadCount != null ? parallelThreadCount
				: "default number of");
		if (isKubernetesEnvironment()) {
			CacheServiceRestClient apiClient = getCacheServiceRestClient();
			try {
				if (null != apiClient.execute()) {
					jobExecuted = true;
				}
			} catch (IOException exception) {
				LOGGER.error("Exception occurred when loading data to Couchbase", exception);
				throw new WorkFlowExecutionException("Hive to couchbase loading failed", exception);
			}
		} else {
			HiveToCouchbaseDataExporter cacheDataExporter = getHiveToCouchbaseDataExporter();
			jobExecuted = cacheDataExporter.loadDataToCouchbase(jobName, hiveTableName, parallelThreadCount);
		}
		LOGGER.info("Data loading complete for {} job.", jobName);
		return jobExecuted;
	}

	private CacheServiceRestClient getCacheServiceRestClient() {
		return new CacheServiceRestClient(jobName, hiveTableName, parallelThreadCount);
	}

	private HiveToCouchbaseDataExporter getHiveToCouchbaseDataExporter() {
		return new HiveToCouchbaseDataExporter();
	}

	private static boolean isKubernetesEnvironment() {
		return toBoolean(System.getenv(schedulerConstants.IS_K8S));
	}
}

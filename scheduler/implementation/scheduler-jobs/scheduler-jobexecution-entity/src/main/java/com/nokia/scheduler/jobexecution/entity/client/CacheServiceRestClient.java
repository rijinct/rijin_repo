
package com.rijin.scheduler.jobexecution.entity.client;

import org.apache.commons.lang3.math.NumberUtils;

import com.google.gson.JsonObject;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.client.BaseApiClient;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.RequestBody;

public class CacheServiceRestClient extends BaseApiClient {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CacheServiceRestClient.class);

	private static final String KEY_CACHESERVICE_AGENT_ENDPOINT = "CACHESERVICE_AGENT_ENDPOINT";

	private static final String HEADER_ACCEPT = "application/vnd.rijin-cacheagent-schedules-v1+json";

	private static final String HEADER_CONTENT_TYPE = "application/vnd.rijin-cacheagent-schedules-v1+json";

	private static final MediaType JSON = MediaType
			.parse("application/vnd.rijin-cacheagent-schedules-v1+json");

	private static final String CACHE_SERVICE_READ_TIMEOUT_MINUTES = "CACHE_SERVICE_READ_TIMEOUT_MINUTES";

	private String jobName;

	private String hiveTableName;

	private String parallelThreadCount;

	public CacheServiceRestClient(String jobName, String hiveTableName,
			String parallelThreadCount) {
		this.jobName = jobName;
		this.hiveTableName = hiveTableName;
		this.parallelThreadCount = parallelThreadCount;
	}

	@Override
	protected String getRequestUrl() {
		String url = buildURL(System.getenv(KEY_CACHESERVICE_AGENT_ENDPOINT));
		LOGGER.debug("Cache Service Agent URL : {}", url);
		return url;
	}

	@Override
	protected String getAcceptHeader() {
		return HEADER_ACCEPT;
	}

	@Override
	protected String getContentType() {
		return HEADER_CONTENT_TYPE;
	}

	@Override
	protected int getReadTimeout() {
		return NumberUtils.toInt(System.getenv(CACHE_SERVICE_READ_TIMEOUT_MINUTES),
				DEFAULT_READ_TIMEOUT_MINUTES);
	}

	@Override
	protected RequestBody createRequestBody() {
		JsonObject json = new JsonObject();
		json.addProperty("sourceHiveTableName", hiveTableName);
		json.addProperty("sourceJobName", jobName);
		json.addProperty("noOfThreads", parallelThreadCount);
		RequestBody body = RequestBody.create(JSON, json.toString());
		return body;
	}
}


package com.project.rithomas.jobexecution.common.util;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.client.BaseApiClient;
import com.squareup.okhttp.HttpUrl;

public class DataAvailabilityCacheServiceRestClient extends BaseApiClient {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DataAvailabilityCacheServiceRestClient.class);

	private static final String HEADER_ACCEPT = "application/vnd.rijin-data-availability-v1+json";

	private static final String KEY_DATA_AVAILABILITY_CACHESERVICE_ENDPOINT = "DATA_AVAILABILITY_CACHESERVICE_ENDPOINT";

	private String dataAvailabilityKey;

	@Override
	protected String getRequestUrl() {
		String url = buildURL(
				System.getenv(KEY_DATA_AVAILABILITY_CACHESERVICE_ENDPOINT));
		LOGGER.debug("Data Availability Rest URL : {}", url);
		return url;
	}

	protected String getAcceptHeader() {
		return HEADER_ACCEPT;
	}

	@Override
	protected String buildURL(String endpoint) {
		HttpUrl.Builder urlBuilder = HttpUrl.parse(endpoint).newBuilder();
		urlBuilder.addQueryParameter("dataAvailabilityKey",
				dataAvailabilityKey);
		return urlBuilder.build().toString();
	}

	public void setDataAvailabilityKey(String key) {
		dataAvailabilityKey = key;
	}
}

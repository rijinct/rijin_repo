
package com.rijin.scheduler.jobexecution.entity.client;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.client.BaseApiClient;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.RequestBody;

public class ETLRestApiClient extends BaseApiClient {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ETLRestApiClient.class);

	private static final String HEADER_ACCEPT = "application/json";

	private static final String ETL_TOPOLOGY_REFRESH_ENDPOINT = "TOPOLOGY_REFRESH_ENDPOINT";

	private String dimension = null;

	public ETLRestApiClient(String dimensionName) {
		dimension = dimensionName;
	}

	@Override
	protected String getRequestUrl() {
		String url = buildURL(System.getenv(ETL_TOPOLOGY_REFRESH_ENDPOINT));
		LOGGER.debug("ETL Rest URL : {}", url);
		return url;
	}

	@Override
	protected String getAcceptHeader() {
		return HEADER_ACCEPT;
	}

	@Override
	protected RequestBody createRequestBody() {
		return RequestBody.create(null, "");
	}

	@Override
	protected String buildURL(String endpoint) {
		HttpUrl.Builder urlBuilder = HttpUrl.parse(endpoint).newBuilder();
		urlBuilder.addQueryParameter("message", dimension);
		return urlBuilder.build().toString();
	}
}


package com.rijin.scheduler.jobexecution.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.HttpHeaders;

import org.apache.logging.log4j.ThreadContext;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Request.Builder;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public abstract class BaseApiClient {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(BaseApiClient.class);

	private static final long HTTP_CONNECTION_TIMEOUT = 10;

	private static final String HEADER_CORRELATION_ID = "correlation-id";

	private static final String JOBNAME_ROUTER = "JOBNAME";

	public static final int DEFAULT_READ_TIMEOUT_MINUTES = 60;

	public String execute() throws IOException {
		return execute(createRequest());
	}

	protected String execute(Request request) throws IOException {
		String responseValue = null;
		OkHttpClient httpClient = createClient();
		Response response = httpClient.newCall(request).execute();
		LOGGER.debug("Response Code : {} Successful : {}", response.code(),
				response.isSuccessful());
		if (response.isSuccessful()) {
			responseValue = response.body().string();
		}
		return responseValue;
	}

	private OkHttpClient createClient() {
		OkHttpClient httpClient = new OkHttpClient();
		httpClient.setConnectTimeout(HTTP_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
		httpClient.interceptors().add(new RetryInterceptor());
		httpClient.setReadTimeout(getReadTimeout(), TimeUnit.MINUTES);
		return httpClient;
	}

	protected int getReadTimeout() {
		return DEFAULT_READ_TIMEOUT_MINUTES;
	}

	protected Request createRequest() {
		String url = getRequestUrl();
		Builder requestBuilder = new Request.Builder().url(url);
		addHeader(requestBuilder, HttpHeaders.ACCEPT, getAcceptHeader());
		addHeader(requestBuilder, HttpHeaders.CONTENT_TYPE, getContentType());
		addHeader(requestBuilder, HEADER_CORRELATION_ID,
				ThreadContext.get(JOBNAME_ROUTER));
		RequestBody body = createRequestBody();
		addBody(requestBuilder, body);
		return requestBuilder.build();
	}

	private void addHeader(Builder requestBuilder, String headerKey,
			String headerValue) {
		if (headerValue != null) {
			requestBuilder.addHeader(headerKey, headerValue);
		}
	}

	private void addBody(Builder requestBuilder, RequestBody requestBody) {
		if (requestBody != null) {
			requestBuilder.post(requestBody);
		}
	}

	protected String getAcceptHeader() {
		return null;
	}

	protected String getContentType() {
		return null;
	}

	protected RequestBody createRequestBody() {
		return null;
	}

	protected abstract String getRequestUrl();

	protected String buildURL(String endpoint) {
		HttpUrl.Builder urlBuilder = HttpUrl.parse(endpoint).newBuilder();
		return urlBuilder.build().toString();
	}
}

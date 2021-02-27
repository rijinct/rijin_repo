package com.rijin.scheduler.jobexecution.client;

import java.io.IOException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.squareup.okhttp.Interceptor;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class RetryInterceptor implements Interceptor {

	private static final int MAXIMUM_RETRY_COUNT = 3;
	private static final long RETRY_WAIT_INTERVAL = 5000L;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(RetryInterceptor.class);

	@Override
	public Response intercept(Chain chain) throws IOException {

		Request request = chain.request();
		Response response = chain.proceed(request);
		int retryCount = 0;

		while (!response.isSuccessful() && retryCount < MAXIMUM_RETRY_COUNT) {
			try {
				Thread.sleep(RETRY_WAIT_INTERVAL);
			} catch (InterruptedException excep) {
				throw new IOException(excep);
			}
			LOGGER.debug("HTTP Request Failed - Retrying with Retry Count : {}", retryCount);
			retryCount++;
			response = chain.proceed(request);
		}
		return response;
	}

}

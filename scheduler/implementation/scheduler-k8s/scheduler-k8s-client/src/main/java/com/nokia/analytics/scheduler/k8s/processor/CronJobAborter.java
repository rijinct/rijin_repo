
package com.rijin.analytics.scheduler.k8s.processor;

import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;

public class CronJobAborter extends CronBaseProcessor<Object> {

	@Override
	protected String getTemplateName() {
		return null;
	}

	@Override
	protected Call getCall(String resourceName, Object request)
			throws ApiException {
		throw new UnsupportedOperationException(
				"ABORT operation is currently not supported.");
	}
}

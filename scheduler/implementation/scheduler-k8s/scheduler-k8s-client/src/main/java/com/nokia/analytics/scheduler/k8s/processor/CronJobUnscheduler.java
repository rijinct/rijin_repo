
package com.rijin.analytics.scheduler.k8s.processor;

import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;

public class CronJobUnscheduler extends CronBaseProcessor<Object> {

	@Override
	protected String getTemplateName() {
		return null;
	}

	@Override
	protected Call getCall(String resourceName, Object request)
			throws ApiException {
		throw new UnsupportedOperationException(
				"Unschedule operation is currently not supported.");
	}
}

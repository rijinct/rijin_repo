
package com.rijin.analytics.scheduler.k8s.processor;

import java.io.IOException;
import java.util.List;

import org.springframework.http.HttpStatus;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.Job;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;

public class CronJobDeletor extends CronBaseProcessor<Job> {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronJobDeletor.class);

	@Override
	protected String getTemplateName() {
		return null;
	}

	@Override
	protected List<Job> getResourceGroup() {
		return getJobDetails().getSchedule().getJobs();
	}

	@Override
	protected String getRequestKey(Job job) {
		return job.getName();
	}

	@Override
	protected Call getCall(String cronJobName, Object request)
			throws ApiException {
		return getCronApiExecutor().deleteCronCall(cronJobName);
	}

	@Override
	protected String getErrorKey() {
		return "cronJobDeletionFailed";
	}

	@Override
	protected ApiResponse<Job> executeRequest(String jobName, Object request)
			throws ApiException, IOException {
		ApiResponse<Job> response = null;
		try {
			response = super.executeRequest(jobName, request);
		} catch (ApiException ex) {
			handleException(ex, jobName);
		}
		return response;
	}

	protected void handleException(ApiException ex, String jobName)
			throws ApiException {
		if (HttpStatus.NOT_FOUND.value() == ex.getCode()) {
			LOGGER.warn(
					"{} Cronjob does not exist. Skipping deletion of the CronJob",
					jobName);
			LOGGER.debug("Api Response : {}", ex.getResponseBody());
		} else {
			throw ex;
		}
	}
}

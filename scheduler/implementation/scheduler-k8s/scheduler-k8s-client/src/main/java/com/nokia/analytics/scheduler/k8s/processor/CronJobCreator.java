
package com.rijin.analytics.scheduler.k8s.processor;

import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.COMPONENT_GROUP;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRONJOB_NAME;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRON_EXPRESSION;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB_GROUP;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB_PROPERTIES;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.NAMESPACE;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.SERVICE_ACCOUNT;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.SERVICE_PORT;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.BUSYBOX_IMAGE_PATH;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.BUSYBOX_IMAGE_PULL_POLICY;
import static com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation.CREATE;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.stringtemplate.v4.ST;

import com.google.common.reflect.TypeToken;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.k8s.converters.MapToJsonConverter;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;
import io.kubernetes.client.models.V1beta1CronJob;

public class CronJobCreator extends CronBaseProcessor<Job> {

	private static final String KEY_BUSYBOX_IMAGE_PULL_POLICY = "BUSYBOX_IMAGE_PULL_POLICY";

	private static final String KEY_BUSYBOX_IMAGE_PATH = "BUSYBOX_IMAGE_PATH";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronJobCreator.class);

	protected static final String DEFAULT_CRON_EXPRESSION = "0 0 1 1 *";

	private static final String KEY_COMPONENT_GROUP = "COMPONENT_GROUP";

	private static final String KEY_SERVICE_PORT = "SERVICE_PORT";

	@Override
	protected String getTemplateName() {
		return CREATE.getTemplateName();
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
	protected void fillTemplate(ST template, Job job) {
		template.add(NAMESPACE, getNamespace());
		template.add(BUSYBOX_IMAGE_PATH, getBusyBoxImagePath());
		template.add(BUSYBOX_IMAGE_PULL_POLICY, getBusyBoxImagePullPolicy());
		template.add(SERVICE_ACCOUNT, getServiceAccount());
		template.add(CRONJOB_NAME, getNameInK8sFormat(job.getName()));
		template.add(JOB, job);
		template.add(CRON_EXPRESSION, DEFAULT_CRON_EXPRESSION);
		template.add(JOB_GROUP, job.getJobGroup());
		template.add(COMPONENT_GROUP, getComponentGroup());
		template.add(SERVICE_PORT, getServicePort());
		template.add(JOB_PROPERTIES,
				MapToJsonConverter.toJson(getJobProperties()));
	}

	@Override
	protected Call getCall(String resourceName, Object request)
			throws ApiException {
		return getCronApiExecutor().createCronCall(request);
	}

	@Override
	protected String getErrorKey() {
		return "cronJobCreationFailed";
	}

	@Override
	protected Type getResponseType() {
		return new TypeToken<V1beta1CronJob>() {

			private static final long serialVersionUID = -8894541488468715400L;
		}.getType();
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
		if (HttpStatus.CONFLICT.value() == ex.getCode()) {
			LOGGER.warn(
					"{} Cronjob already exist. Skipping creation of the CronJob",
					jobName);
			LOGGER.debug("Api Response : {}", ex.getResponseBody());
		} else {
			throw ex;
		}
	}

	private String getComponentGroup() {
		return System.getenv(KEY_COMPONENT_GROUP);
	}

	private String getServicePort() {
		return System.getenv(KEY_SERVICE_PORT);
	}

	private String getBusyBoxImagePath() {
		return System.getenv(KEY_BUSYBOX_IMAGE_PATH);
	}

	private String getBusyBoxImagePullPolicy() {
		return System.getenv(KEY_BUSYBOX_IMAGE_PULL_POLICY);
	}
}

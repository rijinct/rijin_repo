
package com.rijin.analytics.scheduler.k8s.processor;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.reflect.TypeToken;
import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.converters.YamlToJsonConverter;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;
import io.kubernetes.client.models.V1beta1CronJob;
import io.kubernetes.client.models.V1beta1CronJobList;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class schedulerK8sBulkOperationsProcessor extends K8sBaseProcessor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(schedulerK8sBulkOperationsProcessor.class);

	public schedulerK8sBulkOperationsProcessor() {
		super();
	}

	public void processRequest(JobSchedulingOperation operation,
			String jobGroup) {
		try {
			initializeApiExecutor();
			List<String> jobs = getJobNames(jobGroup);
			ST template = group.getInstanceOf(operation.getTemplateName());
			Object cronPatch = YamlToJsonConverter.toJson(template.render());
			overrideHeaders();
			for (String jobName : jobs) {
				LOGGER.debug("Performing operation {} on job {}", operation,
						jobName);
				Call callToExecute = getCronApiExecutor().patchCronCall(jobName,
						cronPatch);
				ApiResponse<Object> response = getCronApiExecutor().executeCall(
						callToExecute, new TypeToken<V1beta1CronJob>() {

							private static final long serialVersionUID = -851935465679052813L;
						}.getType());
				LOGGER.debug("operation {}, job {}, status :{}", operation,
						jobName, response.getStatusCode());
			}
		} catch (IOException | ApiException e) {
			LOGGER.error("Exception occurred when performing operation {}", e);
			throw new InternalServerErrorException();
		}
	}

	private List<String> getJobNames(String jobGroup) throws ApiException {
		Call callToExecute = getCronApiExecutor().listCronCall(jobGroup);
		ApiResponse<Object> response = getCronApiExecutor().executeCall(
				callToExecute, new TypeToken<V1beta1CronJobList>() {

					private static final long serialVersionUID = -851935465679052813L;
				}.getType());
		Function<V1beta1CronJob, String> function = getListJobNamesFunction();
		return FluentIterable
				.from(((V1beta1CronJobList) response.getData()).getItems())
				.transform(function).toList();
	}

	private Function<V1beta1CronJob, String> getListJobNamesFunction() {
		return new Function<V1beta1CronJob, String>() {

			@Override
			public String apply(V1beta1CronJob cronJob) {
				return cronJob.getMetadata().getName();
			}
		};
	}
}

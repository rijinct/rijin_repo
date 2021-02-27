
package com.rijin.analytics.scheduler.k8s.processor;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.stringtemplate.v4.ST;

import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.exceptions.ServiceException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.k8s.converters.YamlToJsonConverter;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;

public abstract class CronBaseProcessor<T> extends K8sBaseProcessor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronBaseProcessor.class);

	protected static final String K8S_CRON_JOBS_BASE_URI = "/apis/batch/v1beta1/namespaces/{namespace}";

	private JobSchedulingData jobDetails;

	private Map<String, Object> jobProperties;

	private static final String DEFAULT_CRON_SERVICEACCOUNT = "default";

	private static final String KEY_CRON_SERVICEACCOUNT = "CRON_SERVICEACCOUNT";

	public CronBaseProcessor() {
		super();
	}

	@Override
	public schedulerManagementOperationResult processRequest(
			JobSchedulingData jobSchedulingData,
			Map<String, Object> jobProperties) {
		setJobProperties(jobProperties);
		schedulerManagementOperationResult result = new schedulerManagementOperationResult();
		try {
			setJobDetails(jobSchedulingData);
			initializeApiExecutor();
			Map<String, String> requests = getRequests();
			for (Entry<String, String> entry : requests.entrySet()) {
				Object jsonRequest = YamlToJsonConverter.toJson(entry.getValue());
				LOGGER.info("Request received for job {} is {}", entry.getKey(),
						jsonRequest);
				executeRequest(entry.getKey(), jsonRequest);
				LOGGER.info("Successfully executed the request for job {}",
						entry.getKey());
			}
		} catch (Exception exception) {
			LOGGER.error("Exception occured when processing request",
					exception);
			result.setError(true);
			result.addErrorMsg(exception.getMessage());
		}
		return result;
	}

	@Override
	public schedulerManagementOperationResult processRequest(
			JobSchedulingData jobSchedulingData) {
		return processRequest(jobSchedulingData, new HashMap<>());
	}

	protected String getServiceAccount() {
		return StringUtils.defaultString(System.getenv(KEY_CRON_SERVICEACCOUNT),
				DEFAULT_CRON_SERVICEACCOUNT);
	}

	Map<String, String> getRequests() {
		Map<String, String> requests = new HashMap<>();
		List<T> resourceGroup = getResourceGroup();
		if (resourceGroup != null) {
			for (T resource : resourceGroup) {
				String request = createRequest(resource);
				requests.put(getNameInK8sFormat(getRequestKey(resource)),
						request);
			}
		}
		return requests;
	}

	String createRequest(T resource) {
		String request = "";
		String templateName = getTemplateName();
		if (isTemplateDefined(templateName)) {
			ST template = group.getInstanceOf(templateName);
			if (template == null) {
				throw new InternalServerErrorException("Failed to load the template : " + templateName);
			} 
			fillTemplate(template, resource);
			request = template.render();
		}
		return request;
	}

	boolean isTemplateDefined(String templateName) {
		return isNotEmpty(templateName);
	}

	protected List<T> getResourceGroup() {
		return null;
	}

	protected String getRequestKey(T job) {
		return "";
	}

	protected void fillTemplate(ST template, T job) {
	}

	ApiResponse<T> executeRequest(String resourceName, Object request)
			throws ApiException, IOException {
		Call callToExecute = getCall(resourceName, request);
		ApiResponse<T> response = getCronApiExecutor()
				.executeCall(callToExecute, getResponseType());
		LOGGER.debug("Execution response received {}", response.getData());
		return response;
	}

	protected ServiceException getException(HttpStatus errorStatus) {
		String errorKey = getErrorKey();
		if (errorKey != null) {
			return new ServiceException(errorKey, errorStatus);
		}
		return new InternalServerErrorException();
	}

	protected String getErrorKey() {
		return null;
	}

	protected abstract Call getCall(String resourceName, Object request)
			throws ApiException, IOException;

	protected Type getResponseType() {
		return null;
	}

	protected abstract String getTemplateName();

	protected String getPath() {
		return K8S_CRON_JOBS_BASE_URI;
	}

	public JobSchedulingData getJobDetails() {
		return jobDetails;
	}

	public void setJobDetails(JobSchedulingData jobDetails) {
		this.jobDetails = jobDetails;
	}

	public Map<String, Object> getJobProperties() {
		return jobProperties;
	}

	public void setJobProperties(Map<String, Object> jobProperties) {
		this.jobProperties = jobProperties;
	}
}
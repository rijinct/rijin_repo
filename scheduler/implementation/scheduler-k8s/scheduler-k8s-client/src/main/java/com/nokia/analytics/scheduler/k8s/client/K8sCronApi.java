
package com.rijin.analytics.scheduler.k8s.client;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;
import io.kubernetes.client.util.ClientBuilder;

public class K8sCronApi {

	private ApiClient apiClient;

	private final String cronUri;

	private Map<String, String> headers = null;

	private String[] authNames = new String[] { "BearerToken" };

	private static final String KEY_COMPONENT_GROUP = "COMPONENT_GROUP";

	private static final String KEY_ENABLE_DEBUG_FOR_K8S_API_CLIENT = "ENABLE_DEBUG_FOR_K8S_API_CLIENT";

	public K8sCronApi(String namespace) throws IOException {
		this.apiClient = ClientBuilder.cluster().build();
		this.apiClient.setDebugging(isDebugEnabledForApiClient());
		this.cronUri = "/apis/batch/v1beta1/namespaces/" + namespace
				+ "/cronjobs";
		initializeHeaders();
	}

	private void initializeHeaders() {
		this.headers = new HashMap<String, String>() {

			private static final long serialVersionUID = 5561210527827736846L;
			{
				put(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
				put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
			}
		};
	}

	public ApiClient getApiClient() {
		return apiClient;
	}

	public void setApiClient(ApiClient apiClient) {
		this.apiClient = apiClient;
	}

	public Call listCronCall(String jobGroup) throws ApiException {
		return apiClient.buildCall(cronUri + getLabelSelector(jobGroup),
				HttpMethod.GET.name(), null, null, null, headers, null,
				authNames, null);
	}

	public Call getCronCall(String cronJobName) throws ApiException {
		return apiClient.buildCall(cronUri + "/" + cronJobName,
				HttpMethod.GET.name(), null, null, null, headers, null,
				authNames, null);
	}

	public Call createCronCall(Object cronRequest) throws ApiException {
		return apiClient.buildCall(cronUri, HttpMethod.POST.name(), null, null,
				cronRequest, headers, null, authNames, null);
	}

	public Call patchCronCall(String cronJobName, Object cronPatch)
			throws ApiException {
		return apiClient.buildCall(cronUri + "/" + cronJobName,
				HttpMethod.PATCH.name(), null, null, cronPatch, headers, null,
				authNames, null);
	}

	public Call deleteCronCall(String cronJobName) throws ApiException {
		return apiClient.buildCall(cronUri + "/" + cronJobName,
				HttpMethod.DELETE.name(), null, null, null, headers, null,
				authNames, null);
	}

	public <T> ApiResponse<T> executeCall(Call callToExecute, Type responseType)
			throws ApiException {
		return apiClient.execute(callToExecute, responseType);
	}

	public void clearHeaders() {
		this.headers.clear();
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	private String getLabelSelector(String jobGroup) {
		StringBuilder labelSelectorUrl = new StringBuilder();
		labelSelectorUrl.append("?labelSelector=componentGroup%3D");
		labelSelectorUrl.append(getComponentGroup());
		if (StringUtils.isNotEmpty(jobGroup)) {
			labelSelectorUrl.append(",jobGroup%3D");
			labelSelectorUrl.append(jobGroup);
		}
		return labelSelectorUrl.toString();
	}

	private String getComponentGroup() {
		return System.getenv(KEY_COMPONENT_GROUP);
	}

	private boolean isDebugEnabledForApiClient() {
		return BooleanUtils
				.toBoolean(System.getenv(KEY_ENABLE_DEBUG_FOR_K8S_API_CLIENT));
	}
}
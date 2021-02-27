
package com.rijin.analytics.scheduler.k8s.processor;

import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRONJOB_NAME;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.CRON_EXPRESSION;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.JOB_PROPERTIES;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.NAMESPACE;
import static com.rijin.analytics.scheduler.k8s.common.TemplateConstants.SERVICE_ACCOUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.stringtemplate.v4.ST;

import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.exceptions.ServiceException;
import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.k8s.client.K8sCronApi;
import com.rijin.analytics.scheduler.k8s.converters.MapToJsonConverter;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CronJobCreator.class, CronBaseProcessor.class,
		MapToJsonConverter.class })
public class CronJobCreatorTest {

	Map<String, Object> jobProperties = new HashMap<>();
	
	@Spy
	CronJobCreator jobCreator = new CronJobCreator();

	@Mock
	K8sCronApi mockK8sCronApi;

	@Mock
	Call mockCall;

	@Mock
	private JobSchedulingData mockJobDetails;

	@Mock
	private Schedule mockSchedule;

	@Mock
	private Job mockJob;

	@Mock
	private ST mockTemplate;

	private List<Job> listOfMockJobs = new ArrayList<>();

	@Mock
	private ApiResponse<Job> mockResponse;

	private static final String CRON_JOBNAME = "sms-1-perf-sms-segg-1-hour-aggregatejob";

	private static final String DEFAULT_NAMESPACE = "default";

	private static final String CA4CI_SERVICE_ACCOUNT = "ca4ci";

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		listOfMockJobs.add(mockJob);
		PowerMockito.mockStatic(MapToJsonConverter.class);
		
		jobProperties.put("abc", "123");
		jobProperties.put("def", "456");
	}

	@Test
	public void testGetErrorKey() {
		assertEquals("cronJobCreationFailed", jobCreator.getErrorKey());
	}

	@Test
	public void testGetCall() throws ApiException {
		doReturn(mockCall).when(mockK8sCronApi).createCronCall(anyString());
		doReturn(mockK8sCronApi).when(jobCreator).getCronApiExecutor();
		assertEquals(mockCall, jobCreator.getCall("resourceName", "{}"));
		verify(jobCreator, times(1)).getCronApiExecutor();
		verify(mockK8sCronApi, times(1)).createCronCall(anyString());
	}

	@Test
	public void testGetResponseType() {
		assertEquals("io.kubernetes.client.models.V1beta1CronJob",
				jobCreator.getResponseType().getTypeName());
	}

	@Test
	public void testGetTemplateName() {
		assertEquals("create", jobCreator.getTemplateName());
	}

	@Test
	public void testGetResourceGroup() {
		doReturn(mockJobDetails).when(jobCreator).getJobDetails();
		doReturn(mockSchedule).when(mockJobDetails).getSchedule();
		doReturn(listOfMockJobs).when(mockSchedule).getJobs();
		assertEquals(listOfMockJobs, jobCreator.getResourceGroup());
		verify(jobCreator, times(1)).getJobDetails();
		verify(mockJobDetails, times(1)).getSchedule();
		verify(mockSchedule, times(1)).getJobs();
	}

	@Test
	public void testGetRequestKeyJob() {
		doReturn("Perf_SMS_SEGG_1_HOUR_AggregateJob").when(mockJob).getName();
		assertEquals("Perf_SMS_SEGG_1_HOUR_AggregateJob",
				jobCreator.getRequestKey(mockJob));
		verify(mockJob, times(1)).getName();
	}

	@Test
	public void testFillTemplateSTJob() throws Exception {
		doReturn(DEFAULT_NAMESPACE).when(jobCreator).getNamespace();
		doReturn(CA4CI_SERVICE_ACCOUNT).when(jobCreator).getServiceAccount();
		doReturn(CRON_JOBNAME).when(jobCreator).getNameInK8sFormat(anyString());
		doReturn("Perf_SMS_SEGG_1_HOUR_AggregateJob").when(mockJob).getName();
		doReturn("SMS_1").when(mockJob).getJobGroup();
		doReturn(jobProperties).when(jobCreator).getJobProperties();
		PowerMockito.when(MapToJsonConverter.toJson(jobProperties))
				.thenReturn("{}");
		jobCreator.fillTemplate(mockTemplate, mockJob);
		verify(mockTemplate, times(1)).add(NAMESPACE, DEFAULT_NAMESPACE);
		verify(mockTemplate, times(1)).add(SERVICE_ACCOUNT,
				CA4CI_SERVICE_ACCOUNT);
		verify(mockTemplate, times(1)).add(CRONJOB_NAME, CRON_JOBNAME);
		verify(mockTemplate, times(1)).add(JOB, mockJob);
		verify(mockTemplate, times(1)).add(CRON_EXPRESSION, "0 0 1 1 *");
		verify(mockTemplate, times(1)).add(JOB_PROPERTIES, "{}");
		verify(mockJob, times(1)).getName();
	}

	@Test(expected = InternalServerErrorException.class)
	public void testFillTemplateSTJobException() {
		doReturn(DEFAULT_NAMESPACE).when(jobCreator).getNamespace();
		doReturn(CA4CI_SERVICE_ACCOUNT).when(jobCreator).getServiceAccount();
		doReturn(CRON_JOBNAME).when(jobCreator).getNameInK8sFormat(anyString());
		doReturn(jobProperties).when(jobCreator).getJobProperties();
		PowerMockito.when(MapToJsonConverter.toJson(Mockito.anyMap()))
				.thenThrow(InternalServerErrorException.class);
		doReturn("Perf_SMS_SEGG_1_HOUR_AggregateJob").when(mockJob).getName();
		jobCreator.fillTemplate(mockTemplate, mockJob);
		verify(mockTemplate, times(1)).add(NAMESPACE, DEFAULT_NAMESPACE);
		verify(mockTemplate, times(1)).add(SERVICE_ACCOUNT,
				CA4CI_SERVICE_ACCOUNT);
		verify(mockTemplate, times(1)).add(CRONJOB_NAME, CRON_JOBNAME);
		verify(mockTemplate, times(1)).add(JOB, mockJob);
		verify(mockTemplate, times(1)).add(CRON_EXPRESSION, "0 0 1 1 *");
		verify(mockTemplate, times(0)).add(JOB_PROPERTIES, "{}");
		verify(mockJob, times(1)).getName();
	}

	@Test
	public void testExecuteRequest()
			throws ServiceException, ApiException, IOException {
		Mockito.doReturn(mockResponse).when((CronBaseProcessor<Job>) jobCreator)
				.executeRequest(CRON_JOBNAME, mockCall);
		ApiResponse<Job> response = jobCreator.executeRequest(CRON_JOBNAME,
				mockCall);
		assertNotNull(response);
	}

	@Test
	public void testExecuteRequestNullResponse() {
		try {
			Mockito.doReturn(null).when((CronBaseProcessor<Job>) jobCreator)
					.executeRequest(CRON_JOBNAME, mockCall);
			ApiResponse<Job> response = jobCreator.executeRequest(CRON_JOBNAME,
					mockCall);
			assertNull(response);
		} catch (Exception ex) {
			assertFalse(true);
		}
	}

	@Test
	public void testHandleException() {
		try {
			String responseBody = "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"cronjobs.batch \\\"perf-blr-1-day-aggregatejob\\\" already exists\",\"reason\":\"AlreadyExists\",\"details\":{\"name\":\"perf-blr-1-day-aggregatejob\",\"group\":\"batch\",\"kind\":\"cronjobs\"},\"code\":409}";
			ApiException mockException = new ApiException(
					HttpStatus.CONFLICT.value(), null, responseBody);
			jobCreator.handleException(mockException, CRON_JOBNAME);
		} catch (Exception e) {
			fail("Exception not expected here");
		}
	}

	@Test(expected = ApiException.class)
	public void testHandleExceptionThrowsApiExceptionForOtherErrorCodes()
			throws ApiException {
		ApiException mockException = new ApiException(500, null,
				"Some Response body");
		jobCreator.handleException(mockException, CRON_JOBNAME);
	}
}

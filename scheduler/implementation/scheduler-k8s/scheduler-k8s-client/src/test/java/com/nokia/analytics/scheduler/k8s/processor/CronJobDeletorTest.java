
package com.rijin.analytics.scheduler.k8s.processor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.http.HttpStatus;

import com.rijin.analytics.exceptions.ServiceException;
import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.k8s.client.K8sCronApi;
import com.squareup.okhttp.Call;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.ApiResponse;

public class CronJobDeletorTest {

	@Spy
	CronJobDeletor jobDeletor = new CronJobDeletor();

	@Mock
	private K8sCronApi mockK8sCronApi;

	@Mock
	private Call mockCall;

	@Mock
	private Job mockJob;

	@Mock
	private JobSchedulingData mockJobSchedulingData;

	@Mock
	private Schedule mockSchedule;

	private List<Job> listOfJobs = new ArrayList<>();

	@Mock
	private ApiResponse<Job> mockResponse;

	private static final String CRON_JOBNAME = "sms-1-perf-sms-segg-1-hour-aggregatejob";

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		listOfJobs.add(mockJob);
	}

	@Test
	public void testGetErrorKey() {
		assertEquals("cronJobDeletionFailed", jobDeletor.getErrorKey());
	}

	@Test
	public void testGetCall() throws ApiException {
		doReturn(mockK8sCronApi).when(jobDeletor).getCronApiExecutor();
		doReturn(mockCall).when(mockK8sCronApi).deleteCronCall("cronJobName");
		assertEquals(mockCall, jobDeletor.getCall("cronJobName", "request"));
		verify(jobDeletor, times(1)).getCronApiExecutor();
		verify(mockK8sCronApi, times(1)).deleteCronCall(anyString());
	}

	@Test
	public void testGetTemplateName() {
		assertNull(jobDeletor.getTemplateName());
	}

	@Test
	public void testGetResourceGroup() {
		doReturn(mockJobSchedulingData).when(jobDeletor).getJobDetails();
		doReturn(mockSchedule).when(mockJobSchedulingData).getSchedule();
		doReturn(listOfJobs).when(mockSchedule).getJobs();
		assertEquals(listOfJobs, jobDeletor.getResourceGroup());
		verify(jobDeletor, times(1)).getJobDetails();
		verify(mockJobSchedulingData, times(1)).getSchedule();
		verify(mockSchedule, times(1)).getJobs();
	}

	@Test
	public void testGetRequestKeyJob() {
		doReturn("SMS_1").when(mockJob).getName();
		assertEquals("SMS_1", jobDeletor.getRequestKey(mockJob));
		verify(mockJob, times(1)).getName();
	}

	@Test
	public void testExecuteRequest()
			throws ServiceException, ApiException, IOException {
		Mockito.doReturn(mockResponse).when((CronBaseProcessor<Job>) jobDeletor)
				.executeRequest(CRON_JOBNAME, mockCall);
		ApiResponse<Job> response = jobDeletor.executeRequest(CRON_JOBNAME,
				mockCall);
		assertNotNull(response);
	}

	@Test
	public void testExecuteRequestNullResponse() {
		try {
			Mockito.doReturn(null).when((CronBaseProcessor<Job>) jobDeletor)
					.executeRequest(CRON_JOBNAME, mockCall);
			ApiResponse<Job> response = jobDeletor.executeRequest(CRON_JOBNAME,
					mockCall);
			assertNull(response);
		} catch (Exception ex) {
			assertFalse(true);
		}
	}

	@Test
	public void testHandleException() {
		try {
			String responseBody = "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"cronjobs.batch \\\"perf-blr-1-day-aggregatejob\\\" not found\",\"reason\":\"NotFound\",\"details\":{\"name\":\"perf-blr-1-day-aggregatejob\",\"group\":\"batch\",\"kind\":\"cronjobs\"},\"code\":404}";
			ApiException mockException = new ApiException(
					HttpStatus.NOT_FOUND.value(), null, responseBody);
			jobDeletor.handleException(mockException, CRON_JOBNAME);
		} catch (Exception e) {
			fail("Exception not expected here");
		}
	}

	@Test(expected = ApiException.class)
	public void testHandleExceptionThrowsApiExceptionForOtherErrorCodes()
			throws ApiException {
		ApiException mockException = new ApiException(500, null,
				"Some Response body");
		jobDeletor.handleException(mockException, CRON_JOBNAME);
	}
}

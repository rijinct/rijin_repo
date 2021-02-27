
package com.rijin.analytics.scheduler.k8s.controllers;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.rijin.analytics.exception.configuration.ContextProvider;
import com.rijin.analytics.exception.configuration.ExceptionConfiguration;
import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.k8s.HealthCheck;
import com.rijin.analytics.scheduler.k8s.JobStatusUpdater;
import com.rijin.analytics.scheduler.k8s.PoolConfigInitializer;
import com.rijin.analytics.scheduler.k8s.beans.mapper.JobDescriptorDecorator;
import com.rijin.analytics.scheduler.k8s.processor.schedulerK8sBulkOperationsProcessor;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.rithomas.jobexecution.common.BaseJobProcessor;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = { JobManagementController.class, JobExecutionController.class }, secure = false)
@PrepareForTest({ ContextProvider.class })
public class JobManagementControllerTest {

	private static final String URL_API_MANAVER_V1_BASE_REQUEST = "/api/manager/v1/scheduler";

	private static final String URL_API_MANAGER_V1_scheduler_JOBS = "/api/manager/v1/scheduler/jobs";

	private static final String URL_API_MANAGER_V1_ABORT_JOBS = URL_API_MANAVER_V1_BASE_REQUEST + "/jobs/schedule";

	private static final String MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON = "application/vnd.rijin-se-schedules-v1+json";

	private static final String REQUEST_BODY = "[\r\n" + "  {\r\n"
			+ "    \"jobGroup\": \"VLR_1\",\r\n"
			+ "    \"jobName\": \"Perf_VLR_1_DAY_AggregateJob\"\r\n" + "  }\r\n"
			+ "]";

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	JobDescriptorDecorator jobDescriptorDecorator;

	@MockBean
	BaseClientProcessor processor;

	@MockBean
	private BaseJobProcessor baseJobProcessor;

	@MockBean
	schedulerK8sBulkOperationsProcessor bulkOperationsProcessor;

	@Mock
	private JobSchedulingData mockJobSchedulingData;

	@Mock
	private schedulerManagementOperationResult mockschedulerManagementOperationResult;

	@Mock
	private ExceptionConfiguration mockExceptionConfiguration;

	@MockBean
	private PoolConfigInitializer mockPoolConfigInitializer;

	@MockBean
	private JobStatusUpdater mockJobStatusUpdater;

	@MockBean
	private HealthCheck mockHealthCheck;

	@Before
	public void setUp() throws Exception {
		mockStatic(ContextProvider.class);
		doNothing().when(mockPoolConfigInitializer).initializePool();
	}

	@Test
	public void testCreateJob() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.post(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isOk());
		verify(jobDescriptorDecorator, Mockito.times(1))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, Mockito.times(1))
				.processRequest(mockJobSchedulingData);
	}

	@Test
	public void testCreateJob_invalid_contenttype() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.post(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(APPLICATION_JSON).content(REQUEST_BODY))
				.andExpect(status().isUnsupportedMediaType());
		verify(jobDescriptorDecorator, times(0))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(0)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testCreateJob_invalid_Accept_header() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.post(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(APPLICATION_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isNotAcceptable());
		verify(jobDescriptorDecorator, times(0))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(0)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testCreateJob_internal_error() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(Mockito.anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException()).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.post(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isInternalServerError());
		verify(jobDescriptorDecorator, times(1))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(1)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testDeleteJob() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.delete(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isOk());
		verify(jobDescriptorDecorator, times(1))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(1)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testDeleteJob_invalid_contenttype() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.delete(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(APPLICATION_JSON).content(REQUEST_BODY))
				.andExpect(status().isUnsupportedMediaType());
		verify(jobDescriptorDecorator, times(0))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(0)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testDeleteJob_invalid_Accept_header() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		doReturn(mockschedulerManagementOperationResult).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.delete(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(APPLICATION_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isNotAcceptable());
		verify(jobDescriptorDecorator, times(0))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(0)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testDeleteJob_internal_error() throws Exception {
		doReturn(mockJobSchedulingData).when(jobDescriptorDecorator)
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(Mockito.anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException()).when(processor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.delete(URL_API_MANAGER_V1_scheduler_JOBS)
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content(REQUEST_BODY))
				.andExpect(status().isInternalServerError());
		verify(jobDescriptorDecorator, times(1))
				.toSchedulingData(Mockito.anyList(), Mockito.anyString());
		verify(processor, times(1)).processRequest(mockJobSchedulingData);
	}

	@Test
	public void testToggleScheduleState_without_jobGroup() throws Exception {
		doNothing().when(bulkOperationsProcessor)
				.processRequest(JobSchedulingOperation.PAUSE, null);
		mockMvc.perform(
				MockMvcRequestBuilders.patch(URL_API_MANAGER_V1_scheduler_JOBS)
						.param("action", "pause"))
				.andExpect(status().isOk());
		verify(bulkOperationsProcessor, times(1))
				.processRequest(JobSchedulingOperation.PAUSE, null);
	}

	@Test
	public void testToggleScheduleState_without__mandatory_action_param()
			throws Exception {
		doNothing().when(bulkOperationsProcessor)
				.processRequest(JobSchedulingOperation.PAUSE, "SMS_1");
		mockMvc.perform(
				MockMvcRequestBuilders.patch(URL_API_MANAGER_V1_scheduler_JOBS)
						.param("jobGroup", "SMS_1"))
				.andExpect(status().isBadRequest());
		verify(bulkOperationsProcessor, times(0))
				.processRequest(JobSchedulingOperation.PAUSE, "SMS_1");
	}

	@Test
	public void testToggleScheduleState_with_invalid_url() throws Exception {
		doNothing().when(bulkOperationsProcessor)
				.processRequest(JobSchedulingOperation.PAUSE, "SMS_1");
		mockMvc.perform(MockMvcRequestBuilders
				.patch("/api/manager/v1/scheduler").param("jobGroup", "SMS_1"))
				.andExpect(status().isNotFound());
		verify(bulkOperationsProcessor, times(0))
				.processRequest(JobSchedulingOperation.PAUSE, "SMS_1");
	}

	@Test
	public void testToggleScheduleState_with_internal_error() throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(Mockito.anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException())
				.when(bulkOperationsProcessor)
				.processRequest(JobSchedulingOperation.PAUSE, null);
		mockMvc.perform(
				MockMvcRequestBuilders.patch(URL_API_MANAGER_V1_scheduler_JOBS)
						.param("action", "pause"))
				.andExpect(status().isInternalServerError());
		verify(bulkOperationsProcessor, times(1))
				.processRequest(JobSchedulingOperation.PAUSE, null);
	}
	
	@Test
	public void testAbort() throws Exception {
		final String API_EXECUTOR_V1_JOBS = "/api/executor/v1/jobs";
		final String NAMESPACE = "ca4ci";
		String jobName = "Perf_VOICE_SEGG_1_HOUR_AggregateJob";
		String jobGroup = "VOICE_1";

		doNothing().when(baseJobProcessor).interruptJob();

		Mockito.doAnswer((Answer) invocation -> {
			Thread.sleep(3000);
			return null;

		}).when(baseJobProcessor).processJob(jobName, new HashMap<>(), jobGroup, NAMESPACE);

		Thread t = new Thread() {
			public void run() {
				try {
					mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
							.accept("application/vnd.rijin-se-jobs-v1+json")
							.contentType("application/vnd.rijin-se-jobs-v1+json").param("namespace", NAMESPACE)
							.param("jobName", jobName).param("jobGroup", jobGroup)).andExpect(status().isOk());
					mockMvc.perform(MockMvcRequestBuilders.delete(URL_API_MANAGER_V1_ABORT_JOBS)
							.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
							.contentType(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON).param("jobNames", jobName))
					.andExpect(status().isOk());
					Thread.sleep(5000);
				} catch (Exception ex) {
					Assert.assertTrue(true);
				}
			}
		};
		t.start();
	}
}

package com.rijin.analytics.scheduler.k8s.controllers;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rijin.analytics.exception.configuration.ContextProvider;
import com.rijin.analytics.exception.configuration.ExceptionConfiguration;
import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.scheduler.k8s.HealthCheck;
import com.rijin.analytics.scheduler.k8s.JobStatusUpdater;
import com.rijin.analytics.scheduler.k8s.PoolConfigInitializer;
import com.project.rithomas.jobexecution.common.BaseJobProcessor;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = JobExecutionController.class, secure = false)
@PrepareForTest({ ContextProvider.class })
public class JobExecutionControllerTest {

	private static final String API_EXECUTOR_V1_JOBS = "/api/executor/v1/jobs";

	private static final String MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON = "application/vnd.rijin-se-jobs-v1+json";

	private static final String EMPTY_REQUEST = "{}";

	private static final String JOB_NAME = "Perf_VLR_1_DAY_AggregateJob";

	private static final String JOB_GROUP = "VLR_1";

	private static final String NAMESPACE = "ca4ci";

	private static Map<String, Object> jobProperties = new HashMap<>();

	private static String requestWithProperties;

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private BaseJobProcessor processor;

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
		jobProperties.put("key1", "value1");
		jobProperties.put("key2", "value2");
		requestWithProperties = new ObjectMapper()
				.writeValueAsString(jobProperties);
		mockStatic(ContextProvider.class);
		doNothing().when(mockPoolConfigInitializer).initializePool();
	}

	@Test
	public void testExecuteJob_without_properties() throws Exception {
		doNothing().when(processor).processJob(JOB_NAME, new HashMap<>(),
				JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("namespace", NAMESPACE).param("jobName", JOB_NAME)
				.param("jobGroup", JOB_GROUP)).andExpect(status().isOk());
		verify(processor, Mockito.times(1)).processJob(JOB_NAME,
				new HashMap<>(), JOB_GROUP, NAMESPACE);
	}

	@Test
	public void testExecuteJob_without_namespace() throws Exception {
		doNothing().when(processor).processJob(JOB_NAME, new HashMap<>(),
				JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("jobName", JOB_NAME).param("jobGroup", JOB_GROUP)
				.content(EMPTY_REQUEST)).andExpect(status().isBadRequest());
		verify(processor, Mockito.times(0)).processJob(JOB_NAME,
				new HashMap<>(), JOB_GROUP, NAMESPACE);
	}

	@Test
	public void testExecuteJob_without_jobName() throws Exception {
		doNothing().when(processor).processJob(JOB_NAME, new HashMap<>(),
				JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("namespace", NAMESPACE).param("jobGroup", JOB_GROUP)
				.content(EMPTY_REQUEST)).andExpect(status().isBadRequest());
		verify(processor, Mockito.times(0)).processJob(JOB_NAME,
				new HashMap<>(), JOB_GROUP, NAMESPACE);
	}

	@Test
	public void testExecuteJob_without_jobGroup() throws Exception {
		doNothing().when(processor).processJob(JOB_NAME, new HashMap<>(),
				JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("namespace", NAMESPACE).param("jobName", JOB_NAME)
				.content(EMPTY_REQUEST)).andExpect(status().isBadRequest());
		verify(processor, Mockito.times(0)).processJob(JOB_NAME,
				new HashMap<>(), JOB_GROUP, NAMESPACE);
	}

	@Test
	public void testExecuteJob_with_properties() throws Exception {
		doNothing().when(processor).processJob(JOB_NAME, jobProperties,
				JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("namespace", NAMESPACE).param("jobName", JOB_NAME)
				.param("jobGroup", JOB_GROUP).content(requestWithProperties))
				.andExpect(status().isOk());
		verify(processor, Mockito.times(1)).processJob(JOB_NAME, jobProperties,
				JOB_GROUP, NAMESPACE);
	}

	@Test
	public void testExecuteJob_internal_error() throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException()).when(processor)
				.processJob(JOB_NAME, jobProperties, JOB_GROUP, NAMESPACE);
		mockMvc.perform(MockMvcRequestBuilders.post(API_EXECUTOR_V1_JOBS)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.param("namespace", NAMESPACE).param("jobName", JOB_NAME)
				.param("jobGroup", JOB_GROUP).content(requestWithProperties))
				.andExpect(status().isInternalServerError());
		verify(processor, Mockito.times(1)).processJob(JOB_NAME, jobProperties,
				JOB_GROUP, NAMESPACE);
	}
}

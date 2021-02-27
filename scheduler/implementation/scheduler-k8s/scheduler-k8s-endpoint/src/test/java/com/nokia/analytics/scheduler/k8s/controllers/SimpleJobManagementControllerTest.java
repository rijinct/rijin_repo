
package com.rijin.analytics.scheduler.k8s.controllers;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.springframework.http.MediaType;
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
import com.rijin.analytics.scheduler.k8s.beans.mapper.SimpleJobDescriptorDecorator;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = SimpleJobManagementController.class, secure = false)
@PrepareForTest({ ContextProvider.class })
public class SimpleJobManagementControllerTest {

	private static final String URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE = "/api/manager/v1/scheduler/jobs/simple";

	private static final String MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON = "application/vnd.rijin-se-schedules-v1+json";

	private static final String REQUEST_BODY = "[\r\n" + "  {\r\n"
			+ "    \"jobGroup\": \"VLR_1\",\r\n"
			+ "    \"jobName\": \"Perf_VLR_1_DAY_AggregateJob\",\r\n"
			+ "    \"startTime\": \"2018.11.02 11:15:00\"\r\n" + "  }\r\n"
			+ "]";

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private SimpleJobDescriptorDecorator mockSimpleJobDescriptorDecorator;

	@MockBean
	private BaseClientProcessor mockBaseClientProcessor;

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
		when(mockSimpleJobDescriptorDecorator.toSchedulingData(anyList(),
				Mockito.anyString())).thenReturn(mockJobSchedulingData);
		doNothing().when(mockPoolConfigInitializer).initializePool();
	}

	@Test
	public void testScheduleSimpleJob_valid_request() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.content(REQUEST_BODY)).andExpect(status().isOk()).andReturn();
	}

	@Test
	public void testScheduleSimpleJob_invalid_ContentType() throws Exception {
		when(mockBaseClientProcessor.processRequest(mockJobSchedulingData))
				.thenReturn(mockschedulerManagementOperationResult);
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(MediaType.APPLICATION_JSON).content(REQUEST_BODY))
				.andExpect(status().isUnsupportedMediaType()).andReturn();
	}

	@Test
	public void testScheduleCronJob_invalid_Accept() throws Exception {
		when(mockBaseClientProcessor.processRequest(mockJobSchedulingData))
				.thenReturn(mockschedulerManagementOperationResult);
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MediaType.APPLICATION_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.content(REQUEST_BODY)).andExpect(status().isNotAcceptable())
				.andReturn();
	}

	@Test
	public void testScheduleCronJob_internal_error() throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException())
				.when(mockBaseClientProcessor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.content(REQUEST_BODY))
				.andExpect(status().isInternalServerError()).andReturn();
	}

	@Test
	public void testScheduleCronJob_No_body_passed_in_request()
			throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException())
				.when(mockBaseClientProcessor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON))
				.andExpect(status().isBadRequest()).andReturn();
	}

	@Test
	public void testScheduleCronJob_invalid_request() throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException())
				.when(mockBaseClientProcessor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_JOBS_SIMPLE)
				.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.contentType(
						MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
				.content("{abc:123}")).andExpect(status().isBadRequest())
				.andReturn();
	}

	@Test
	public void testScheduleCronJob_invalid_path() throws Exception {
		PowerMockito.when(ContextProvider.getBean(ExceptionConfiguration.class))
				.thenReturn(mockExceptionConfiguration);
		when(mockExceptionConfiguration.getProperty(anyString()))
				.thenReturn("errorCode");
		doThrow(new InternalServerErrorException())
				.when(mockBaseClientProcessor)
				.processRequest(mockJobSchedulingData);
		mockMvc.perform(
				MockMvcRequestBuilders.patch("/api/manager/v1/scheduler/jobs")
						.accept(MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.contentType(
								MEDIATYPE_APPLICATION_VND_RIJIN_SE_SCHEDULES_V1_JSON)
						.content("{abc:123}"))
				.andExpect(status().isNotFound()).andReturn();
	}
}


package com.rijin.analytics.scheduler.k8s.controllers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.rijin.analytics.exception.configuration.ContextProvider;
import com.rijin.analytics.k8s.client.configmap.ConfigMapClient;
import com.rijin.analytics.scheduler.k8s.HealthCheck;
import com.rijin.analytics.scheduler.k8s.JobStatusUpdater;
import com.rijin.analytics.scheduler.k8s.PoolConfigInitializer;
import com.rijin.analytics.scheduler.k8s.validator.HintsSchemaValidator;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@WebMvcTest(value = ConfigurationManagementController.class, secure = false)
@PrepareForTest({ ContextProvider.class, System.class,
		ConfigurationManagementController.class })
public class ConfigurationManagementControllerTest {

	private static final String URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS = "/api/manager/v1/scheduler/configuration/hints";

	private static final String CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML = "application/vnd.rijin-se-settings-v1+xml";

	private static final String DATA_KEY = "hive_settings.xml";

	private static final String NAMESPACE = "default";

	private static final String CONFIG_MAP_NAME = "ca4ci-schedulerengine-query-settings";

	private static final String CONFIG_MAP_CONTENT = "<?xml version=\"1.0\"?>\\n  <QuerySettings>    \\n    <GlobalSettings>\\n      <Job>        \\n        <Name>DEFAULT</Name>\\n        <QueryHints>\\n          <hint>CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'</hint>\\n        </QueryHints>\\n      </Job>\\n      <Job>\\n        <Pattern>Entity.*CorrelationJob</Pattern>\\n        <QueryHints>\\n          <hint>SET mapreduce.job.queuename=root.rijin.ca4ci.dimension</hint>\\n        </QueryHints>\\n      </Job>\\n    </GlobalSettings>\\n    <Hive2Settings>\\n      <Job>\\n        <Name>DEFAULT</Name>\\n        <QueryHints>\\n          <hint>CREATE TEMPORARY FUNCTION initcap AS 'org.apache.hadoop.hive.ql.udf.UDFInitCap'</hint>\\n        </QueryHints>\\n      </Job>\\n    </Hive2Settings>\\n  </QuerySettings>";

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private ConfigMapClient mockConfigMapClient;

	@MockBean
	private PoolConfigInitializer poolConfigInitializer;

	@MockBean
	private JobStatusUpdater mockJobStatusUpdater;

	@MockBean
	private HealthCheck mockHealthCheck;
	
	@MockBean
	private HintsSchemaValidator hintsSchemaValidator;

	@Before
	public void setUp() throws Exception {
		mockStatic(System.class);
		PowerMockito.when(System.getenv("RELEASE_NAMESPACE"))
				.thenReturn(NAMESPACE);
		PowerMockito.when(System.getenv("QUERY_SETTINGS_CONFIGMAP_NAME"))
				.thenReturn(CONFIG_MAP_NAME);
		PowerMockito.when(System.getenv("HIVE_SETTINGS_KEY"))
				.thenReturn(DATA_KEY);
		when(mockConfigMapClient.readConfigMap(NAMESPACE, CONFIG_MAP_NAME,
				DATA_KEY)).thenReturn(CONFIG_MAP_CONTENT);
		doNothing().when(mockConfigMapClient).patchConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY, CONFIG_MAP_CONTENT);
		doNothing().when(hintsSchemaValidator).validate(Mockito.anyString(), Mockito.anyString());
	}

	@Test
	public void test_fetchConfigMap() throws Exception {
		MvcResult result = mockMvc
				.perform(MockMvcRequestBuilders
						.get(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
						.contentType(
								CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML)
						.param("queryEngine", "HIVE"))
				.andExpect(status().isOk()).andReturn();
		assertEquals(CONFIG_MAP_CONTENT,
				result.getResponse().getContentAsString());
		verify(mockConfigMapClient, times(1)).readConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY);
	}

	@Test
	public void test_fetchConfigMap_missing_mandatory_queryParam()
			throws Exception {
		mockMvc.perform(MockMvcRequestBuilders
				.get(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
				.contentType(
						CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML))
				.andExpect(status().isBadRequest()).andReturn();
		verify(mockConfigMapClient, times(0)).readConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY);
	}

	@Test
	public void test_fetchConfigMap_additional_queryengine_Param()
			throws Exception {
		MvcResult result = mockMvc
				.perform(MockMvcRequestBuilders
						.get(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
						.contentType(
								CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML)
						.param("queryEngine", "HIVE")
						.param("additional_param", "values"))
				.andExpect(status().isOk()).andReturn();
		assertEquals(CONFIG_MAP_CONTENT,
				result.getResponse().getContentAsString());
		verify(mockConfigMapClient, times(1)).readConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY);
	}

	@Test
	public void test_updateConfigMap() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
				.contentType(
						CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML)
				.param("queryEngine", "HIVE").content(CONFIG_MAP_CONTENT))
				.andExpect(status().isOk()).andReturn();
		verify(mockConfigMapClient, times(1)).patchConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY, CONFIG_MAP_CONTENT);
	}

	@Test
	public void test_updateConfigMap_invalid_ContentType() throws Exception {
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
				.param("queryEngine", "HIVE").content(CONFIG_MAP_CONTENT))
				.andExpect(status().isUnsupportedMediaType()).andReturn();
		verify(mockConfigMapClient, times(0)).patchConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY, CONFIG_MAP_CONTENT);
	}

	@Test
	public void test_updateConfigMap_missing_mandatory_queryEngine_param()
			throws Exception {
		mockMvc.perform(MockMvcRequestBuilders
				.patch(URL_API_MANAGER_V1_scheduler_CONFIGURATION_HINTS)
				.contentType(
						CONTENT_TYPE_APPLICATION_VND_RIJIN_SE_SETTINGS_V1_XML)
				.content(CONFIG_MAP_CONTENT)).andExpect(status().isBadRequest())
				.andReturn();
		verify(mockConfigMapClient, times(0)).patchConfigMap(NAMESPACE,
				CONFIG_MAP_NAME, DATA_KEY, CONFIG_MAP_CONTENT);
	}
}


package com.rijin.analytics.scheduler.k8s;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.cem.sai.jdbc.PoolConfig;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ PoolConfigInitializer.class, HiveConfigurationProvider.class,
		ConnectionManager.class, GetDBResource.class, QueryExecutor.class })
public class PoolConfigInitializerTest {

	@Spy
	private PoolConfigInitializer poolConfigInitializer = new PoolConfigInitializer();;

	@Mock
	private HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	private ConnectionManager mockConnectionManager;

	@Mock
	private WorkFlowContext mockWorkFlowContext;

	@Mock
	private JobExecutionContext mockContext;

	@Mock
	private GetDBResource mockGetDBResource;

	@Mock
	private QueryExecutor mockQueryExecutor;

	@Before
	public void setUp() throws Exception {
		mockStatic(HiveConfigurationProvider.class);
		mockStatic(ConnectionManager.class);
		mockStatic(GetDBResource.class);
	}

	@Test
	public void testInitialiseConnection() throws Exception {
		List<String> mockHiveHints = new ArrayList<>();
		Map<String, PoolConfig> map = preparePoolConfig();
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		PowerMockito.doNothing().when(ConnectionManager.class);
		ConnectionManager.configureConnPool(map.get("hive"), mockHiveHints,
				"hive");
		when(mockHiveConfigurationProvider.getPoolConfig()).thenReturn(map);
		when(mockHiveConfigurationProvider.getQueryHints(null, "hive"))
				.thenReturn(mockHiveHints);
		poolConfigInitializer.initializePool();
		verify(mockHiveConfigurationProvider, times(1)).getPoolConfig();
		verify(mockHiveConfigurationProvider, times(1)).getQueryHints(null,
				"hive");
	}

	@Test(expected = RuntimeException.class)
	public void testInitialiseConnection_throws_exception() throws Exception {
		List<String> mockHiveHints = new ArrayList<>();
		Map<String, PoolConfig> map = preparePoolConfig();
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		doThrow(new RuntimeException("mock-exception"))
				.when(ConnectionManager.class);
		ConnectionManager.configureConnPool(map.get("hive"), mockHiveHints,
				"hive");
		when(mockHiveConfigurationProvider.getPoolConfig()).thenReturn(map);
		when(mockHiveConfigurationProvider.getQueryHints(null, "hive"))
				.thenReturn(mockHiveHints);
		poolConfigInitializer.initializePool();
		verify(mockHiveConfigurationProvider, times(1)).getPoolConfig();
		verify(mockHiveConfigurationProvider, times(1)).getQueryHints(null,
				"hive");
	}

	public Map<String, PoolConfig> preparePoolConfig() throws IOException {
		Map<String, PoolConfig> poolConfigDBMap = new HashMap<String, PoolConfig>();
		PoolConfig poolConfig = new PoolConfig();
		poolConfig.setDbDriver("dbDriver");
		poolConfig.setDbUrl("dbUrl");
		poolConfig.setCustomDBUrl("customDBUrl");
		poolConfig.setUserName("hiveUserName");
		poolConfig.setEnableConnectionPool(true);
		poolConfig.setMinConnections(0);
		poolConfig.setMaxConnections(100);
		poolConfig.setIdleConnTestInMin(60);
		poolConfig.setIdleConnTestSql("idleConnectionTestSQL");
		poolConfig.setTestConnection(true);
		poolConfigDBMap.put("hive", poolConfig);
		return poolConfigDBMap;
	}
}

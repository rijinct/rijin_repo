
package com.rijin.analytics.scheduler.k8s;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ JobStatusUpdater.class, HiveConfigurationProvider.class,
		ConnectionManager.class, GetDBResource.class, QueryExecutor.class })
public class JobStatusUpdaterTest {

	@Spy
	private JobStatusUpdater jobStatusUpdater = new JobStatusUpdater();

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
		whenNew(JobExecutionContext.class).withNoArguments()
				.thenReturn(mockContext);
		whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(mockGetDBResource);
		doNothing().when(mockGetDBResource)
				.retrievePostgresObjectProperties(mockContext);
	}

	@Test
	public void testUpdateJobStatus() throws Exception {
		doNothing().when(mockQueryExecutor).executePostgresqlUpdate(
				QueryConstants.SE_RESTART_UPDATE_QUERY, mockContext);
		jobStatusUpdater.updateJobsToError();
		verify(mockGetDBResource, times(1))
				.retrievePostgresObjectProperties(mockContext);
		verify(mockQueryExecutor, times(1)).executePostgresqlUpdate(
				QueryConstants.SE_RESTART_UPDATE_QUERY, mockContext);
	}

	@Test
	public void testUpdateJobStatus_throws_exception() throws Exception {
		Mockito.doThrow(new JobExecutionException("mock-exception"))
				.when(mockQueryExecutor).executePostgresqlUpdate(
						QueryConstants.SE_RESTART_UPDATE_QUERY, mockContext);
		jobStatusUpdater.updateJobsToError();
		verify(mockGetDBResource, times(1))
				.retrievePostgresObjectProperties(mockContext);
		verify(mockQueryExecutor, times(1)).executePostgresqlUpdate(
				QueryConstants.SE_RESTART_UPDATE_QUERY, mockContext);
	}
}

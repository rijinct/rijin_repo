
package com.project.rithomas.jobexecution.project;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DBConnectionManager.class, UpdateJobStatus.class,
		GetDBResource.class, FileSystem.class,
		RetrieveDimensionValues.class,
		ConnectionManager.class, HiveConfigurationProvider.class })
public class RedundanceCheckTest {

	RedundanceCheck check = new RedundanceCheck();

	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();

	DBConnectionManager mockDBConnectionManager = mock(DBConnectionManager.class);

	Connection mockConnection = mock(Connection.class);

	Statement mockStatement = mock(Statement.class);

	ResultSet mockResultSet = mock(ResultSet.class);

	ResultSetMetaData mockResultmetadata = mock(ResultSetMetaData.class);

	@Mock
	UpdateJobStatus updateJobStatus;
	
	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;


	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT, 1800l);
		PowerMockito.mockStatic(DBConnectionManager.class);
		mockStatic(ConnectionManager.class);
		mockStatic(HiveConfigurationProvider.class);
		
		PowerMockito.when(DBConnectionManager.getInstance()).thenReturn(
				mockDBConnectionManager);
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT, 1800l);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_LOCATION_1_CorrelationJob");
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance()).thenReturn(
				mockDBConnectionManager);
		
		PowerMockito.when(HiveConfigurationProvider.getInstance()).thenReturn(mockHiveConfigurationProvider);
		List<String> hints = new ArrayList();
		Mockito.when(mockHiveConfigurationProvider.getQueryHints(Mockito.anyString(), Mockito.anyString())).thenReturn(hints);
		
		
		PowerMockito.when(
				mockDBConnectionManager.getPostgresConnection(
						GetDBResource.getPostgreDriver(),
						GetDBResource.getPostgresUrl(),
						GetDBResource.getPostgresCacheUser(),
						GetDBResource.getPostgresCachePassword())).thenReturn(
				mockConnection);

		PowerMockito.when(mockConnection.createStatement()).thenReturn(
				mockStatement);
		QueryExecutor mockQueryExecutor = mock(QueryExecutor.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
		mockStatic(ConnectionManager.class);
		PowerMockito.when(mockDBConnectionManager
				.getConnection(Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(mockConnection);
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	@Test
	public void testNoDuplicateRows() throws WorkFlowExecutionException,
			SQLException {
		String sqlRedundantRegion = QueryConstants.SQL_REDUNDANT_REGION;
		PowerMockito.when(mockStatement.executeQuery(sqlRedundantRegion))
				.thenReturn(mockResultSet);
		PowerMockito.when(mockResultSet.next()).thenReturn(false);
		Assert.assertEquals(true, check.execute(context));
	}

	@Test
	public void testWithDuplicateRows() throws WorkFlowExecutionException,
			SQLException {
		PowerMockito.when(mockResultSet.getMetaData()).thenReturn(
				mockResultmetadata);
		PowerMockito.when(mockResultSet.getString("region")).thenReturn(
				"MOCK_REGION");
		PowerMockito.when(mockResultSet.getString("mcc")).thenReturn(
				"MOCK_REGION");
		PowerMockito.when(mockResultSet.getString("mnc")).thenReturn(
				"MOCK_REGION");
		String sqlRedundantRegion = QueryConstants.SQL_REDUNDANT_REGION;
		PowerMockito.when(mockStatement.executeQuery(sqlRedundantRegion))
				.thenReturn(mockResultSet);
		PowerMockito.when(mockResultSet.next()).thenReturn(true)
				.thenReturn(false);
		Assert.assertEquals(false, check.execute(context));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testsqlexception() throws WorkFlowExecutionException,
			SQLException {
		String sqlRedundantRegion = QueryConstants.SQL_REDUNDANT_REGION;
		PowerMockito.when(mockStatement.executeQuery(sqlRedundantRegion))
				.thenThrow(new SQLException("es_location failed"));
		Assert.assertEquals(false, check.execute(context));
	}

}

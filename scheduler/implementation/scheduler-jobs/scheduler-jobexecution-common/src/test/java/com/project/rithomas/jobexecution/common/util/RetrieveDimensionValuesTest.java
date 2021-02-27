
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.DbConfigurator;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { RetrieveDimensionValues.class, HiveJobRunner.class,
		DBConnectionManager.class, QueryExecutor.class })
public class RetrieveDimensionValuesTest {

	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();

	schedulerJobRunner jobRunner = null;

	@Mock
	ResultSet rs;

	@Mock
	Statement st;

	@Mock
	QueryExecutor queryExec;

	@Mock
	List<Object> list;

	@Mock
	Session session;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	Connection connection;

	@Mock
	DbConfigurator dbConfigurator;

	@Before
	public void setUp() throws Exception {
		mockStatic(DBConnectionManager.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(st);
	}

	@Test
	public void testGetAvailableDataCheck() throws JobExecutionException {
		context.setProperty(JobExecutionContext.SQL, testQuery);
		List<Object[]> resultSet = null;
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						QueryConstants.AVAILABLE_DATA_SGSN_QUERY, context))
				.thenReturn(resultSet);
		String availableData = RetrieveDimensionValues
				.getAvailableDataCheck(context);
		assertEquals("0", availableData);
	}

	@Test
	public void testCheckSource() throws JobExecutionException {
		context.setProperty(JobExecutionContext.SQL, query);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_HTTP_1_HOUR_AggregateJob");
		Object[] resultSet = new String[] { "Yes" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.AVAILABLE_DATA_HTTP_QUERY, context))
				.thenReturn(resultSet);
		String availableData2G = RetrieveDimensionValues
				.getAvailableType2G(context);
		assertEquals("Yes", availableData2G);
	}

	@Test
	public void testGetAvailableType2GGN()
			throws SQLException, JobExecutionException {
		context.setProperty(JobExecutionContext.SQL, query);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_GN_DG_1_HOUR_AggregateJob");
		List<Object> resultSet = null;
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						QueryConstants.AVAILABLE_DATA_GN_QUERY, context))
				.thenReturn(resultSet);
		String availableData2G = RetrieveDimensionValues
				.getAvailableType2G(context);
		assertEquals("Yes", availableData2G);
	}

	@Test
	public void testGetAvailableType4GHTTP()
			throws SQLException, JobExecutionException {
		context.setProperty(JobExecutionContext.SQL, query);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_GN_DG_1_HOUR_AggregateJob");
		Object[] resultSet = new String[] { "Yes" };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						QueryConstants.AVAILABLE_DATA_GN_4G_QUERY, context))
				.thenReturn(resultSet);
		String availableData2G = RetrieveDimensionValues
				.getAvailableType4G(context);
		assertEquals("Yes", availableData2G);
	}

	@Test
	public void testGetAvailableType4GException()
			throws SQLException, JobExecutionException {
		String jobId = "Entity_LOCATION_1_CorrelationJob";
		PowerMockito
				.when(dbConfigurator.shouldConnectToCustomDbUrl(
						Mockito.anyString(), Mockito.anyString()))
				.thenReturn(true);
		List<Object[]> resultSet = new ArrayList<Object[]>();
		List<Object> resultSet2 = new ArrayList<Object>();
		resultSet2.add("2");
		resultSet2.add("3");
		String jobId = "Entity_LOCATION_1_CorrelationJob";
		jobRunner = schedulerJobRunnerfactory.getRunner("HIVE", false);
		List<Object[]> resultSet = new ArrayList<Object[]>();
		List<Object> resultSet2 = new ArrayList<Object>();
		List<Object[]> resultSet = new ArrayList<Object[]>();
				QueryConstants.GET_MAX_REGION_QUERY, "Reg", "STRING", "HIVE" });
		List<Object[]> resultSet = new ArrayList<Object[]>();

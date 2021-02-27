package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { schedulerJobRunnerfactory.class,
		DBConnectionManager.class })
public class schedulerJobRunnerfactoryTest {

	WorkFlowContext context = new JobExecutionContext();

	schedulerJobRunner runner = null;

	@Mock
	DBConnectionManager mockDBConnectionManager;

	@Mock
	Connection mockConnection;

	@Mock
	Statement mockStatement;

	@Mock
	PreparedStatement mockPreparedStatement;

	@Mock
	ResultSet mockResultSet;

	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, true);
		PowerMockito.mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(mockDBConnectionManager);
		PowerMockito.when(mockDBConnectionManager
				.getConnection(Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(mockConnection);
		PowerMockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
	}

	@Test
	public void testGetRunner() throws Exception {
		runner = schedulerJobRunnerfactory.getRunner("HIVE", (boolean) context
				.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
		assertTrue(runner instanceof HiveJobRunner);
		assertFalse(runner instanceof SparkJobRunner);
		runner = schedulerJobRunnerfactory.getRunner("SPARK", (boolean) context
				.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
		assertTrue(runner instanceof SparkJobRunner);
	}
}

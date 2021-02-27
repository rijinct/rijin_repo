
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.DbConfigurator;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DBConnectionManager.class, ConnectionManager.class
, DbConfigurator.class })
public class HiveTableQueryUtilTest {

	@Mock
	DBConnectionManager dbConnManager;
	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();

	DBConnectionManager mockDBConnectionManager = mock(
			DBConnectionManager.class);

	Connection mockConnection = mock(Connection.class);

	Statement mockStatement = mock(Statement.class);

	ResultSet mockResultSet = mock(ResultSet.class);

	@Mock
	DbConfigurator dbConfigurator;
	// .getLogger(HiveTableQueryUtil.class)).setLevel(Level.TRACE);
	// }
	@Before
	public void setUp() throws Exception {
		mockStatic(ConnectionManager.class);
		mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(mockDBConnectionManager);
		PowerMockito
				.when(mockDBConnectionManager.getHiveConnection(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.anyString(), Mockito.anyString()))
				.thenReturn(mockConnection);
		PowerMockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(mockConnection);
		PowerMockito.when(mockStatement.executeQuery(Mockito.anyString()))
				.thenReturn(mockResultSet);
		PowerMockito.when(mockResultSet.next()).thenReturn(true, false, true,
				false);
		PowerMockito.when(mockResultSet.getString(1)).thenReturn("Location");
		PowerMockito.when(mockResultSet.getString(2)).thenReturn(
				"hdfs://rithomas232/rithomas/ps/MGW_1/MGW_A_1_1_15MIN",
				"hdfs://rithomas232/rithomas/ps/MGW_1/MGW_B_1_1_15MIN");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	@Test(expected = HiveQueryException.class)
	public void testRetrieveHDFSLocationGivenTableNameSQLException()
			throws Exception {
		context.setProperty(JobExecutionContext.TARGET, "PS_MGW_C_1_1_15MIN");
		PowerMockito.when(mockConnection.createStatement())
				.thenThrow(new SQLException());
		// PowerMockito.doThrow(new
		// SQLException()).when(mockDBConnectionManager)
		// .closeConnection(mockResultSet, mockStatement, mockConnection);
		assertEquals(null,
				HiveTableQueryUtil.retrieveTargetTableHDFSLocation(context));
	}

	@Ignore
	public void testRetrieveHDFSLocationGivenTableName()
			throws HiveQueryException {
		context.setProperty(JobExecutionContext.TARGET,
				"PS_MGW_A_1_1_15MIN,PS_MGW_B_1_1_15MIN");
		List<String> tableNames = HiveTableQueryUtil
				.retrieveTargetTableHDFSLocation(context);
		assertEquals("/rithomas/ps/MGW_1/MGW_A_1_1_15MIN", tableNames.get(0));
		assertEquals("/rithomas/ps/MGW_1/MGW_B_1_1_15MIN", tableNames.get(1));
	}

	@Test
	public void testRetrieveHDFSLocationGivenTableName1() throws Exception {
		context.setProperty(JobExecutionContext.TARGET, "PS_MGW_A_1_1_15MIN");
		PowerMockito
				.when(dbConfigurator.shouldConnectToCustomDbUrl(
						Mockito.anyString(), Mockito.anyString()))
				.thenReturn(true);
		assertEquals("hdfs://rithomas232/rithomas/ps/MGW_1/MGW_A_1_1_15MIN",
				HiveTableQueryUtil.retrieveTargetTableHDFSLocation(context)
						.get(0));
	}

	@Test
	public void testRetrieveHDFSLocationGivenTableNameSQLException1()
			throws HiveQueryException, ClassNotFoundException, SQLException {
		context.setProperty(JobExecutionContext.TARGET, "PS_VOICE_A_1_15MIN");
		PowerMockito.when(mockResultSet.getString(2)).thenReturn(
				"hdfs://rithomas232/rithomas/ps/VOICE_1/PS_VOICE_A_1_15MIN");
		PowerMockito.doThrow(new SQLException()).when(mockDBConnectionManager)
				.closeConnection(mockResultSet, mockStatement, mockConnection);
		assertEquals("hdfs://rithomas232/rithomas/ps/VOICE_1/PS_VOICE_A_1_15MIN",
				HiveTableQueryUtil.retrieveTargetTableHDFSLocation(context)
						.get(0));
	}
}

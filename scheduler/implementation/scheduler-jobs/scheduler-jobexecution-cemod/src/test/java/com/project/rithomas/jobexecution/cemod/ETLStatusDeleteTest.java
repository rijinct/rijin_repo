
package com.project.rithomas.jobexecution.project;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.RuntimeProperty;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.model.utils.SDKEncryptDecryptUtils;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { RuntimePropertyQuery.class, FileInputStream.class,
		ETLStatusDelete.class, SDKEncryptDecryptUtils.class,
		DBConnectionManager.class, GetDBResource.class })
public class ETLStatusDeleteTest {

	private static final String PERSISTANCE_CONFIG_FILE = "persistence-config.properties";

	private ETLStatusDelete etlStatusDelete = new ETLStatusDelete();

	private WorkFlowContext context = new GeneratorWorkFlowContext();

	PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

	DBConnectionManager mockDBConnectionManager = mock(DBConnectionManager.class);

	Connection mockConnection = mock(Connection.class);

	@Mock
	GetDBResource getDbResource;

	@Before
	public void setUp() throws Exception {
		RuntimePropertyQuery mockRuntimePropertyQuery = mock(RuntimePropertyQuery.class);
		PowerMockito.whenNew(RuntimePropertyQuery.class).withNoArguments()
				.thenReturn(mockRuntimePropertyQuery);
		RuntimeProperty runtimeProperty = new RuntimeProperty();
		runtimeProperty.setParamname(JobExecutionContext.ETL_STATUS_DAYS_KEPT);
		runtimeProperty.setParamvalue("5");
		PowerMockito.when(
				mockRuntimePropertyQuery
						.retrieve(JobExecutionContext.ETL_STATUS_DAYS_KEPT))
				.thenReturn(runtimeProperty);
		String filePath = this.getClass().getClassLoader()
				.getResource("persistence-config.properties").getPath();
		if (filePath != null && filePath.contains("%20")) {
			filePath = filePath.replace("%20", " ");
		}
		FileInputStream fis = new FileInputStream(filePath);
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getSDKApplicationConfDirectory()
								+ PERSISTANCE_CONFIG_FILE).thenReturn(fis);
		PowerMockito.mockStatic(SDKEncryptDecryptUtils.class);
		Mockito.when(
				SDKEncryptDecryptUtils
						.decryptPassword("olkaYmgydW4/VI7niPLQNQ=="))
				.thenReturn("rithomas");
		PowerMockito.mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance()).thenReturn(
				mockDBConnectionManager);
		PowerMockito.when(
				mockDBConnectionManager.getPostgresConnection(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.anyString(), Mockito.anyString())).thenReturn(
				mockConnection);
		PowerMockito.when(mockConnection.prepareStatement(Mockito.anyString()))
				.thenReturn(mockPreparedStatement);
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		PowerMockito.when(GetDBResource.getPostgreDriver()).thenReturn("PostgreDriver");
		PowerMockito.when(GetDBResource.getPostgresUrl()).thenReturn("PostgresUrl");
		PowerMockito.when(GetDBResource.getPostgreUserName()).thenReturn("PostgreUserName");
		PowerMockito.when(GetDBResource.getPostgrePassword()).thenReturn("PostgrePassword");
	}

	@Test
	public void testNoFilesOlderThanRetentionDays()
			throws WorkFlowExecutionException, SQLException {
		PowerMockito.when(mockPreparedStatement.executeUpdate()).thenReturn(2);
		assertEquals(true, etlStatusDelete.execute(context));
	}

	@Test
	public void testDeleteFilesOlderThanRetentionDays() throws SQLException,
			WorkFlowExecutionException {
		PowerMockito.when(mockPreparedStatement.executeUpdate()).thenReturn(0);
		Mockito.doNothing().when(mockDBConnectionManager)
				.closeConnection(null, mockPreparedStatement, mockConnection);
		assertEquals(true, etlStatusDelete.execute(context));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testSQLException() throws SQLException,
			WorkFlowExecutionException, ClassNotFoundException {
		Mockito.when(
				mockDBConnectionManager.getPostgresConnection(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.anyString(), Mockito.anyString())).thenThrow(
				new SQLException());
		assertEquals(false, etlStatusDelete.execute(context));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testClassNotFoundException() throws SQLException,
			WorkFlowExecutionException, ClassNotFoundException {
		Mockito.when(
				mockDBConnectionManager.getPostgresConnection(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.anyString(), Mockito.anyString())).thenThrow(
				new ClassNotFoundException());
		assertEquals(false, etlStatusDelete.execute(context));
	}

	@Test
	public void testCloseConnectionWithException() throws SQLException,
			WorkFlowExecutionException {
		Mockito.doThrow(new SQLException()).when(mockDBConnectionManager)
				.closeConnection(null, mockPreparedStatement, mockConnection);
		assertEquals(true, etlStatusDelete.execute(context));
	}
}

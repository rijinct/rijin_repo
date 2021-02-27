
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.utils.SDKEncryptDecryptUtils;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { SDKEncryptDecryptUtils.class, GetDBResource.class,
		DBConnectionManager.class })
public class ETLStatusUpdaterTest {

	ETLStatusUpdater etlStatusUpdater = new ETLStatusUpdater();

	private static final String PERSISTANCE_CONFIG_FILE = "persistence-config.properties";

	DBConnectionManager mockDBConnectionManager = mock(DBConnectionManager.class);

	Connection mockConnection = mock(Connection.class);

	PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);

	@Before
	public void setUp() throws Exception {
		String filePath = this.getClass().getClassLoader()
				.getResource("persistence-config.properties").getPath();
		if (filePath != null && filePath.contains("%20")) {
			filePath = filePath.replace("%20", " ");
		}
		FileInputStream fis = new FileInputStream(filePath);
		String clientPropFile = this.getClass().getClassLoader()
				.getResource("client.properties").getPath();
		if (clientPropFile != null && clientPropFile.contains("%20")) {
			clientPropFile = clientPropFile.replace("%20", " ");
		}
		FileInputStream clientPropFis = new FileInputStream(new File(
				clientPropFile));
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getSDKApplicationConfDirectory()
								+ PERSISTANCE_CONFIG_FILE).thenReturn(fis);
		PowerMockito
				.whenNew(FileInputStream.class)
				.withArguments(
						SDKSystemEnvironment.getschedulerConfDirectory()
								+ "client.properties")
				.thenReturn(clientPropFis);
		GeneratorPropertyQuery mockGeneratorPropertyQuery = mock(GeneratorPropertyQuery.class);
		PowerMockito.whenNew(GeneratorPropertyQuery.class).withNoArguments()
				.thenReturn(mockGeneratorPropertyQuery);
		PowerMockito.when(
				mockGeneratorPropertyQuery.retrieve(Mockito.anyString()))
				.thenReturn(null);
		PowerMockito.mockStatic(SDKEncryptDecryptUtils.class);
		Mockito.when(
				SDKEncryptDecryptUtils
						.decryptPassword("olkaYmgydW4/VI7niPLQNQ=="))
				.thenReturn("rithomas");
		Mockito.when(
				SDKEncryptDecryptUtils
						.decryptPassword("H1X1wH++kTxdWijrpgdOCw=="))
				.thenReturn("hive");
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
	}

	// @Test
	public void testUpdateStatusForRunningJobToError()
			throws WorkFlowExecutionException {
		etlStatusUpdater
				.updateStatusForRunningJobToError("Usage_VOICE_1_LoadJob");
	}

	// @Test
	public void testUpdateStatusForRunningJobToErrorClassNotFoundException()
			throws WorkFlowExecutionException, ClassNotFoundException,
			SQLException {
		PowerMockito.when(
				mockDBConnectionManager.getPostgresConnection(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.anyString(), Mockito.anyString())).thenThrow(
				new ClassNotFoundException());
		try {
			etlStatusUpdater
					.updateStatusForRunningJobToError("Usage_VOICE_1_LoadJob");
		} catch (Exception e) {
			assertEquals("Unable to load jdbc driver class.", e.getMessage());
		}
	}

	// @Test
	public void testUpdateStatusForRunningJobToErrorSQLException()
			throws WorkFlowExecutionException, SQLException {
		PowerMockito.when(mockConnection.prepareStatement(Mockito.anyString()))
				.thenThrow(new SQLException());
		Mockito.doThrow(new SQLException()).when(mockDBConnectionManager)
				.closeConnection(null, mockPreparedStatement, mockConnection);
		try {
			etlStatusUpdater
					.updateStatusForRunningJobToError("Usage_VOICE_1_LoadJob");
		} catch (Exception e) {
			assertEquals("Exception while executing query.", e.getMessage());
		}
	}

	// @Test
	public void testUpdateStatusForRunningJobToErrorSQLExceptionCloseConnection()
			throws WorkFlowExecutionException, SQLException {
		Mockito.doThrow(new SQLException()).when(mockDBConnectionManager)
				.closeConnection(null, mockPreparedStatement, mockConnection);
		etlStatusUpdater
				.updateStatusForRunningJobToError("Usage_VOICE_1_LoadJob");
	}
}

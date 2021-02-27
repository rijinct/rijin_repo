
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DBConnectionManager.class })
public class DBConnectionManagerTest {

	private static String hiveUrl = "jdbc:hive://10.58.127.71:50030/saidata";

	private String hiveDriver = "org.apache.hive.jdbc.HiveDriver";

	private static String postgresDriver = "org.postgresql.Driver";

	private static String postgresUrl = "jdbc:postgresql://10.58.127.71/sai";

	Connection mockConnection = mock(Connection.class);

	@Before
	public void setUp() {
		PowerMockito.mockStatic(DriverManager.class);
	}

	@Ignore
	public void testGetHiveConnection()
			throws SQLException, ClassNotFoundException {
		PowerMockito.when(DriverManager.getConnection(hiveUrl, "", ""))
				.thenReturn(mockConnection);
		PowerMockito.doNothing().when(Class.forName(Mockito.anyString()));
		assertNotNull(DBConnectionManager.getInstance()
				.getHiveConnection(hiveDriver, hiveUrl, "", ""));
	}

	@Test
	public void testGetPostgresConnection()
			throws SQLException, ClassNotFoundException {
		PowerMockito.when(DriverManager.getConnection(postgresUrl, "", ""))
				.thenReturn(mockConnection);
		assertNotNull(DBConnectionManager.getInstance()
				.getPostgresConnection(postgresDriver, postgresUrl, "", ""));
	}

	@Test
	public void testCloseConnectionResultSetStatementConnection()
			throws SQLException {
		Statement st = mock(Statement.class);
		DBConnectionManager.getInstance().closeConnection(st, mockConnection);
	}

	@Test
	public void testCloseConnectionStatementConnection() throws SQLException {
		ResultSet rs = mock(ResultSet.class);
		Statement st = mock(Statement.class);
		DBConnectionManager.getInstance().closeConnection(rs, st,
				mockConnection);
	}
}

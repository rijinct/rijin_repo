package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.datanucleus.store.connection.ConnectionManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;



@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DBConnectionManager.class })
public class HiveJobRunnerTest {
	
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

	private HiveJobRunner hiveJobRunner;
	
	@Before
	public void setUpBeforeClass() throws Exception {
		mockStatic(DBConnectionManager.class);
		when(DBConnectionManager.getInstance()).thenReturn(mockDBConnectionManager);
		when(mockDBConnectionManager.getConnection(Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(mockConnection);
		when(mockConnection.createStatement()).thenReturn(mockStatement);
		hiveJobRunner = new HiveJobRunner(true);
		verify(mockConnection,Mockito.times(1)).createStatement();
	}

	@Test
	public void testRunQuery() throws SQLException {
		try {
			when(mockConnection.prepareStatement(Mockito.anyString())).
			thenReturn(mockPreparedStatement);
			hiveJobRunner.runQuery(Mockito.anyString());
			verify(mockPreparedStatement, Mockito.times(1)).execute();
		}catch(SQLException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void testRunQuery_WithParameters() throws SQLException {
		try {
			when(mockStatement.executeQuery(Mockito.anyString())).
							thenReturn(mockResultSet);
		Object object = hiveJobRunner.runQuery("","",null);
		verify(mockStatement,Mockito.times(1)).executeQuery(Mockito.anyString());
		assertTrue(object instanceof ResultSet);
		}catch(SQLException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void testSetQueryHint() throws SQLException {
		try {
			hiveJobRunner.setQueryHint(Mockito.anyString());
			verify(mockStatement, Mockito.times(1)).execute(Mockito.anyString());
		} catch (SQLException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void testSetQueryHints_AsList() throws SQLException {
		try {
			List<String> hints = new ArrayList<>();
			hints.add("SET hive.exec.reducers.bytes.per.reducer=1073741824");
			hints.add("SET hive.support.concurrency=false");
			hiveJobRunner.setQueryHints(hints);
			verify(mockStatement, Mockito.times(hints.size())).execute(Mockito.anyString());
		} catch (SQLException e) {
			assertTrue(false);
		}
	}
	
	@Test
	public void testCloseConnection_WithPreparedStatementNotNull() {
		try {
			hiveJobRunner.pst = Mockito.anyObject();
			hiveJobRunner.closeConnection();
			verifyStatic(VerificationModeFactory.times(1));
		}
		catch(Exception e) {
			assertTrue(false);
		}
	}
	@Test
	public void testCloseConnection_WithStatementNotNull() {
		try {
			hiveJobRunner.closeConnection();
			verifyStatic(VerificationModeFactory.times(1));
		}
		catch(Exception e) {
			assertTrue(false);
		}
	}

}

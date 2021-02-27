
package com.project.rithomas.jobexecution.common.util;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveHibernateUtil;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor(value = {
		"com.rijin.scheduler.jobexecution.hive.query.HiveHibernateUtil",
"com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil" })
@PrepareForTest(value = { HiveHibernateUtil.class, MetadataHibernateUtil.class,
		ReConnectUtil.class })
public class QueryExecutorTest {

	SessionFactory mockSessionFactory = mock(SessionFactory.class);

	QueryExecutor queryExecutor = spy(new QueryExecutor());

	WorkFlowContext context = new JobExecutionContext();

	Session mockSession = mock(Session.class);

	
	
	ReConnectUtil reConnector = spy(new ReConnectUtil());//mock(ReConnectUtil.class);

	@Mock
	ReConnectUtil reconnectUtil;

	@Mock
	private NativeQuery mockSQLQuery;

	@Mock
	private Transaction mockTransaction;

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(HiveHibernateUtil.class);
		PowerMockito.mockStatic(MetadataHibernateUtil.class);
		PowerMockito.when(MetadataHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.when(HiveHibernateUtil.getSessionFactory()).thenReturn(
				mockSessionFactory);
		PowerMockito.when(mockSessionFactory.openSession()).thenReturn(
				mockSession);
		PowerMockito.when(mockSession.createSQLQuery("Test Query")).thenReturn(
				mockSQLQuery);
		PowerMockito.when(mockSession.isOpen()).thenReturn(true);
		Mockito.when(reConnector.gettimeToWait()).thenReturn(1L);
		Mockito.when(queryExecutor.getReconnector()).thenReturn(reConnector);
	}

	@Test
	public void testExecutePostgresqlUpdate() throws JobExecutionException {
		String query = "Test Query";
		
		when(mockSession.beginTransaction()).thenReturn(mockTransaction);
		when(mockSession.createNativeQuery(query)).thenReturn(mockSQLQuery);
		when(mockSQLQuery.executeUpdate()).thenReturn(1);
		doNothing().when(mockTransaction).commit();
		
		queryExecutor.executePostgresqlUpdate(query, context);
		
		verify(mockSession, times(1)).beginTransaction();
		verify(mockSession, times(1)).createNativeQuery(query);
		verify(mockSQLQuery, times(1)).executeUpdate();
		verify(mockTransaction, times(1)).commit();
	}

	@Test(expected = JobExecutionException.class)
	public void testExecutePostgresqlUpdateException()
			throws JobExecutionException {
		String query = "Test Query";
		
		when(mockSession.beginTransaction()).thenReturn(mockTransaction);
		when(mockSession.createNativeQuery(query)).thenReturn(mockSQLQuery);
		
		when(mockSQLQuery.executeUpdate()).thenThrow(
				new HibernateException("HibernateException"));
		queryExecutor.executePostgresqlUpdate(query, context);
		
		verify(mockSession, times(1)).beginTransaction();
		verify(mockSession, times(1)).createNativeQuery(query);
		verify(mockSQLQuery, times(1)).executeUpdate();
		verify(mockTransaction, times(0)).commit();
	}

	@Test(expected = JobExecutionException.class)
	public void testExecutePostgresqlQueryException()
			throws JobExecutionException {
		String query = "Test Query";
		PowerMockito.when(mockSQLQuery.list()).thenThrow(
				new HibernateException(""));
		queryExecutor.executeMetadatasqlQuery(query, context);
	}

	@Test
	public void testExecutePostgresqlQuery() throws JobExecutionException {
		String query = "Test Query";
		String[] strArray = new String[2];
		List listObj = new ArrayList<Object>();
		listObj.add(new String("str"));
		listObj.add(strArray);
		PowerMockito.when(mockSQLQuery.list()).thenReturn(listObj);
		queryExecutor.executeMetadatasqlQuery(query, context);
	}

	@Test(expected = JobExecutionException.class)
	public void testExecutePostgresqlQueryMultipleException()
			throws JobExecutionException {
		String query = "Test Query";
		PowerMockito.when(mockSQLQuery.list()).thenThrow(
				new HibernateException("HibernateException"));
		queryExecutor.executeMetadatasqlQueryMultiple(query, context);
	}

	@Test
	public void testExecutePostgresqlQueryMultiple()
			throws JobExecutionException {
		String query = "Test Query";
		List listObj = new ArrayList<Object>();
		PowerMockito.when(mockSQLQuery.list()).thenReturn(listObj);
		queryExecutor.executeMetadatasqlQueryMultiple(query, context);
	}
}

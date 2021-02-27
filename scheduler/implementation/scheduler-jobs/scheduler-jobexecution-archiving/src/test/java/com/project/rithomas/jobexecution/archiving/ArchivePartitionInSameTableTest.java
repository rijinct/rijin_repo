
package com.project.rithomas.jobexecution.archiving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor({
		"com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider",
		"com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil" })
@PrepareForTest(value = { ArchivePartitionInSameTable.class,
		DBConnectionManager.class, ConnectionManager.class,
		HiveConfigurationProvider.class, HiveConfiguration.class,
		schedulerJobRunner.class, PreparedStatement.class,
		HiveTableQueryUtil.class, BaseArchivePartitioner.class,
		FileSystem.class, GetDBResource.class, ReConnectUtil.class,
		QueryExecutor.class, MetadataHibernateUtil.class, SessionFactory.class,
		Session.class, ArchivePartitionExecutor.class })
public class ArchivePartitionInSameTableTest {

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	Connection mockConnection;

	@Mock
	Statement statement;

	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	UpdateJobStatus updateJobStatus;

	ArchivePartitionExecutor executor = new ArchivePartitionExecutor();

	@Mock
	ResultSet result;

	@Mock
	HiveConfiguration hiveConfiguration;

	@Mock
	HiveJobRunner jobRunner;

	@Mock
	GetDBResource getDbResource;

	@Mock
	Configuration conf;

	@Mock
	FileSystem fileSystem;

	@Mock
	QueryExecutor queryExec;

	@Mock
	Session session;

	@Mock
	SessionFactory mockSessionFactory;

	@Mock
	private NativeQuery mockSQLQuery;

	@Mock
	Transaction tr;

	@Mock
	ReaggregationListUtil reaggUtil;
	
	@Mock
	BoundaryQuery boundaryQuery;
	
	List<String> hiveSetting = new ArrayList<String>();
	{

		hiveSetting.add("set hive.archive.enabled=true");
	}

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(HiveJobRunner.class)
				.withArguments(Mockito.anyBoolean()).thenReturn(jobRunner);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_SEGG_1_DAY_ArchivingJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_SMS_SEGG_1_HOUR");
		context.setProperty(JobExecutionContext.MAXVALUE, getMaxValue());
		context.setProperty(JobExecutionContext.LB, 1549737000000l); // 10-02-2019
		context.setProperty(JobExecutionContext.UB, 1549909800000l); // 12-02-2019
		context.setProperty(JobExecutionContext.HIVE_PARTITION_COLUMN, "dt");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		context.setProperty(JobExecutionContext.INTERVAL, "DAY");
		context.setProperty(JobExecutionContext.ARCHIVING_DAYS,
				getArchivingDayIntValue());
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		mockStatic(HiveConfigurationProvider.class);
		mockStatic(HiveConfiguration.class);
		mockStatic(ConnectionManager.class);
		mockStatic(DBConnectionManager.class);
		mockStatic(HiveTableQueryUtil.class);
		mockStatic(SessionFactory.class);
		mockStatic(MetadataHibernateUtil.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(mockConnection);
		PowerMockito.when(mockConnection.createStatement())
				.thenReturn(statement);
		PreparedStatement pstmt = Mockito.mock(PreparedStatement.class);
		Mockito.when(mockConnection.prepareStatement(Mockito.anyString()))
				.thenReturn(pstmt);
		PowerMockito.when(pstmt.execute()).thenReturn(true);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		PowerMockito.whenNew(HiveConfigurationProvider.class).withNoArguments()
				.thenReturn(mockHiveConfigurationProvider);
		PowerMockito.when(
				HiveConfigurationProvider.getInstance().getConfiguration())
				.thenReturn(hiveConfiguration);
		PowerMockito.when(hiveConfiguration.getKerberosPrincipal())
				.thenReturn("principal");
		PowerMockito.when(hiveConfiguration.getKeytabLocation())
				.thenReturn("location");
		PowerMockito
				.when(mockHiveConfigurationProvider.getQueryHints(
						Mockito.anyString(), Mockito.anyString()))
				.thenReturn(hiveSetting);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.when(MetadataHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.when(mockSessionFactory.openSession()).thenReturn(session);
		BigInteger i = new BigInteger("5");
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, i);
		Object[] resultSetSeq = { 4 };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						"select nextval('rithomas.ETL_STAT_SEQ')", context))
				.thenReturn(resultSetSeq);
		PowerMockito.when(session.beginTransaction()).thenReturn(tr);
		NativeQuery sqlQuery = mock(NativeQuery.class);
		PowerMockito.when(session.createNativeQuery(Mockito.anyString()))
				.thenReturn(sqlQuery);
		PowerMockito.when(ConnectionManager.getConnection(Mockito.anyString()))
				.thenReturn(mockConnection);
		PowerMockito.when(ConnectionManager.getConnection(Mockito.anyString(),
				Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(mockConnection);
		PowerMockito.when(mockConnection.createStatement())
				.thenReturn(statement);
		Mockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		PowerMockito.when(result.next()).thenReturn(true, true, true, true,
				false);
		PowerMockito.when(result.getString(1)).thenReturn("dt=1549002600000/tz=Default",
				"dt=1549737000000/tz=Default", "dt=1549823400000/tz=Default", "dt=1549909800000/tz=Default",
				"dt=1549996200000/tz=Default", "dt=1550125800000/tz=Default", "dt=1550212200000/tz=Default");
		mockStatic(GetDBResource.class);
		mockStatic(FileSystem.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		PowerMockito.when(getDbResource.getHdfsConfiguration())
				.thenReturn(conf);
		PowerMockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		PowerMockito.when(HiveTableQueryUtil.getTableLocation(context))
				.thenReturn("/rithomas/ps/SMS_1/SMS_SEGG_1_DAY");
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQuery);
		PowerMockito
				.when(boundaryQuery.retrieveByJobIdAndRegionId(
						"Perf_SMS_SEGG_1_DAY_ArchivingJob", null))
				.thenReturn(getBoundaryListWithMaxValue());
		PowerMockito.whenNew(ReaggregationListUtil.class).withNoArguments()
				.thenReturn(reaggUtil);
	}

	private Timestamp getMaxValue() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
		String dateString = "09-02-2019 12:00:00";
		Date date = sdf.parse(dateString);
		System.out.println("Given Time in milliseconds : " + date.getTime());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		return timeStamp;
	}
	
	private List<Boundary> getBoundaryListWithMaxValue() throws ParseException {
		List<Boundary> boundaryList = new ArrayList<>();
		Boundary boundary = new Boundary();
		boundary.setMaxValue(getMaxValue());
		boundaryList.add(boundary);
		return boundaryList;
	}

	@Test
	public void callArchivePartitionInSameTable()
			throws WorkFlowExecutionException {
		assertTrue(executor.getArchivePartitioner(
				context) instanceof ArchivePartitionInSameTable);
	}

	@Test
	public void testArchiveInSameTable() throws WorkFlowExecutionException {
		executor.execute(context);
		Long boundValue = 1549909800000l;
		assertEquals(boundValue,
				context.getProperty(JobExecutionContext.QUERYDONEBOUND));
	}

	@Test
	public void testArchiveForJobException()
			throws WorkFlowExecutionException, SQLException {
		try {
			Mockito.when(statement.executeQuery(Mockito.anyString()))
					.thenThrow(new SQLException("Exception: "));
			executor.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains("Unable to list partitions"));
		}
	}

	@Test
	public void testArchiveForWorkFlowException() throws HiveQueryException {
		try {
			PowerMockito.when(HiveTableQueryUtil.getTableLocation(context))
					.thenThrow(new HiveQueryException(
							"Error getting table location"));
			executor.execute(context);
			assertFalse(true);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage()
					.contains("Exception during archiving each partition"));
			assertTrue(
					((String) context.getProperty(JobExecutionContext.STATUS))
							.equals("E"));
			assertTrue(((String) context.getProperty(
					JobExecutionContext.ERROR_DESCRIPTION)).contains(
							"Exception during archiving each partition"));
		}
	}
	private String getArchivingDayIntValue() throws ParseException {
		final SimpleDateFormat formatter = new SimpleDateFormat("dd MM yyyy");
		Date currentDate = formatter.parse(formatter.format(new Date()));
		Date date = formatter.parse("14 02 2019");
		long diff = currentDate.getTime() - date.getTime();
		return String.valueOf(diff / (1000 * 60 * 60 * 24));
	}
}

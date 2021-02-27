
package com.project.rithomas.jobexecution.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockitoSession;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DropPartition.class, DriverManager.class,
		DBConnectionManager.class, FileSystem.class, FileStatus.class,
		ConnectionManager.class, GetDBResource.class, PartitionUtil.class,
		DateFunctionTransformation.class })
public class DropPartitionTest {

	DropPartition dropPartition = null;

	WorkFlowContext context = new JobExecutionContext();

	Calendar calendar = new GregorianCalendar();

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	QueryExecutor queryExec;

	@Mock
	Session session;

	@Mock
	org.apache.hadoop.conf.Configuration conf;

	@Mock
	FileSystem fileSystem;

	@Mock
	FileStatus mockFileStatus;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	GetDBResource getDbResource;

	@Mock
	ResultSet result;

	@Mock
	ResultSet resultExt;

	@Mock
	Connection connection;

	@Mock
	PreparedStatement statement;

	@Mock
	ReaggregationListUtil reaggUtil;

	@Mock
	PartitionUtil partitionUtil;

	@Mock
	DateFunctionTransformation dateFunctionTransformation;

	@Before
	public void setUp() throws Exception {
		dropPartition = new DropPartition();
		// setting new object
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		// setting property in the context
		context.setProperty(JobExecutionContext.HIVE_PARTITION_COLUMN, "dt");
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		// setting hive DB connections
		mockStatic(FileSystem.class);
		// mock static class
		mockStatic(DBConnectionManager.class);
		mockStatic(DriverManager.class);
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		String sqlToExecute = "SHOW PARTITIONS ps_vlr_1_hour";
		calendar.add(Calendar.DATE, -1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		String partVal = "dt=" + calendar.getTimeInMillis();
		mockStatic(ConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.when(statement.executeQuery(sqlToExecute))
				.thenReturn(result);
		PowerMockito.when(result.next()).thenReturn(true).thenReturn(false);
		PowerMockito.when(result.getString(1)).thenReturn(partVal);
		context.setProperty(JobExecutionContext.IS_PARTITION_DROPPED, "false");
		mockStatic(PartitionUtil.class);
		PowerMockito.when(PartitionUtil.isArchiveEnabled(context))
				.thenReturn(true);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		context.setProperty(JobExecutionContext.IS_PARTITION_DROPPED, "false");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		PowerMockito.whenNew(ReaggregationListUtil.class).withNoArguments()
				.thenReturn(reaggUtil);
	}

	@Test
	public void testDropPartitionForAggJob() throws Exception {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_AggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_VLR_1_HOUR");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
		String sql = "ALTER TABLE ps_vlr_1_hour DROP PARTITION (dt < ?)";
		mockGetPartitionsToBeDropped();
		runMockedQuery(sql);
		assertTrue(dropPartition.execute(context));
		PowerMockito.verifyStatic(VerificationModeFactory.times(1));
		PartitionUtil.isArchiveEnabled(context);
	}

	@Test
	public void testDropPartitionForAggJobWhenHistoricReaggDependentJobsActive()
			throws Exception {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_AggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_VLR_1_HOUR");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
		PowerMockito.when(reaggUtil.isAnyReaggDependentJobsActive(context, 1))
				.thenReturn(true);
		dropPartition.execute(context);
		PowerMockito.verifyStatic(VerificationModeFactory.times(0));
		PartitionUtil.isArchiveEnabled(context);
	}

	private void mockGetPartitionsToBeDropped() throws Exception {
		Calendar calendar2 = Calendar.getInstance();
		calendar2.setTime(
				DateUtils.truncate(DateUtils.addDays(calendar.getTime(), -2),
						Calendar.DAY_OF_MONTH));
		String calValue = Long.toString(calendar2.getTimeInMillis());
		String partitionColValue = new StringBuilder("dt=").append(calValue)
				.append("/tz=\"Default\"/").toString();
		List<String> partitionList = Arrays.asList(partitionColValue);
		boolean shouldGetOnlyFirstPartition = false;
		PowerMockito
				.when(PartitionUtil.showPartitionsResult(context,
						"PS_VLR_1_HOUR", shouldGetOnlyFirstPartition))
				.thenReturn(partitionList);
		PowerMockito.when(PartitionUtil.getPartitionColumn(context))
				.thenReturn("dt");
		PowerMockito
				.when(PartitionUtil.getPartitionValue(partitionColValue, "dt"))
				.thenReturn(calValue);
	}

	public static void setValues(Calendar calendar, WorkFlowContext context,
			PreparedStatement statement, ResultSet result,
			FileSystem fileSystem, ResultSet resultExt,
			GetDBResource getDbResource, Configuration conf)
			throws IOException, IllegalArgumentException, SQLException {
		calendar.add(Calendar.DATE, -2);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		String partVal = "dt=" + calendar.getTimeInMillis();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.TARGET, "US_VLR_1");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
		context.setProperty(JobExecutionContext.STAGE_RETENTIONDAYS, "1");
		context.setProperty(JobExecutionContext.SOURCE, "INT_VLR_1");
		context.setProperty(JobExecutionContext.IMPORT_PATH,
				"/rithomas/us/import/VLR_1/VLR_1/");
		PowerMockito.when(result.next()).thenReturn(true).thenReturn(false);
		String sql_ext = "SHOW PARTITIONS int_vlr_1";
		PowerMockito.when(statement.executeQuery(sql_ext))
				.thenReturn(resultExt);
		PowerMockito.when(resultExt.next()).thenReturn(true).thenReturn(false);
		PowerMockito.when(resultExt.getString(1)).thenReturn(partVal);
		PowerMockito.when(result.getString(1)).thenReturn(partVal);
		PowerMockito.when(getDbResource.getHdfsConfiguration())
				.thenReturn(conf);
		PowerMockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		Path importPath = new Path("/rithomas/us/import/VLR_1/VLR_1/");
		Path workPath = new Path("/rithomas/us/work/VLR_1/VLR_1/");
		FileStatus fileStatus = new FileStatus(0, true, 0, 0, 0, importPath);
		FileStatus fileStatus1 = new FileStatus(0, true, 0, 0, 0, workPath);
		FileStatus[] fileStatusList = { fileStatus, fileStatus1 };
		PowerMockito.when(fileSystem.listStatus(importPath))
				.thenReturn(fileStatusList);
		PowerMockito.when(fileSystem.listStatus(workPath))
				.thenReturn(fileStatusList);
		PowerMockito.when(fileSystem.delete(fileStatus.getPath(), true))
				.thenReturn(true);
		PowerMockito.when(fileSystem.delete(fileStatus1.getPath(), true))
				.thenReturn(true);
		context.setProperty(JobExecutionContext.JOBTYPE, "Loading");
	}

	@Test
	public void testDropPartitionForUsageJob() throws Exception {
		// Setting calendar object
		setValues(calendar, context, statement, result, fileSystem, resultExt,
				getDbResource, conf);
		String sql = "SHOW PARTITIONS us_vlr_1";
		sql = "ALTER TABLE us_vlr_1 DROP PARTITION (dt < ?)";
		mockGetPartitionsToBeDropped();
		runMockedQuery(sql);
		PowerMockito.when(PartitionUtil.isArchiveEnabled(context))
				.thenReturn(true);
		PowerMockito.doNothing().when(queryExec).executePostgresqlUpdate(
				Mockito.anyString(), Mockito.anyObject());
		assertTrue(dropPartition.execute(context));
	}

	private void runMockedQuery(String sql) throws Exception, SQLException {
		Connection conn = Mockito.mock(Connection.class);
		PreparedStatement query = Mockito.mock(PreparedStatement.class);
		PowerMockito.when(connection = DBConnectionManager.getInstance()
				.getConnection(Mockito.anyString(), Mockito.anyBoolean()))
				.thenReturn(conn);
		PowerMockito.when(conn.prepareStatement(Mockito.anyString()))
				.thenReturn(query);
	}

	@Test
	public void testDropPartitionForJobException()
			throws SQLException, Exception {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_AggregateJob");
		context.setProperty(JobExecutionContext.TARGET, "PS_VLR_1_HOUR");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
		String sql = "ALTER TABLE ps_vlr_1_hour DROP PARTITION (dt < ?)";
		mockGetPartitionsToBeDropped();
		runMockedQuery(sql);
		try {
			dropPartition.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertEquals("Exception in Query ", e.getMessage());
		}
	}

	@Test
	public void testDropPartitionForIOException() throws Exception {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.TARGET, "US_VLR_1");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
		context.setProperty(JobExecutionContext.STAGE_RETENTIONDAYS, "1");
		context.setProperty(JobExecutionContext.SOURCE, "INT_VLR_1");
		context.setProperty(JobExecutionContext.IMPORT_PATH,
				"/rithomas/us/import/VLR_1/VLR_1/");
		String sql = "SHOW PARTITIONS us_vlr_1";
		PowerMockito.when(statement.executeQuery(sql)).thenReturn(result);
		PowerMockito.when(result.next()).thenReturn(true).thenReturn(false);
		String sql_ext = "SHOW PARTITIONS int_vlr_1";
		PowerMockito.when(statement.executeQuery(sql_ext)).thenReturn(result);
		PowerMockito.when(result.next()).thenReturn(true).thenReturn(false);
		// Setting calendar object
		calendar.add(Calendar.DATE, -2);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		String partVal = "dt=" + calendar.getTimeInMillis();
		PowerMockito.when(result.getString(1)).thenReturn(partVal);
		Path importPath = new Path("/rithomas/us/import/VLR_1/VLR_1/");
		PowerMockito.when(getDbResource.getHdfsConfiguration())
				.thenReturn(conf);
		PowerMockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		PowerMockito.when(fileSystem.listStatus(importPath)).thenThrow(
				new IOException("Exception in creating hadoop fileSystem "));
		sql = "ALTER TABLE us_vlr_1 DROP PARTITION (dt < ?)";
		mockGetPartitionsToBeDropped();
		runMockedQuery(sql);
		try {
			dropPartition.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertEquals("Exception in creating hadoop fileSystem ",
					e.getMessage());
		}
	}
}

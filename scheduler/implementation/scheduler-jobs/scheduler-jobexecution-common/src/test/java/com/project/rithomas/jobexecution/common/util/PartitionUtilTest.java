package com.project.rithomas.jobexecution.common.util;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { PartitionUtil.class, FileSystem.class,
		DBConnectionManager.class })
public class PartitionUtilTest {

	@Mock
	FileSystem fileSystem;

	@Mock
	DBConnectionManager connectionManager;

	@Mock
	Path path;

	@Mock
	Connection con;

	@Mock
	PreparedStatement statement;

	WorkFlowContext context;

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.when(FileSystem.get((Configuration) anyObject()))
				.thenReturn(fileSystem);
		PowerMockito.mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(connectionManager);
		PowerMockito
				.when(connectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE, false))
				.thenReturn(con);
		context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
	}

	@Test
	public void testdeleteParticularDirInHDFS() throws Exception {
		fileSystem = FileSystem.get(new Configuration());
		Path dir = new Path("/rithomas/arc/SMS");
		String partitionColumn = "dt";
		String partitionVal = "1500000000";
		PowerMockito
				.whenNew(Path.class).withArguments(dir + File.separator
						+ partitionColumn + "=" + partitionVal)
				.thenReturn(path);
		PowerMockito.when(fileSystem.exists(path)).thenReturn(true);
		PartitionUtil.deleteParticularDirInHDFS(dir, fileSystem,
				partitionColumn, partitionVal);
		verify(fileSystem).delete(path, true);
	}

	@Test
	public void testdeleteParticularDirInHDFSWhenPathNotExist()
			throws Exception {
		fileSystem = FileSystem.get(new Configuration());
		Path dir = new Path("/rithomas/arc/SMS");
		String partitionColumn = "dt";
		String partitionVal = "1500000000";
		PowerMockito
				.whenNew(Path.class).withArguments(dir + File.separator
						+ partitionColumn + "=" + partitionVal)
				.thenReturn(path);
		PowerMockito.when(fileSystem.exists(path)).thenReturn(false);
		PartitionUtil.deleteParticularDirInHDFS(dir, fileSystem,
				partitionColumn, partitionVal);
		verify(fileSystem, never()).delete(path, true);
	}

	@Test
	public void testDropPartitionsBasedOnCondition() throws Exception {
		String tableName = "PS_SMS_SEGG_1_HOUR";
		long timeInMillis = 1500000l;
		String condition = "<";
		String sql = "ALTER TABLE PS_SMS_SEGG_1_HOUR DROP PARTITION (dt< ?)";
		PowerMockito.when(con.prepareStatement(sql)).thenReturn(statement);
		PartitionUtil.dropPartitionsBasedOnCondition(context, tableName,
				timeInMillis, condition);
		verify(statement).execute();
	}

	@Test
	public void testDropPartitionsBasedOnConditionWhenTnpPerfType()
			throws Exception {
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.TNP_PERF_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_DESCRIPTION, "tz");
		String tableName = "PS_SMS_SEGG_1_HOUR";
		long timeInMillis = 1500000l;
		String condition = "<";
		String sql = "ALTER TABLE PS_SMS_SEGG_1_HOUR DROP PARTITION (dt< ?,gp=?)";
		PowerMockito.when(con.prepareStatement(sql)).thenReturn(statement);
		PartitionUtil.dropPartitionsBasedOnCondition(context, tableName,
				timeInMillis, condition);
		verify(statement).execute();
	}

	@Test
	public void testDropPartitionsBasedOnConditionWhenTnpThresholdType()
			throws Exception {

		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.TNP_THRESHOLD_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_SEGG_1_HOUR_AggregateJob");
		String tableName = "PS_SMS_SEGG_1_HOUR";
		long timeInMillis = 1500000l;
		String condition = "<";
		String sql = "ALTER TABLE PS_SMS_SEGG_1_HOUR DROP PARTITION (dt< ?,src_job=?)";
		PowerMockito.when(con.prepareStatement(sql)).thenReturn(statement);
		PartitionUtil.dropPartitionsBasedOnCondition(context, tableName,
				timeInMillis, condition);
		verify(statement).execute();
	}
}

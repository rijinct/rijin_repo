
package com.project.rithomas.jobexecution.archiving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider")
@PrepareForTest(value = { ArchivePartitionInArchiveTable.class,
		DBConnectionManager.class, ConnectionManager.class,
		HiveConfigurationProvider.class, HiveConfiguration.class,
		schedulerJobRunner.class, PreparedStatement.class,
		HiveTableQueryUtil.class, BaseArchivePartitioner.class,
		FileSystem.class, GetDBResource.class, ArchivePartitionExecutor.class })
public class ArchivePartitionInArchiveTableTest {

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
		context.setProperty(JobExecutionContext.LOWER_BOUND, 1549737000000l); // 10-02-2019
		context.setProperty(JobExecutionContext.UPPER_BOUND, 1549909800000l); // 12-02-2019
		context.setProperty(JobExecutionContext.HIVE_PARTITION_COLUMN, "dt");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		context.setProperty(JobExecutionContext.INTERVAL, "DAY");
		context.setProperty(JobExecutionContext.ARCHIVING_DAYS,
				getArchivingDayIntValue());
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.SQL,

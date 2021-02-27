
package com.project.rithomas.jobexecution.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.ApplicationIdLoggerHiveServer2Util;
import com.project.rithomas.jobexecution.common.GetJobMetadata;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ProcessUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderHelper;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { LoadManager.class, DriverManager.class,
		DBConnectionManager.class, ProcessUtil.class, GetDBResource.class,
		FileSystem.class, Process.class, File.class, InputStreamReader.class,
		BufferedReader.class, UsageAggregationStatusUtil.class,
		ReaggregationListUtil.class, ConnectionManager.class,
		HiveRowCountUtil.class, HiveJobRunner.class,
		RetrieveDimensionValues.class, HiveConfigurationProvider.class,
		TimeZoneUtil.class })
public class LoadManagerTest {

	WorkFlowContext context = new JobExecutionContext();

	LoadManager loadManager = null;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	QueryExecutor queryExec;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	Statement statement;

	@Mock
	PreparedStatement preparedStatment;

	@Mock
	BoundaryQuery boundQuery;

	@Mock
	Boundary boundary;

	@Mock
	GetDBResource getDbResource;

	@Mock
	ResultSet result;

	@Mock
	Connection connection;

	@Mock
	GetJobMetadata getMetadata;

	@Mock
	org.apache.hadoop.conf.Configuration conf;

	@Mock
	FileSystem fileSystem;

	@Mock
	File file;

	@Mock
	Process process;

	@Mock
	ProcessBuilderHelper processBuilder;

	@Mock
	InputStream in;

	@Mock
	InputStreamReader inputReader;

	@Mock
	BufferedReader bufferReader;

	@Mock
	UsageAggregationStatusUtil mockStatusUtil;

	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	HiveRowCountUtil hiveRowCounter;

	List<String> hiveSetting = new ArrayList<String>();
	{
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION uf_trunc AS 'com.project.rithomas.hive.udf.TruncUDF'");
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION nvl AS 'com.nexr.platform.hive.udf.GenericUDFNVL'");
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'");
	}

	Calendar calendar = new GregorianCalendar();

	@Before
	public void setUp() throws Exception {
		loadManager = spy(new LoadManager());
		Mockito.when(hiveRowCounter.getLogReadDelay()).thenReturn(1L);
		// setting new object
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundQuery);
		PowerMockito.whenNew(GetJobMetadata.class).withNoArguments()
				.thenReturn(getMetadata);
		PowerMockito.whenNew(ProcessBuilderHelper.class).withNoArguments()
				.thenReturn(processBuilder);
		PowerMockito.whenNew(InputStreamReader.class).withArguments(in)
				.thenReturn(inputReader);
		PowerMockito.whenNew(BufferedReader.class).withArguments(inputReader)
				.thenReturn(bufferReader);
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		boundaryList.add(boundary);
		PowerMockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		// mock static class
		mockStatic(DBConnectionManager.class);
		mockStatic(DriverManager.class);
		mockStatic(HiveConfigurationProvider.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		mockStatic(GetDBResource.class);
		mockStatic(FileSystem.class);
		mockStatic(File.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		calendar.add(Calendar.HOUR_OF_DAY, -3);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		// setting required values
		Set<String> availableDir = new HashSet<String>();
		availableDir
				.add("hdfs://10.63.22.236:54310/rithomas/us/import/VLR_1/VLR_1/dt="
						+ calendar.getTimeInMillis() + "/tz=RGN1");
		calendar.add(Calendar.HOUR_OF_DAY, -1);
		availableDir
				.add("hdfs://10.63.22.236:54310/rithomas/us/import/VLR_1/VLR_1/dt="
						+ calendar.getTimeInMillis() + "/tz=RGN1");
		calendar.add(Calendar.HOUR_OF_DAY, -1);
		availableDir
				.add("hdfs://10.63.22.236:54310/rithomas/us/import/VLR_1/VLR_1/dt="
						+ calendar.getTimeInMillis() + "/tz=RGN2");
		PowerMockito.whenNew(File.class).withArguments(Mockito.anyString())
				.thenReturn(file);
		PowerMockito.when(getDbResource.getHdfsConfiguration())
				.thenReturn(conf);
		PowerMockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		PowerMockito
				.when(mockHiveConfigurationProvider.getQueryHints(
						"Perf_VLR_1_HOUR_AggregateJob",
						schedulerConstants.HIVE_DATABASE))
				.thenReturn(hiveSetting);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.when(connection.prepareStatement(Mockito.anyString()))
				.thenReturn(preparedStatment);
		// setting in context
		context.setProperty(JobExecutionContext.HIVEDRIVER,
				"org.apache.hadoop.hive.jdbc.HiveDriver");
		context.setProperty("WORK_DIR_PATH", availableDir);
		context.setProperty(JobExecutionContext.TARGET, "US_VLR_1");
		context.setProperty(JobExecutionContext.TIMESTAMP_HINT,
				"US_VLR_11377231859061");
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT,
				Long.parseLong("1800"));
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.HIVE_PARTITION_COLUMN, "dt");
		context.setProperty(JobExecutionContext.TIMEZONE_PARTITION_COLUMN,
				"tz");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		context.setProperty(GeneratorWorkFlowContext.TIME_ZONE_SUPPORT, "YES");
		context.setProperty(JobExecutionContext.SQL,

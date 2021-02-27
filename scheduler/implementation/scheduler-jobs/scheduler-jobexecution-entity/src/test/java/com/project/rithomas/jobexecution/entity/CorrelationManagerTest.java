
package com.project.rithomas.jobexecution.entity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.corelation.EntitySpecReferences;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.query.EntitySpecificationQuery;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.util.EntitySpecificationGeneratorUtil;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DBConnectionManager.class, UpdateJobStatus.class,
		GetDBResource.class, FileSystem.class, RetrieveDimensionValues.class,
		ReConnectUtil.class, CorrelationManager.class, ConnectionManager.class,
		schedulerJobRunnerfactory.class, HiveConfigurationProvider.class,
		GeneratorWorkFlowContext.class, HiveJobRunner.class,
		EntitySpecificationQuery.class, EntitySpecification.class,
		EntitySpecificationGeneratorUtil.class, EntitySpecReferences.class,
		Configuration.class })
public class CorrelationManagerTest {

	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();

	DBConnectionManager mockDBConnectionManager = mock(
			DBConnectionManager.class);

	Connection mockConnection = mock(Connection.class);

	Statement mockStatement = mock(Statement.class);

	ResultSet mockResultSet = mock(ResultSet.class);

	CorrelationManager correlationManager = new CorrelationManager();

	HiveRowCountUtil rowCountUtil = mock(HiveRowCountUtil.class);

	ReConnectUtil reConnectUtil = mock(ReConnectUtil.class);

	@Mock
	HiveJobRunner runner;

	@Mock
	schedulerJobRunner scrRunner;

	@Mock
	EntitySpecificationQuery mockEntitySpecificationQuery;

	@Mock
	EntitySpecReferences mockEntitySpecReferences;

	@Mock
	EntitySpecification mockEntitySpecification;

	@Mock
	RetrieveDimensionValues retrieveDimensionValues;

	@Mock
	EntitySpecificationGeneratorUtil mockEntitySpecificationGeneratorUtil;

	@Mock
	private HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	org.apache.hadoop.conf.Configuration conf;

	@Mock
	GetDBResource getDbResource;

	@Mock
	private GeneratorWorkFlowContext mockGeneratorWorkFlowContext;

	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT, 1800l);//
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		context.setProperty(JobExecutionContext.JOB_NAME, "Job_Name");
		context.setProperty(GeneratorWorkFlowContext.QUERY_SEPARATOR, "Query");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				"SqlException");
		PowerMockito.mockStatic(DBConnectionManager.class,
				schedulerJobRunnerfactory.class,
				HiveConfigurationProvider.class, RetrieveDimensionValues.class,
				GeneratorWorkFlowContext.class, HiveJobRunner.class,
				EntitySpecificationQuery.class, EntitySpecification.class,
				EntitySpecificationGeneratorUtil.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(mockDBConnectionManager);
		GeneratorPropertyQuery mockGeneratorPropertyQuery = mock(
				GeneratorPropertyQuery.class);
		PowerMockito.whenNew(GeneratorPropertyQuery.class).withNoArguments()
				.thenReturn(mockGeneratorPropertyQuery);
		PowerMockito.whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(rowCountUtil);
		PowerMockito
				.when(mockGeneratorPropertyQuery.retrieve(Mockito.anyString()))
				.thenReturn(null);
		PowerMockito.whenNew(EntitySpecificationQuery.class).withNoArguments()
				.thenReturn(mockEntitySpecificationQuery);
		when(mockEntitySpecificationQuery.retrieveLatest(Mockito.anyString()))
				.thenReturn(mockEntitySpecification);
		PowerMockito
				.when(mockDBConnectionManager.getPostgresConnection(
						GetDBResource.getPostgreDriver(),
						GetDBResource.getPostgresUrl(),
						GetDBResource.getPostgresCacheUser(),
						GetDBResource.getPostgresCachePassword()))
				.thenReturn(mockConnection);
		PowerMockito.whenNew(ReConnectUtil.class).withNoArguments()
				.thenReturn(reConnectUtil);
		PowerMockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
		PowerMockito.when(reConnectUtil.isRetryRequired(Mockito.anyString()))
				.thenReturn(false);
		PowerMockito.when(reConnectUtil.shouldRetry()).thenReturn(true)
				.thenReturn(false);
		PowerMockito.whenNew(Configuration.class).withAnyArguments()
				.thenReturn(conf);
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		PowerMockito.when(getDbResource.getHdfsConfiguration())
				.thenReturn(conf);
		QueryExecutor mockQueryExecutor = mock(QueryExecutor.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
		String file = this.getClass().getClassLoader()
				.getResource("hive_settings.xml").getPath();
		if (file != null && file.contains("%20")) {
			file = file.replace("%20", " ");
		}
		FileReader fileReader = new FileReader(new File(file));
		PowerMockito.whenNew(FileReader.class)
				.withArguments(SDKSystemEnvironment.getschedulerConfDirectory()
						+ "hive_settings.xml")
				.thenReturn(fileReader);
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_IP,
				"10.63.63.232");
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_PORT, "54310");
		FileSystem mockFileSystem = mock(FileSystem.class);
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenReturn(mockFileSystem);
		context.setProperty(JobExecutionContext.ADAPTATION_ID,
				"COMMON_DIMENSION");
		context.setProperty(JobExecutionContext.ADAPTATION_VERSION, "1");
		context.setProperty("FILEEXTENSION", "csv");
		Path availableDirPath = new Path(
				"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1");
		Path path = new Path(
				"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1/ext_operator_1.csv");
		FileStatus fs = new FileStatus(0, false, 0, 0, 0, 0, null, "rithomas",
				"rithomas", path);
		FileStatus fileStatus[] = { fs };
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(availableDirPath),
						Mockito.any(PathFilter.class)))
				.thenReturn(fileStatus);
		PowerMockito.whenNew(HiveJobRunner.class).withAnyArguments()
				.thenReturn(runner);
		mockStatic(ConnectionManager.class);
		PowerMockito
				.when(ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(mockConnection);
		PowerMockito.whenNew(EntitySpecificationGeneratorUtil.class)
				.withNoArguments()
				.thenReturn(mockEntitySpecificationGeneratorUtil);
		when(EntitySpecificationGeneratorUtil.getEntitySpecExtTableName(
				Mockito.anyString(), Mockito.anyString()))
						.thenReturn("tanbleName");
		when(mockResultSet.next()).thenReturn(true);
	}

	@Test
	public void test() throws Exception {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		context.setProperty("TARGET", "ES_OPERATOR_1");
		Map<String, String> testmap = new HashMap<>();
		testmap.put("EXT_OPERATOR_1", "OPERATORD");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, testmap);
		String query = QueryConstants.UNIQUE_ROWS_QUERY;
		query = query.replace(QueryConstants.TABLE_NAME, "EXT_CLEAR_CODE_1")
				.replace(QueryConstants.UNIQUE_COLS, "CLEAR_CODE_ID");
		when(schedulerJobRunnerfactory
				.getRunner(schedulerConstants.HIVE_DATABASE, false))
						.thenReturn(runner);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints(context.JOB_NAME,
				"COMMON_DIMENSION_1/OPERATOR_1"))
						.thenReturn(Arrays.asList(
								"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1"));
		Mockito.when(RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, operatorJob, runner))
				.thenReturn("-dfs");
		PowerMockito.when(mockStatement.execute(query)).thenReturn(true);
		boolean excepted = true;
		boolean actual = correlationManager.execute(context);
		assertEquals(excepted, actual);
	}

	@Test
	public void testDuplicateRows() throws Exception {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		context.setProperty("TARGET", "ES_OPERATOR_1");
		Map<String, String> testmap = new HashMap<>();
		testmap.put("EXT_OPERATOR_1", "OPERATORD");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, testmap);
		String query = QueryConstants.UNIQUE_ROWS_QUERY;
		query = query.replace(QueryConstants.TABLE_NAME, "EXT_CLEAR_CODE_1")
				.replace(QueryConstants.UNIQUE_COLS, "CLEAR_CODE_ID");
		when(schedulerJobRunnerfactory
				.getRunner(schedulerConstants.HIVE_DATABASE, false))
						.thenReturn(runner);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints(context.JOB_NAME,
				"COMMON_DIMENSION_1/OPERATOR_1"))
						.thenReturn(Arrays.asList(
								"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1"));
		Mockito.when(RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, operatorJob, runner))
				.thenReturn("Entity_OPERATOR_1_CorrelationJob");
		when(mockEntitySpecification.getEntitySpecRef())
				.thenReturn(mockEntitySpecReferences);
		when(runner.runQuery(Mockito.anyString(), Mockito.anyString(),
				Mockito.any())).thenReturn(mockResultSet);
		when(mockEntitySpecification.getSpecId()).thenReturn("specId");
		PowerMockito.when(mockStatement.execute(query)).thenReturn(true);
		boolean actual = correlationManager.execute(context);
		assertEquals(true, actual);
	}

	
	@Test
	public void testDuplicateRowsLOCATION() throws Exception {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		context.setProperty("TARGET", "ES_OPERATOR_1");
		Map<String, String> testmap = new HashMap<>();
		testmap.put("EXT_OPERATOR_1", "OPERATORD");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, testmap);
		String query = QueryConstants.UNIQUE_ROWS_QUERY;
		query = query.replace(QueryConstants.TABLE_NAME, "EXT_CLEAR_CODE_1")
				.replace(QueryConstants.UNIQUE_COLS, "CLEAR_CODE_ID");
		when(schedulerJobRunnerfactory
				.getRunner(schedulerConstants.HIVE_DATABASE, false))
						.thenReturn(runner);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints(context.JOB_NAME,
				"COMMON_DIMENSION_1/OPERATOR_1"))
						.thenReturn(Arrays.asList(
								"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1"));
		Mockito.when(RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, operatorJob, runner))
				.thenReturn("-dfs");
		PowerMockito.when(mockStatement.execute(query)).thenReturn(true);
		correlationManager.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testDuplicateRowsCLEAR_CODE()
			throws WorkFlowExecutionException, SQLException {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_CLEAR_CODE_1_CorrelationJob");
		// context.setProperty(JobExecutionContext.SOURCE, "EXT_CLEAR_CODE_1");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, "CLEAR_CODE_ID");
		String query = QueryConstants.UNIQUE_ROWS_QUERY;
		query = query.replace(QueryConstants.TABLE_NAME, "EXT_CLEAR_CODE_1")
				.replace(QueryConstants.UNIQUE_COLS, "CLEAR_CODE_ID");
		PowerMockito.when(mockStatement.executeQuery(query))
				.thenReturn(mockResultSet);
		PowerMockito.when(mockResultSet.next()).thenReturn(true);
		correlationManager.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testSQLException() throws Exception {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		context.setProperty("TARGET", "ES_OPERATOR_1");
		Map<String, String> testmap = new HashMap<>();
		testmap.put("EXT_OPERATOR_1", "OPERATORD");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, testmap);
		String query = QueryConstants.UNIQUE_ROWS_QUERY;
		query = query.replace(QueryConstants.TABLE_NAME, "EXT_CLEAR_CODE_1")
				.replace(QueryConstants.UNIQUE_COLS, "CLEAR_CODE_ID");
		when(schedulerJobRunnerfactory
				.getRunner(schedulerConstants.HIVE_DATABASE, false))
						.thenReturn(runner);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		when(mockHiveConfigurationProvider.getQueryHints(context.JOB_NAME,
				"COMMON_DIMENSION_1/OPERATOR_1"))
						.thenReturn(Arrays.asList(
								"/rithomas/es/work/COMMON_DIMENSION_1/OPERATOR_1"));
		Mockito.when(RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, operatorJob, runner))
				.thenThrow(new SQLException(""));
		PowerMockito.when(mockStatement.execute(query)).thenReturn(true);
		correlationManager.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testIOException()
			throws IOException, WorkFlowExecutionException {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenThrow(new IOException());
		correlationManager.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testSQLException2()
			throws IOException, WorkFlowExecutionException {
		String operatorJob = this.getClass().getClassLoader()
				.getResource("es_operator_1_correlationjob.txt").getPath();
		context.setProperty(JobExecutionContext.SQL, operatorJob);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_OPERATOR_1_CorrelationJob");
		context.setProperty("SOURCE", "EXT_OPERATOR_1");
		PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenThrow(new IOException());
		correlationManager.execute(context);
	}
}

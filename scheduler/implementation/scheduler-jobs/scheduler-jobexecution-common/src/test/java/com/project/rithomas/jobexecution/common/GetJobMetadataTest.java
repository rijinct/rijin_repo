
package com.project.rithomas.jobexecution.common;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.StringWriter;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.hibernate.Session;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.ext.adddb.workflow.generator.util.HiveGeneratorUtil;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecification;
import com.project.rithomas.sdk.model.corelation.EntitySpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.corelation.query.EntitySpecificationQuery;
import com.project.rithomas.sdk.model.meta.AdaptationCatalog;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.JobPropertyPK;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.AdaptationCatalogQuery;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.model.others.JobProperties;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GetJobMetadata.class, GetDBResource.class,
		ResultSet.class, ConnectionManager.class, BoundaryQuery.class,
		TimeZoneUtil.class, HiveTableQueryUtil.class, DBConnectionManager.class,
		RetrieveDimensionValues.class, HiveConfigurationProvider.class,
		HiveGeneratorUtil.class, Clob.class, StringWriter.class })
public class GetJobMetadataTest {

	GetJobMetadata getJobProp = null;

	WorkFlowContext context = new JobExecutionContext();

	Boundary boundary = new Boundary();

	JobDictionary jobDictionary = new JobDictionary();

	JobDictionary srcJobId = new JobDictionary();

	JobTypeDictionary jobTypeDictionary = new JobTypeDictionary();

	JobPropertyPK jobPropertyPK = new JobPropertyPK();

	JobProperty jobProp = new JobProperty();

	List<Boundary> boundaryList = new ArrayList<Boundary>();

	List<Boundary> boundaryListEmpty = new ArrayList<Boundary>();

	List<JobDictionary> jobDictList = new ArrayList<JobDictionary>();

	List<JobProperty> jobPropList = new ArrayList<JobProperty>();

	List<JobProperty> srcJobPropList = new ArrayList<JobProperty>();

	JobProperties jobProperties = new JobProperties();

	Calendar calendar = new GregorianCalendar();

	@Mock
	ReConnectUtil reConnectUtil;

	@Mock
	StringWriter stringWriter;

	@Mock
	TypedQuery query;

	@Mock
	BoundaryQuery boundQuery;

	@Mock
	ResultSet mockResultSet, mockResultSetForRgnTZ;

	@Mock
	JobDictionaryQuery jobDictQuery;

	@Mock
	JobPropertyQuery jobPropQuery;

	@Mock
	GeneratorPropertyQuery genPropQuery;

	@Mock
	EntityManager entityManager;

	@Mock
	GetDBResource getDbResource;

	@Mock
	QueryExecutor queryExec;

	@Mock
	Session session;

	@Mock
	Connection mockConnection;

	@Mock
	AdaptationCatalogQuery adaptCataQuery;

	@Mock
	Statement mockStatement;

	@Mock
	ResultSet mockResultSetForEs_reagg_config;

	@Mock
	ResultSet mockResultSetRgnInfo;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	private final List<String> sourceJobIdList = new ArrayList<String>();

	private Map<String, String> runtimePropMap = new HashMap<String, String>();

	@Before
	public void setUp() throws Exception {
		mockStatic(HiveConfigurationProvider.class, HiveGeneratorUtil.class);
		getJobProp = new GetJobMetadata();
		Timestamp timeStamp = new Timestamp(
				new SimpleDateFormat("yyyy.MM.dd HH:mm:ss", Locale.ENGLISH)
						.parse("2012.10.21 10:30:00").getTime());
		// setting boundary values
		boundary.setSourceJobId("SOURCE_TEST_JOB");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setMaxValue(timeStamp);
		boundary.setSourceJobType("Aggregation");
		// setting to the list
		boundaryList.add(boundary);
		// mocking new object call
		PowerMockito.whenNew(Boundary.class).withNoArguments()
				.thenReturn(boundary);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundQuery);
		PowerMockito.whenNew(JobDictionaryQuery.class).withNoArguments()
				.thenReturn(jobDictQuery);
		PowerMockito.whenNew(JobPropertyQuery.class).withNoArguments()
				.thenReturn(jobPropQuery);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(AdaptationCatalogQuery.class).withNoArguments()
				.thenReturn(adaptCataQuery);
		PowerMockito.mockStatic(RetrieveDimensionValues.class);
		PowerMockito.when(RetrieveDimensionValues.getRuntimePropFromDB(context))
				.thenReturn(runtimePropMap);
		PowerMockito.whenNew(GeneratorPropertyQuery.class).withNoArguments()
				.thenReturn(genPropQuery);
		PowerMockito.whenNew(ReConnectUtil.class).withNoArguments()
				.thenReturn(reConnectUtil);
		PowerMockito.mockStatic(TimeZoneUtil.class);
		AdaptationCatalog adaptCata = new AdaptationCatalog();
		adaptCata.setAdaptationid("VOICE");
		adaptCata.setVersion("1");
		Mockito.when(adaptCataQuery.retrieve(Mockito.anyString(),
				Mockito.anyString())).thenReturn(adaptCata);
		context.setProperty(JobExecutionContext.ENTITY_MANAGER, entityManager);
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.POSTGRESDRIVER,
				"org.postgresql.Driver");
		context.setProperty(JobExecutionContext.POSTGRESURL,
				"jdbc:postgresql://localhost/sai");
		context.setProperty(JobExecutionContext.USERNAME, "rithomas");
		context.setProperty(JobExecutionContext.PASSWORD, "rithomas");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDbResource);
		Object[] resultSet = { "sql1", "sql2" };
		Object[] resultSetImportDir = { "sql1" };
		Object[] resultSetRuntimeProp = { "20" };
		String sqlToExecute = "select TO_EXECUTE,CUSTOM_EXECUTE from TO_EXECUTE where JOB_ID = 'Usage_VOICE_1_LoadJob'";
		String importDirSql = QueryConstants.USAGE_IMPORT_DIR
				.replace("$TNES_TYPE", "'Usage_VOICE_1_LoadJob'");
		String runtimePropSql = QueryConstants.RUNTIME_PROP_QUERY;
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(sqlToExecute, context))
				.thenReturn(resultSet);
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(importDirSql, context))
				.thenReturn(resultSetImportDir);
		PowerMockito.when(
				queryExec.executeMetadatasqlQuery(runtimePropSql, context))
				.thenReturn(resultSetRuntimeProp);
		PowerMockito.when(reConnectUtil.reconnectCount()).thenReturn(2);
		PowerMockito.when(reConnectUtil.reconnectWaitTime()).thenReturn(1000L);
		mockStatic(ConnectionManager.class);
		mockStatic(DBConnectionManager.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(mockConnection);
		Mockito.when(mockConnection.createStatement())
				.thenReturn(mockStatement);
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		Mockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		Mockito.when(mockHiveConfigurationProvider.shouldConnectToCustomDbUrl(
				Mockito.anyString(), Mockito.anyString())).thenReturn(false);
		JobProperty archProp = new JobProperty();
		archProp.setParamvalue("10");
		Mockito.when(
				jobPropQuery.retrieve(1, JobExecutionContext.ARCHIVING_DAYS))
				.thenReturn(archProp);
	}

	@Test
	public void testJobProperties()
			throws WorkFlowExecutionException, SQLException {
		sourceJobIdList.add("Usage_VOICE_1_LoadJob");
		jobPropertyPK.setParamname("RETENTIONDAYS");
		jobProp.setParamvalue("45");
		jobProp.setParamName("RETENTIONDAYS");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Usage_VOICE_1_LoadJob");
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(boundQuery.getSourceJobIds(Mockito.anyString()))
				.thenReturn(sourceJobIdList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testJobPropertiesWithTimeZoneIsYes() throws Exception {
		sourceJobIdList.add("Usage_VOICE_1_LoadJob");
		// setting jobPropertyPK
		jobPropertyPK.setParamname("RETENTIONDAYS");
		// setting jobProperty
		jobProp.setParamvalue("45");
		jobProp.setParamName("RETENTIONDAYS");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Loading");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		// setting to the list
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Usage_VOICE_1_LoadJob");
		context.setProperty("TIME_ZONE_SUPPORT", "YES");
		List<Object> resultSet = new ArrayList<Object>();
		resultSet.add("Region1");
		resultSet.add("Region2");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select distinct region_id from saidata.es_data_src_rgn_info_1",
				context)).thenReturn(resultSet);
		List<Object[]> resultSetEs_reagg_config = new ArrayList<Object[]>();
		resultSetEs_reagg_config.add(new Object[] { "IuPs", "Yes", "SGSN" });
		resultSetEs_reagg_config.add(new Object[] { "Gb", "No", "SGSN" });
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				"select param_name, param_value from saidata.es_reagg_config_1",
				context)).thenReturn(resultSetEs_reagg_config);
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		Object[] sqlForJobDictDepTableResultset = {};
		String sqlForJobDictDepTable = "select source_jobid from job_dict_dependency where bound_jobid = ( select id from boundary where jobid ='Usage_VOICE_1_LoadJob' and region_id is null)";
		Mockito.when(queryExec.executeMetadatasqlQuery(sqlForJobDictDepTable,
				context)).thenReturn(sqlForJobDictDepTableResultset);
		Object[] boundarySelectResultset = { "boundarySourceId",
				"boundarySourceJobType", "boundarySourcePartColumn",
				timeStamp };
		String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='Usage_VOICE_1_LoadJob' and region_id is null";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(sqlForBoundaryTable, context))
				.thenReturn(boundarySelectResultset);
		// Mocking query classes
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(boundQuery.getSourceJobIds(Mockito.anyString()))
				.thenReturn(sourceJobIdList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testJobPropertiesWithTimeZoneIsYesForReaggJobWithOnlyUnKnownRegionAvailable()
			throws Exception {
		sourceJobIdList.add("Perf_SGSN_1_HOUR_ReaggregateJob");
		// setting jobPropertyPK
		jobPropertyPK.setParamname("RETENTIONDAYS");
		// setting jobProperty
		jobProp.setParamvalue("45");
		jobProp.setParamName("RETENTIONDAYS");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Reaggregation");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		// setting to the list
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Perf_SGSN_1_HOUR_ReaggregateJob");
		context.setProperty("TIME_ZONE_SUPPORT", "YES");
		context.setProperty("PERF_JOB_NAME", "Perf_SGSN_1_HOUR_ReaggregateJob");
		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
		Mockito.when(mockResultSet.getString(1)).thenReturn("Unknown")
				.thenReturn("Unknown");
		Mockito.when(mockStatement.executeQuery(
				"select distinct region_id from es_data_src_rgn_info_1"))
				.thenReturn(mockResultSet);
		Mockito.when(mockStatement
				.executeQuery("select tmz_region from es_rgn_tz_info_1"))
				.thenReturn(mockResultSet);
		Mockito.when(mockResultSetForEs_reagg_config.next()).thenReturn(true)
				.thenReturn(false);
		Mockito.when(mockResultSetForEs_reagg_config.getString(1))
				.thenReturn("ParamName_OFFPEAKHOUR");
		Mockito.when(mockResultSetForEs_reagg_config.getString(2))
				.thenReturn("ParamValue_OFFPEAKHOUR");
		Mockito.when(mockStatement.executeQuery(
				"select param_name, param_value from es_reagg_config_1"))
				.thenReturn(mockResultSetForEs_reagg_config);
		// Mockito.doNothing().when(mockResultSetForDataSrcRgnInfo.getString(1));
		Object[] boundarySelectResultset = { "boundarySourceId",
				"boundarySourceJobType", "boundarySourcePartColumn",
				"boundaryMaxValue" };
		String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='Usage_VOICE_1_LoadJob' and region_id is null";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(sqlForBoundaryTable, context))
				.thenReturn(boundarySelectResultset);
		// Mocking query classes
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(boundQuery.getSourceJobIds(Mockito.anyString()))
				.thenReturn(sourceJobIdList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testJobPropertiesWithTimeZoneIsYesWithMultipleRegions()
			throws Exception {
		sourceJobIdList.add("Usage_VOICE_1_LoadJob");
		// setting jobPropertyPK
		jobPropertyPK.setParamname("RETENTIONDAYS");
		// setting jobProperty
		jobProp.setParamvalue("45");
		jobProp.setParamName("RETENTIONDAYS");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Loading");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		// setting to the list
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Usage_VOICE_1_LoadJob");
		context.setProperty("TIME_ZONE_SUPPORT", "YES");
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		Object[] boundarySelectResultset = { "boundarySourceId",
				"boundarySourceJobType", "boundarySourcePartColumn",
				timeStamp };
		String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='Usage_VOICE_1_LoadJob' and region_id is null";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(sqlForBoundaryTable, context))
				.thenReturn(boundarySelectResultset);
		Mockito.when(mockStatement.executeQuery(
				"select distinct region_id from es_data_src_rgn_info_1"))
				.thenReturn(mockResultSetRgnInfo);
		Mockito.when(mockResultSetRgnInfo.next()).thenReturn(true)
				.thenReturn(true).thenReturn(false);
		Mockito.when(mockResultSetRgnInfo.getString(1)).thenReturn("Region1")
				.thenReturn("Region1").thenReturn("Region2")
				.thenReturn("Region2");
		Mockito.when(mockStatement
				.executeQuery("select tmz_region from es_rgn_tz_info_1"))
				.thenReturn(mockResultSet);
		Mockito.when(mockStatement.executeQuery(
				"select param_name, param_value from es_reagg_config_1"))
				.thenReturn(mockResultSet);
		// Mockito.doNothing().when(mockResultSetForDataSrcRgnInfo.getString(1));
		// Mocking query classes
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(boundQuery.getSourceJobIds(Mockito.anyString()))
				.thenReturn(sourceJobIdList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testJobPropertiesWithTimeZoneIsYesWithNoRegions()
			throws Exception {
		sourceJobIdList.add("Usage_VOICE_1_LoadJob");
		// setting jobPropertyPK
		jobPropertyPK.setParamname("RETENTIONDAYS");
		// setting jobProperty
		jobProp.setParamvalue("45");
		jobProp.setParamName("RETENTIONDAYS");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Loading");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		// setting to the list
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Usage_VOICE_1_LoadJob");
		context.setProperty("TIME_ZONE_SUPPORT", "YES");
		Object[] boundarySelectResultset = { "boundarySourceId",
				"boundarySourceJobType", "boundarySourcePartColumn",
				"boundaryMaxValue" };
		String sqlForBoundaryTable = "select sourcejobid,sourcejobtype,sourcepartcolumn,maxvalue from boundary where jobid ='Usage_VOICE_1_LoadJob' and region_id is null";
		Mockito.when(
				queryExec.executeMetadatasqlQuery(sqlForBoundaryTable, context))
				.thenReturn(boundarySelectResultset);
		Mockito.when(mockStatement.executeQuery(
				"select distinct region_id from es_data_src_rgn_info_1"))
				.thenReturn(mockResultSetRgnInfo);
		Integer dummyValue = new Integer(1);
		BigInteger dummyBigInt = BigInteger.valueOf(dummyValue.intValue());
		Object[] resultSetWithNoRegions = { dummyBigInt };
		String sqlNoRegions = "select count(region_id) from boundary where jobid = 'Usage_VOICE_1_LoadJob';";
		Mockito.when(queryExec.executeMetadatasqlQuery(sqlNoRegions, context))
				.thenReturn(resultSetWithNoRegions);
		Mockito.when(mockResultSetRgnInfo.next()).thenReturn(false);
		Mockito.when(mockStatement
				.executeQuery("select tmz_region from es_rgn_tz_info_1"))
				.thenReturn(mockResultSet);
		Mockito.when(mockStatement.executeQuery(
				"select param_name, param_value from es_reagg_config_1"))
				.thenReturn(mockResultSet);
		// Mocking query classes
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(boundQuery.getSourceJobIds(Mockito.anyString()))
				.thenReturn(sourceJobIdList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testGetMetadataForEmptyBoundary()
			throws WorkFlowExecutionException {
		try {
			context.setProperty(JobExecutionContext.JOB_NAME,
					"Usage_VOICE_1_LoadJob");
			jobPropertyPK.setParamname("RETENTIONDAYS");
			jobProp.setParamvalue("45");
			jobProp.setParamName("RETENTIONDAYS");
			jobProp.setJobPropPK(jobPropertyPK);
			jobPropList.add(jobProp);
			jobProperties.setJobProperty(jobPropList);
			jobTypeDictionary.setJobtypeid("Aggregation");
			jobDictionary.setTypeid(jobTypeDictionary);
			jobDictionary.setId(1);
			jobDictionary.setJobProperties(jobProperties);
			Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
					.thenReturn(jobDictionary);
			Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
					.thenReturn(boundaryListEmpty);
			boolean success = getJobProp.execute(context);
			assertEquals(false, success);
		} catch (WorkFlowExecutionException e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testJobPropertiesWithNullContext() {
		try {
			getJobProp.execute(null);
			Assert.fail("Operation didn't fail with null context");
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testJobPropertiesForEmptyJobProp() {
		try {
			context.setProperty("JOB_NAME", "Usage_VOICE_1_LoadJob");
			jobProperties.setJobProperty(jobPropList);
			jobTypeDictionary.setJobtypeid("Aggregation");
			jobDictionary.setTypeid(jobTypeDictionary);
			jobDictionary.setId(1);
			jobDictionary.setJobProperties(jobProperties);
			// setting to the list
			jobDictList.add(jobDictionary);
			// Mocking query classes
			Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
					.thenReturn(boundaryList);
			Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
					.thenReturn(jobDictionary);
			Mockito.when(jobPropQuery.retrieve(1)).thenReturn(jobPropList);
			boolean success = getJobProp.execute(context);
			assertEquals(false, success);
		} catch (WorkFlowExecutionException e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testGetMetadataWithSQLException() throws Exception {
		jobPropertyPK.setParamname("TEST_NAME");
		jobProp.setParamvalue("TEST_VALUE");
		jobProp.setParamName("TEST_NAME");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Aggregation");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		jobDictionary.setJobProperties(jobProperties);
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "TEST_JOB");
		context.setProperty(JobExecutionContext.POSTGRESDRIVER,
				"org.postgresql.Driver");
		Mockito.when(boundQuery.retrieveByJobId(Mockito.anyString()))
				.thenReturn(boundaryList);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		PowerMockito.when(mockResultSetRgnInfo.getObject(0))
				.thenThrow(new SQLException(""));
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}

	@Test
	public void testGetMetaDataForEntityJobPKCols() throws Exception {
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.ENTITY_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SGSN_AVAILABLE_DATA_1_CorrelationJob");
		context.setProperty(JobExecutionContext.ADAPTATION_ID,
				"COMMON_DIMENSION");
		context.setProperty(JobExecutionContext.UNIQUE_KEY, "TYPE,SOURCE");
		context.setProperty(JobExecutionContext.SOURCE, "TYPE,SOURCE");
		context.setProperty(JobExecutionContext.ADAPTATION_VERSION, "1");
		jobPropertyPK.setParamname("TARGET");
		jobProp = new JobProperty();
		jobProp.setJobId(1);
		jobProp.setParamName("TARGET");
		jobProp.setParamvalue("saidata.ES_SGSN_AVAILABLE_DATA_1");
		jobProp.setJobPropPK(jobPropertyPK);
		jobPropList.add(jobProp);
		jobProperties.setJobProperty(jobPropList);
		jobTypeDictionary.setJobtypeid("Correlation");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setJobProperties(jobProperties);
		jobDictionary.setId(1);
		jobDictionary.setAdaptationId("COMMON_DIMENSION");
		jobDictionary.setAdaptationVersion("1");
		jobDictionary.setJobid("Entity_SGSN_AVAILABLE_DATA_1_CorrelationJob");
		Mockito.when(boundQuery
				.retrieveByJobId("Entity_SGSN_AVAILABLE_DATA_1_CorrelationJob"))
				.thenReturn(boundaryListEmpty);
		Mockito.when(jobDictQuery
				.retrieve("Entity_SGSN_AVAILABLE_DATA_1_CorrelationJob"))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
		List<EntitySpecificationCharacteristicUse> esCharUseList = new ArrayList<EntitySpecificationCharacteristicUse>();
		CharacteristicSpecification charSpec = new CharacteristicSpecification();
		EntitySpecificationCharacteristicUse esCharUse = new EntitySpecificationCharacteristicUse();
		charSpec.setName("TYPE");
		charSpec.setUnik("Unique");
		esCharUse.setSpecChar(charSpec);
		esCharUseList.add(esCharUse);
		esCharUse = new EntitySpecificationCharacteristicUse();
		charSpec = new CharacteristicSpecification();
		charSpec.setName("AVAILABLE");
		esCharUse.setSpecChar(charSpec);
		esCharUseList.add(esCharUse);
		esCharUse = new EntitySpecificationCharacteristicUse();
		charSpec = new CharacteristicSpecification();
		charSpec.setName("TYPE,SOURCE");
		charSpec.setUnik("Unique");
		esCharUse.setSpecChar(charSpec);
		esCharUseList.add(esCharUse);
		EntitySpecification entitySpecification = new EntitySpecification();
		entitySpecification.setSpecId("SGSN_AVAILABLE_DATA");
		entitySpecification.setVersion("1");
		entitySpecification
				.setEntitySpecificationCharacteristicUse(esCharUseList);
		EntitySpecificationQuery mockEntitySpecificationQuery = mock(
				EntitySpecificationQuery.class);
		PowerMockito.whenNew(EntitySpecificationQuery.class).withNoArguments()
				.thenReturn(mockEntitySpecificationQuery);
		PowerMockito
				.when(mockEntitySpecificationQuery
						.retrieve("SGSN_AVAILABLE_DATA", "1"))
				.thenReturn(entitySpecification);
		List<String> list = new ArrayList<>();
		list.add("sourceTable");
		Mockito.when(
				HiveGeneratorUtil.getSpecIDVerFromTabName(Mockito.anyString()))
				.thenReturn(list);
		assertEquals("TYPE,SOURCE",
				context.getProperty(JobExecutionContext.UNIQUE_KEY));
	}

	@Test
	public void testExecuteWhenQSJob()
			throws WorkFlowExecutionException, SQLException {
		jobTypeDictionary.setJobtypeid("Queryscheduler");
		jobDictionary.setTypeid(jobTypeDictionary);
		jobDictionary.setId(1);
		// setting to the list
		jobDictList.add(jobDictionary);
		context.setProperty("JOB_NAME", "Entity_SMS_TYPE_1_QSJob");
		// Mocking query classes
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		boolean success = getJobProp.execute(context);
		assertEquals(true, success);
	}
}

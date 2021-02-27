
package com.project.rithomas.jobexecution.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveRowCountUtil;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.etl.exception.ETLException;
import com.project.rithomas.jobexecution.common.DQICalculator;
import com.project.rithomas.jobexecution.common.GetJobMetadata;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.CommonAggregationUtil;
import com.project.rithomas.jobexecution.common.util.DBConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@Ignore
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider")
@PrepareForTest(value = { AggregationManager.class, DBConnectionManager.class,
		UpdateJobStatus.class, TimeZoneUtil.class, UpdateBoundary.class,
		QueryExecutor.class, ResultSet.class, AggregationUtil.class,
		ConnectionManager.class, ReConnectUtil.class, GetJobMetadata.class,
		Connection.class, DateFunctionTransformation.class, Executors.class,
		RetrieveDimensionValues.class, HiveConfigurationProvider.class,
		HiveJobRunner.class, schedulerJobRunner.class,
		CommonAggregationUtil.class, DayLightSavingUtil.class,
		TimeZoneUtil.class, })
public class AggregationManagerTest {

	WorkFlowContext context = new JobExecutionContext();

	AggregationManager aggManager = null;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	UpdateBoundary updateBoundary;

	@Mock
	QueryExecutor queryExec;

	@Mock
	Session session;

	@Mock
	DBConnectionManager dbConnManager;

	@Mock
	ResultSet result;

	@Mock
	Connection connection;

	@Mock
	GetJobMetadata getMetadata;

	@Mock
	PreparedStatement statement;

	@Mock
	DQICalculator dqiCalculator;

	@Mock
	UsageAggregationStatusUtil usageAggStatusUtil;

	@Mock
	ExecutorService mockExecutorService;

	@Mock
	HiveRowCountUtil hiveRowCountUtil;

	@Mock
	RetrieveDimensionValues retrieveDimensionValues;

	@Mock
	RuntimePropertyQuery runtimePropertyQuery;

	@Mock
	HiveConfigurationProvider mockHiveConfigurationProvider;

	@Mock
	HiveJobRunner JobRunner;

	@Mock
	AggregationUtil aggregationUtil;

	@Mock
	DayLightSavingUtil dayLightSavingUtil;

	@Mock
	ExecutorService executors;

	@Mock
	TimeZoneUtil timeZoneUtil;

	List<String> hiveSetting = new ArrayList<String>();
	{
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION uf_trunc AS 'com.project.rithomas.hive.udf.TruncUDF'");
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION nvl AS 'com.nexr.platform.hive.udf.GenericUDFNVL'");
		hiveSetting.add(
				"CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'");
	}

	@SuppressWarnings("deprecation")
	@Before
	public void setUp() throws Exception {
		aggManager = new AggregationManager();
		Whitebox.setInternalState(aggManager, "aggregationUtil",
				aggregationUtil);
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments()
				.thenReturn(updateBoundary);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(GetJobMetadata.class).withNoArguments()
				.thenReturn(getMetadata);
		PowerMockito.whenNew(DQICalculator.class).withNoArguments()
				.thenReturn(dqiCalculator);
		PowerMockito.whenNew(UsageAggregationStatusUtil.class).withNoArguments()
				.thenReturn(usageAggStatusUtil);
		PowerMockito.whenNew(HiveRowCountUtil.class).withNoArguments()
				.thenReturn(hiveRowCountUtil);
		PowerMockito.whenNew(RuntimePropertyQuery.class).withNoArguments()
				.thenReturn(runtimePropertyQuery);
		PowerMockito.whenNew(RetrieveDimensionValues.class).withNoArguments()
				.thenReturn(retrieveDimensionValues);
		// mock static class
		mockStatic(DBConnectionManager.class, RetrieveDimensionValues.class,
				schedulerJobRunner.class);
		mockStatic(HiveConfigurationProvider.class);
		mockStatic(CommonAggregationUtil.class);
		PowerMockito.when(DBConnectionManager.getInstance())
				.thenReturn(dbConnManager);
		PowerMockito.whenNew(HiveConfigurationProvider.class).withNoArguments()
				.thenReturn(mockHiveConfigurationProvider);
		PowerMockito.when(HiveConfigurationProvider.getInstance())
				.thenReturn(mockHiveConfigurationProvider);
		context.setProperty(JobExecutionContext.LB,
				Long.parseLong("1354307400000"));
		context.setProperty(JobExecutionContext.UB,
				Long.parseLong("1354321800000"));
		context.setProperty(JobExecutionContext.NEXT_LB,
				Long.parseLong("1354311000000"));
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT,
				Long.parseLong("4"));
		context.setProperty(JobExecutionContext.QUERY_TIMEOUT,
				Long.parseLong("1800"));
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(ReConnectUtil.HIVE_RETRY_COUNT, 2);
		context.setProperty(ReConnectUtil.HIVE_RETRY_WAIT_INTERVAL, 1000L);
		context.setProperty(JobExecutionContext.DQM_ENABLED, "NO");
		context.setProperty(JobExecutionContext.DQI_ENABLED, "NO");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_2G, "50");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_3G, "200");
		context.setProperty(JobExecutionContext.CUTOFF_VOLUME_4G, "500");
		context.setProperty(JobExecutionContext.UB_DST_COUNT, 0);
		context.setProperty(JobExecutionContext.TYPE_AVAILABLE_HO,
				"'IuPs','3G'");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		context.setProperty(JobExecutionContext.TYPE_AVAILABLE_SGSN,
				"'IuPs','2G_3G','2G','2G_3G','4G','3G','Gb','4G'");
		mockStatic(ConnectionManager.class);
		PowerMockito
				.when(HiveConfigurationProvider.getInstance().getQueryHints(
						"Perf_VLR_1_HOUR_AggregateJob",
						schedulerConstants.HIVE_DATABASE))
				.thenReturn(hiveSetting);
		PowerMockito.when(dbConnManager.getConnection(Mockito.anyString(),
				Mockito.anyBoolean())).thenReturn(connection);
		PowerMockito
				.when(dbConnManager.getHiveConnection(
						"org.apache.hive.jdbc.HiveDriver",
						"jdbc:hive2://host:port/dbname", "", ""))
				.thenReturn(connection);
		context.setProperty("SOURCEJOBID", "SOURCEJOBID1");
		String query1 = "select region_id from rithomas.boundary where jobid ='SOURCEJOBID1'";
		List<String> regionIds = new ArrayList<String>();
		regionIds.add("Region1");
		regionIds.add("Region2");
		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
				regionIds);
		Mockito.when(queryExec.executeMetadatasqlQueryMultiple(query1, context))
				.thenReturn(regionIds);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.when(connection.prepareStatement(Mockito.anyString()))
				.thenReturn(statement);
		PowerMockito.when(runtimePropertyQuery
				.retrieve("RETRY_HIVE_ALL_EXCEPTIONS", "true"))
				.thenReturn("false");
		context.setProperty(JobExecutionContext.EXECUTION_ENGINE, "HIVE");
		String jobType = "insert overwrite table PS_SMS_1_15MIN partition(dt='1395588600000' )select '2014-03-23 21:00:00',SMS_TYPE,CLEAR_CODE_ID,IMSI,SAC_ID,CELL_ID,LAC,IMEI,SOURCE_ID,decode(SMS_TYPE,0,ORIG_CALLING_NUM,2,MSISDN,-10) MSISDN,substr(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM),1,6),length(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM)),nvl(CELL_ID,SAC_ID),RADIO_ACCESS_INFO,-10,-10,-11+1,round(sum(nvl((DELIVERY_TIME-INCOMING_TIME)/1000,0))),count(*),sum(SMS_LENGTH) from US_SMS_1 where US_SMS_1.dt >='1395588600000' and US_SMS_1.dt<'1395589500000'  AND US_SMS_1.SMS_TYPE in (0,2) group by '2014-03-23 21:00:00',SMS_TYPE,CLEAR_CODE_ID,IMSI,SAC_ID,CELL_ID,LAC,IMEI,SOURCE_ID,decode(SMS_TYPE,0,ORIG_CALLING_NUM,2,MSISDN,-10),substr(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM),1,6),length(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM)),nvl(CELL_ID,SAC_ID),RADIO_ACCESS_INFO,-12+2,-10,-11+1";
		Mockito.when(RetrieveDimensionValues.replaceDynamicValuesInJobSQL(
				Matchers.any(), Matchers.anyString(), Matchers.any()))
				.thenReturn(jobType);
	}

	@Test
	public void testAggregationManagerWithHourInterval()
			throws WorkFlowExecutionException, JobExecutionException,
			SQLException, ClassNotFoundException {
		context.setProperty(JobExecutionContext.SQL,
				QueryConstants.HOME_COUNTRY_CODE_QUERY, "91", "INTEGER",
				"POSTGRES" });
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						QueryConstants.DYNAMIC_PARAM_QUERY.replace(
								JobExecutionContext.ENTITY_JOB_NAME,
								(String) context.getProperty(
										JobExecutionContext.JOB_NAME)),
						context))
				.thenReturn(resultSet1);
		List<Object> resultSet = new ArrayList<Object>();
		resultSet.add("91");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						QueryConstants.HOME_COUNTRY_CODE_QUERY, context))
				.thenReturn(resultSet);
		Mockito.when(dayLightSavingUtil
				.getCntIfLBInTimeZoneDST(truncCalLBForDST, null, context))
				.thenReturn(2);
		List<String> availableCheck = new ArrayList<String>();
		availableCheck.add("SGSN,Yes");
		context.setProperty(JobExecutionContext.HIVEDRIVER,
				"org.apache.hadoop.hive.jdbc.HiveDriver");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE,
				JobTypeDictionary.USAGE_JOB_TYPE);
		context.setProperty(JobExecutionContext.RETURN, 0);
		settingRegionsValue();
		context.setProperty("UB_DST_COUNT_Region1", 1);
		context.setProperty("UB_DST_COUNT_Region2", 1);
		List<Object[]> resultSet1 = new ArrayList<Object[]>();
		PowerMockito.when(statement.executeQuery(sql)).thenReturn(result);
		PowerMockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		PowerMockito.when(CommonAggregationUtil.replaceKPIFormula(
				Mockito.anyString(), Mockito.anyString(),
				Mockito.any(WorkFlowContext.class))).thenReturn(sql);
		PowerMockito
				.when(aggregationUtil
						.replaceKPIFormula(Mockito.any(WorkFlowContext.class)))
				.thenReturn(sql);
		mockStatic(TimeZoneUtil.class);
		Mockito.when(TimeZoneUtil.isTimeZoneEnabled(context)).thenReturn(true);
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
		assertTrue(aggManager.execute(context));
	}

	public void settingRegionsValue() {
		context.setProperty("LB_Region1", Long.parseLong("1581274500000"));
		context.setProperty("LB_Region2", Long.parseLong("1581274800000"));
		context.setProperty("UB_Region1", Long.parseLong("1581276900000"));
		context.setProperty("UB_Region2", Long.parseLong("1581279900000"));
		context.setProperty("NEXT_LB_Region1", Long.parseLong("1581276600000"));
		context.setProperty("NEXT_LB_Region2", Long.parseLong("1581276700000"));
	}

	@Test
	public void testAggregationManagerWithSourceJobLoading()
			throws WorkFlowExecutionException, JobExecutionException,
			SQLException, ClassNotFoundException {
		context.setProperty(JobExecutionContext.SQL,
		PowerMockito.when(statement.executeQuery(sql)).thenReturn(result);
		PowerMockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		PowerMockito.when(CommonAggregationUtil.replaceKPIFormula(
				Mockito.anyString(), Mockito.anyString(),
				Mockito.any(WorkFlowContext.class))).thenReturn(sql);
		PowerMockito
				.when(aggregationUtil
						.replaceKPIFormula(Mockito.any(WorkFlowContext.class)))
				.thenReturn(sql);
		assertTrue(aggManager.execute(context));
	}

	@Test
	public void testAggregationManagerWithHourIntervalForMGWWithTimer()
			throws WorkFlowExecutionException, JobExecutionException,
			SQLException, ClassNotFoundException {
		context.setProperty(JobExecutionContext.SQL,
		PowerMockito.when(statement.executeQuery(sql)).thenReturn(result);
		PowerMockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		PowerMockito
				.when(aggregationUtil
						.replaceKPIFormula(Mockito.any(WorkFlowContext.class)))
				.thenReturn(sql);
		PowerMockito.when(CommonAggregationUtil.replaceKPIFormula(
				Mockito.anyString(), Mockito.anyString(),
				Mockito.any(WorkFlowContext.class))).thenReturn(sql);
		assertTrue(aggManager.execute(context));
	}

	@Test
	public void testAggregationManagerWithHourIntervalWithHomeIMSICode()
			throws WorkFlowExecutionException, JobExecutionException,
			SQLException {
		context.setProperty(JobExecutionContext.SQL,
				.thenReturn(result);
		PowerMockito.when(statement.executeQuery(Mockito.anyString()))
				.thenReturn(result);
		assertTrue(aggManager.execute(context));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testAggregationManagerJobExecutionException() throws Exception {
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "SOURCEJOBTYPE");
		context.setProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC,
				"LBUB_SECOND_TIME_EXEC");
		context.setProperty(JobTypeDictionary.USAGE_JOB_TYPE, "SOURCEJOBTYPE");
		context.setProperty(JobExecutionContext.RETURN, 10);
		context.setProperty(JobExecutionContext.SQL,
		String sql = "insert into table PS_VLR_1_HOUR partition(dt='1354307400000') select msisdn,cell,sac from PS_VLR_1_15MIN where dt >= '1354307400000' and dt < '1354311000000'";
		PowerMockito
				.when(aggregationUtil
						.replaceKPIFormula(Mockito.any(WorkFlowContext.class)))
				.thenReturn(
						(String) context.getProperty(JobExecutionContext.SQL));
		PowerMockito.when(CommonAggregationUtil.replaceKPIFormula(
				Mockito.anyString(), Mockito.anyString(),
				Mockito.any(WorkFlowContext.class))).thenReturn(sql);
		PowerMockito.when(statement.execute()).thenThrow(new SQLException(""));
		PowerMockito.when(aggregationUtil.replaceKPIFormula(context))
				.thenReturn(sql);
		try {
			aggManager.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains("Exception"));
		}
	}

	@Test
	public void testAggregationManagerETLException()
			throws WorkFlowExecutionException, JobExecutionException,
			ClassNotFoundException, SQLException, NumberFormatException,
			ETLException {
		context.setProperty(JobExecutionContext.SQL,
		AggregationManager aggManager = new AggregationManager();
		Calendar mockCalendar = PowerMockito.mock(Calendar.class);
		DateFunctionTransformation mockDateFn = PowerMockito
				.mock(DateFunctionTransformation.class);
		Mockito.when(mockCalendar.getTime()).thenReturn(new Date(1574674740l));
		PowerMockito.when(mockDateFn.getFormattedDate(Mockito.anyLong()))
				.thenReturn("1354307400000");
		long lb = 1395588600000L;
		String regionId = "tz='RGN1'";
		String expected = "insert overwrite table PS_SMS_1_15MIN partition(dt='1395588600000' )select '2014-03-23 21:00:00',SMS_TYPE,CLEAR_CODE_ID,IMSI,SAC_ID,CELL_ID,LAC,IMEI,SOURCE_ID,decode(SMS_TYPE,0,ORIG_CALLING_NUM,2,MSISDN,-10) MSISDN,substr(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM),1,6),length(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM)),nvl(CELL_ID,SAC_ID),RADIO_ACCESS_INFO,-10,-10,-11+1,round(sum(nvl((DELIVERY_TIME-INCOMING_TIME)/1000,0))),count(*),sum(SMS_LENGTH) from US_SMS_1 where US_SMS_1.dt >='1395588600000' and US_SMS_1.dt<'1395589500000'  AND US_SMS_1.SMS_TYPE in (0,2) group by '2014-03-23 21:00:00',SMS_TYPE,CLEAR_CODE_ID,IMSI,SAC_ID,CELL_ID,LAC,IMEI,SOURCE_ID,decode(SMS_TYPE,0,ORIG_CALLING_NUM,2,MSISDN,-10),substr(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM),1,6),length(decode(SMS_TYPE,0,CALLED_NUMBER,2,ORIG_CALLING_NUM)),nvl(CELL_ID,SAC_ID),RADIO_ACCESS_INFO,-12+2,-10,-11+1";
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		PowerMockito.when(aggregationUtil.replaceKPIFormula(context))
				.thenReturn(expected);
		PowerMockito
				.when(CommonAggregationUtil.replaceKPIFormula(
						Mockito.anyString(), Mockito.anyString(),
						Mockito.any(WorkFlowContext.class)))
				.thenReturn(expected);
		schedulerJobRunner jobRunner = schedulerJobRunnerfactory
				.getRunner(JobExecutionContext.HIVE_DB, false);
		assertEquals(expected, aggManager.getSqlToExecute(mockCalendar, 123L,
				lb, 1395589500000L, context, regionId, jobRunner));
	}
}

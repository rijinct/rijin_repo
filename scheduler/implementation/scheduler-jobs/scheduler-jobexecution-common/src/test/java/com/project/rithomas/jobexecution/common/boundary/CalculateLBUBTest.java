
package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { CalculateLBUB.class, BoundaryCalculator.class,
		JobPropertyRetriever.class, BoundaryCalculatorFactory.class,
		BoundaryQuery.class, TimeZoneUtil.class, DayLightSavingUtil.class,
		QueryExecutor.class, MultiSourceWithoutCache.class,
		MultiSourceWithCache.class, JobDictionaryQuery.class,
		JobDictionary.class, DataAvailabilityCacheUtil.class,
		MultiSourceWithCache.class, JobDetails.class })
public class CalculateLBUBTest {
	// WorkFlowContext context = new JobExecutionContext();

	WorkFlowContext context = new JobExecutionContext();

	CalculateLBUB calculateLBUB = new CalculateLBUB();

	@Mock
	ModifySourceJobList modifySourceJobList;

	@Mock
	BoundaryQuery boundaryQueryMock;

	@Mock
	SingleSource singleSourceMock;

	@Mock
	BoundaryCalculator boundaryCalculatorMock;

	@Mock
	QueryExecutor queryExec;

	@Mock
	JobDictionary jobDictionary;

	@Mock
	Boundary boundaryMock;

	@Mock
	BoundaryCalculatorFactory boundaryCalculatorFactoryMock;

	@Mock
	JobDictionaryQuery jobDictionaryQuery;

	@Mock
	ResultSet mockResultSet;

	@Mock
	DataAvailabilityCacheUtil dataAvailabilityCacheUtil;

	@Mock
	DayLightSavingUtil dayLightSavingUtil;

	BoundaryQuery boundaryQuery = new BoundaryQuery();

	Boundary boundary = new Boundary();

	Boundary srcBoundary = new Boundary();

	Boundary usBoundary = new Boundary();

	List<String> timeZoneRgns = new ArrayList<String>();

	List<Boundary> srcBoundaryList = new ArrayList<Boundary>();

	List<Boundary> usBoundaryList = new ArrayList<Boundary>();

	Calendar calendar = new GregorianCalendar();

	Calendar calendarDSTFor15MIN = new GregorianCalendar();

	List<Boundary> boundaryList = new ArrayList<Boundary>();

	@Mock
	private WorkFlowContext workflowContextMock;

	@Mock
	JobDetails jobdetailsMock;

	@Mock
	JobPropertyRetriever jobPropertyRetrieverMock;

	@Mock
	RuntimePropertyRetriever runTimePropertyRetriever;

	@Mock
	Calendar calendarMock;

	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.SOURCERETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCE_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS, "200");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "200");
		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setSourceJobType("Aggregation");
		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
		boundary.setId(1);
		srcBoundary.setSourceJobId("Usage_VLR_1_LoadJob");
		srcBoundary.setSourcePartColumn("REPORT_TIME");
		srcBoundary.setSourceJobType("Loading");
		srcBoundary.setJobId("Perf_VLR_1_15MIN_AggregateJob");
		srcBoundary.setId(1);
		usBoundary.setJobId("Usage_VLR_1_LoadJob");
		usBoundary.setId(1);
		calendar.add(Calendar.HOUR_OF_DAY, -5);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendarDSTFor15MIN.add(Calendar.HOUR_OF_DAY, -5);
		calendarDSTFor15MIN.set(Calendar.MINUTE, 15);
		calendarDSTFor15MIN.set(Calendar.SECOND, 0);
		calendarDSTFor15MIN.set(Calendar.MILLISECOND, 0);
		PowerMockito.whenNew(Calendar.class).withNoArguments()
				.thenReturn(calendarMock);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQueryMock);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
	}

	public static void setValuesForBoundary(WorkFlowContext context,
			QueryExecutor queryExec, JobDictionaryQuery jobDictionaryQuery,
			JobDictionary jobDictionary,
			DataAvailabilityCacheUtil dataAvailabilityCacheUtil)
			throws Exception {
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		PowerMockito.whenNew(JobDictionaryQuery.class).withAnyArguments()
				.thenReturn(jobDictionaryQuery);
		PowerMockito.whenNew(JobDictionary.class).withAnyArguments()
				.thenReturn(jobDictionary);
		PowerMockito.when(jobDictionaryQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		PowerMockito.whenNew(DataAvailabilityCacheUtil.class).withAnyArguments()
				.thenReturn(dataAvailabilityCacheUtil);
		mockStatic(TimeZoneUtil.class);
		PowerMockito.when(TimeZoneUtil.getZoneId(context, null))
				.thenReturn(null);
		String partitionUpdateFormula = "{HH24-5 20},{MIN15-5 20},{HH24 96},{MIN15 96}";
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Loading";
		sourceJobIdList.add("Usage_SMS_1_LoadJob");
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		// Setting context
		context.setProperty("PARTITION_UPDATE_FORMULA", partitionUpdateFormula);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "200");
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "MONDAY");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		// sourceJobIdList.add(jobName);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty("Usage_SMS_1_LoadJob" + "_PARTITION_CHECK_REQUIRED",
				true);
		context.setProperty("Usage_SMS_1_LoadJob" + "_PARTITION_UPDATE_FORMULA",
				"job,level");
		context.setProperty("Usage_SMS_1_LoadJob_RETENTIONDAYS", "15");
	}

	@Test
	public void testScenario1() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_SMS_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>();
		stageTypeList.add("MSS");
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SMS");
		PowerMockito.when(dataAvailabilityCacheUtil.getValue(Mockito.any()))
				.thenReturn("11");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.DAY_OF_MONTH, -15);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario2() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_SMS_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("MSS"));
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.set(Calendar.HOUR, 23);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SMS");
		PowerMockito.when(dataAvailabilityCacheUtil.getValue(Mockito.any()))
				.thenReturn("1");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario3() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_SMS_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("MSS"));
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SMS");
		PowerMockito.when(dataAvailabilityCacheUtil.getValue(Mockito.any()))
				.thenReturn("123");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario4() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_SMS_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("MSS", "SGS_4G"));
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SMS");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn(null).thenReturn("123");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario5() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_SMS_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_SMS_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("MSS", "SGS_4G"));
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SMS");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1549305000000");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario7() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String UsageSourceJobId1 = "Usage_LTE_4G_1_LoadJob";
		String UsageSourceJobId2 = "Usage_SGSN_1_LoadJob";
		context.setProperty(UsageSourceJobId2 + "_PARTITION_CHECK_REQUIRED",
				true);
		context.setProperty(UsageSourceJobId2 + "_PARTITION_UPDATE_FORMULA",
				"job,level");
		context.setProperty(UsageSourceJobId1 + "_PARTITION_CHECK_REQUIRED",
				true);
		context.setProperty(UsageSourceJobId1 + "_PARTITION_UPDATE_FORMULA",
				"job,level");
		List<String> sourceJobIdList = new ArrayList<String>(
				Arrays.asList(UsageSourceJobId1, UsageSourceJobId2));
		String jobName = "Perf_DATA_CP_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("2G", "3G", "Gb", "IuPs"));
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(UsageSourceJobId2);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceLTEList = new ArrayList<Boundary>();
		Boundary boundarySourceLTE = new Boundary();
		boundarySourceLTE.setJobId(UsageSourceJobId1);
		boundarySourceLTE.setMaxValue(timeBoundary);
		boundarySourceLTEList.add(boundarySourceLTE);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(UsageSourceJobId2))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(UsageSourceJobId1))
				.thenReturn(boundarySourceLTEList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito.when(
				dataAvailabilityCacheUtil.getValue(Mockito.anyString() + "2G"))
				.thenReturn("1");
		PowerMockito.when(
				dataAvailabilityCacheUtil.getValue(Mockito.anyString() + "3G"))
				.thenReturn("2");
		PowerMockito.when(
				dataAvailabilityCacheUtil.getValue(Mockito.anyString() + "Gb"))
				.thenReturn("3");
		PowerMockito
				.when(dataAvailabilityCacheUtil
						.getValue(Mockito.anyString() + "IuPs"))
				.thenReturn(null).thenReturn("4");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario8() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String UsageSourceJobId1 = "Usage_LTE_4G_1_LoadJob";
		String UsageSourceJobId2 = "Usage_SGSN_1_LoadJob";
		context.setProperty(UsageSourceJobId2 + "_PARTITION_CHECK_REQUIRED",
				true);
		context.setProperty(UsageSourceJobId2 + "_PARTITION_UPDATE_FORMULA",
				"job,level");
		context.setProperty(UsageSourceJobId1 + "_PARTITION_CHECK_REQUIRED",
				true);
		context.setProperty(UsageSourceJobId1 + "_PARTITION_UPDATE_FORMULA",
				"job,level");
		List<String> sourceJobIdList = new ArrayList<String>(
				Arrays.asList(UsageSourceJobId1, UsageSourceJobId2));
		String jobName = "Perf_DATA_CP_SEGG_1_HOUR_AggregateJob";
		List<Object> stageTypeList = new ArrayList<Object>(
				Arrays.asList("2G", "3G", "Gb", "IuPs", "S1MME"));
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(UsageSourceJobId2);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		calBoundary.add(Calendar.HOUR, 2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceLTEList = new ArrayList<Boundary>();
		Boundary boundarySourceLTE = new Boundary();
		boundarySourceLTE.setJobId(UsageSourceJobId1);
		boundarySourceLTE.setMaxValue(timeBoundary);
		boundarySourceLTEList.add(boundarySourceLTE);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(UsageSourceJobId2))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(UsageSourceJobId1))
				.thenReturn(boundarySourceLTEList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		PowerMockito
				.when(queryExec.executeMetadatasqlQueryMultiple(
						Mockito.anyString(), Mockito.any()))
				.thenReturn(stageTypeList);
		assertTrue(calculateLBUB.execute(context));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		calBoundary.add(Calendar.HOUR, -1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario12() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String usageSourceJobId1 = "Usage_S1U_1_LoadJob";
		String usageSourceJobId2 = "Usage_GNUP_1_LoadJob";
		List<String> sourceJobIdList = new ArrayList<String>(
				Arrays.asList(usageSourceJobId1, usageSourceJobId2));
		String jobName = "Perf_DATA_UP_SEGG_1_HOUR_AggregateJob";
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(usageSourceJobId1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceLTEList = new ArrayList<Boundary>();
		Boundary boundarySourceLTE = new Boundary();
		boundarySourceLTE.setJobId(usageSourceJobId2);
		boundarySourceLTE.setMaxValue(timeBoundary);
		boundarySourceLTEList.add(boundarySourceLTE);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId1))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId2))
				.thenReturn(boundarySourceLTEList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		calBoundary.add(Calendar.HOUR, -1);
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario13() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String usageSourceJobId1 = "Usage_S1U_1_LoadJob";
		String usageSourceJobId2 = "Usage_GNUP_1_LoadJob";
		List<String> sourceJobIdList = new ArrayList<String>(
				Arrays.asList(usageSourceJobId1, usageSourceJobId2));
		String jobName = "Perf_DATA_UP_SEGG_1_HOUR_AggregateJob";
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(usageSourceJobId1);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		calBoundary.add(Calendar.HOUR, 2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceLTEList = new ArrayList<Boundary>();
		Boundary boundarySourceLTE = new Boundary();
		boundarySourceLTE.setJobId(usageSourceJobId2);
		boundarySourceLTE.setMaxValue(timeBoundary);
		boundarySourceLTEList.add(boundarySourceLTE);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId1))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId2))
				.thenReturn(boundarySourceLTEList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.HOUR, -1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario14() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String usageSourceJobId1 = "Usage_S1U_1_LoadJob";
		String usageSourceJobId2 = "Usage_GNUP_1_LoadJob";
		List<String> sourceJobIdList = new ArrayList<String>(
				Arrays.asList(usageSourceJobId1, usageSourceJobId2));
		String jobName = "Perf_DATA_UP_SEGG_1_HOUR_AggregateJob";
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(usageSourceJobId1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.HOUR, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		calBoundary.add(Calendar.HOUR, 1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceLTEList = new ArrayList<Boundary>();
		Boundary boundarySourceLTE = new Boundary();
		boundarySourceLTE.setJobId(usageSourceJobId2);
		boundarySourceLTE.setMaxValue(timeBoundary);
		boundarySourceLTEList.add(boundarySourceLTE);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId1))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(usageSourceJobId2))
				.thenReturn(boundarySourceLTEList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.HOUR, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		calBoundary.add(Calendar.HOUR, -1);
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario15() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		String sourceJobId1 = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		String sourceJobId2 = "Perf_DATA_UP_SEGG_1_DAY_AggregateJob";
		String sourceJobId3 = "Perf_SMS_SEGG_1_DAY_AggregateJob";
		String sourceJobId4 = "Perf_VOICE_SEGG_1_1_DAY_AggregateJob";
		context.setProperty(sourceJobId1 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId2 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId3 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId4 + "_PLEVEL", "DAY");
		List<String> sourceJobIdList = new ArrayList<String>(Arrays.asList(
				sourceJobId1, sourceJobId2, sourceJobId3, sourceJobId4));
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		String jobName = "Perf_CDM2_SEGG_1_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId1);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId1))
				.thenReturn(boundaryList);
		boundary.setJobId(sourceJobId2);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId2))
				.thenReturn(boundaryList);
		boundary.setJobId(sourceJobId3);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId3))
				.thenReturn(boundaryList);
		boundary.setJobId(sourceJobId4);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId4))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario16() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		String sourceJobId1 = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		String sourceJobId2 = "Perf_DATA_UP_SEGG_1_DAY_AggregateJob";
		String sourceJobId3 = "Perf_SMS_SEGG_1_DAY_AggregateJob";
		String sourceJobId4 = "Perf_VOICE_SEGG_1_1_DAY_AggregateJob";
		context.setProperty(sourceJobId1 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId2 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId3 + "_PLEVEL", "DAY");
		context.setProperty(sourceJobId4 + "_PLEVEL", "DAY");
		List<String> sourceJobIdList = new ArrayList<String>(Arrays.asList(
				sourceJobId1, sourceJobId2, sourceJobId3, sourceJobId4));
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		String jobName = "Perf_CDM2_SEGG_1_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId1);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		boundary.setJobId(sourceJobId2);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId2))
				.thenReturn(boundaryList);
		boundary.setJobId(sourceJobId3);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId3))
				.thenReturn(boundaryList);
		boundary.setJobId(sourceJobId4);
		boundaryList.add(boundary);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId4))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		boundarySourceUB.setJobId(sourceJobId1);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId1))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario17() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		String sourceJobId = "Usage_BCSI_1_LoadJob";
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add(sourceJobId);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		String jobName = "Perf_CEI2_BCSI_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.DAY_OF_MONTH, -2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario18() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		context.setProperty("Usage_BCSI_1_LoadJob_PLEVEL", "DAY");
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Usage_BCSI_1_LoadJob";
		sourceJobIdList.add(sourceJobId);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		String jobName = "Perf_CEI2_BCSI_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 23);
		calBoundary.set(Calendar.MINUTE, 55);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.set(Calendar.HOUR_OF_DAY, 23);
		calBoundary.set(Calendar.MINUTE, 55);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario19() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Perf_DATA_CP_SEGG_1_HOUR_AggregateJob";
		String sourceJobType = "Aggregation";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "HOUR");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 23);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario20() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobId = "Perf_DATA_CP_SEGG_1_HOUR_AggregateJob";
		String sourceJobType = "Aggregation";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "HOUR");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 22);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.set(Calendar.HOUR_OF_DAY, 23);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario21() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario22() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_DAY_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "DAY");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -2);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario23() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_WEEK_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "WEEK");
		context.setProperty(JobExecutionContext.PLEVEL, "WEEK");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setId(1);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -2);
		calBoundary.set(Calendar.DAY_OF_WEEK, 2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		Calendar calBoundaryUb = Calendar.getInstance();
		calBoundaryUb.set(Calendar.HOUR_OF_DAY, 0);
		calBoundaryUb.set(Calendar.MINUTE, 0);
		calBoundaryUb.set(Calendar.SECOND, 0);
		calBoundaryUb.set(Calendar.MILLISECOND, 0);
		assertEquals(calBoundaryUb.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundaryUb.add(Calendar.DAY_OF_MONTH, 1);
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario24() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_WEEK_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "WEEK");
		context.setProperty(JobExecutionContext.PLEVEL, "WEEK");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -1);
		calBoundary.set(Calendar.DAY_OF_WEEK, 2);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		Calendar calBoundaryUb = Calendar.getInstance();
		calBoundaryUb.set(Calendar.HOUR_OF_DAY, 0);
		calBoundaryUb.set(Calendar.MINUTE, 0);
		calBoundaryUb.set(Calendar.SECOND, 0);
		calBoundaryUb.set(Calendar.MILLISECOND, 0);
		calBoundaryUb.add(Calendar.WEEK_OF_MONTH, -1);
		calBoundaryUb.add(Calendar.DAY_OF_WEEK, 1);
		assertEquals(calBoundaryUb.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario25() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_WEEK_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_WEEK_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "WEEK");
		context.setProperty(JobExecutionContext.PLEVEL, "WEEK");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "WEEK");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -1);
		calBoundary.set(Calendar.DAY_OF_WEEK, 2);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario26() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_WEEK_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_WEEK_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "WEEK");
		context.setProperty(JobExecutionContext.PLEVEL, "WEEK");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "15");
		context.setProperty(sourceJobId + "_PLEVEL", "WEEK");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.WEEK_OF_MONTH, -2);
		calBoundary.set(Calendar.DAY_OF_WEEK, 2);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 7);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario27() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_MONTH_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "MONTH");
		context.setProperty(JobExecutionContext.PLEVEL, "Month");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "200");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.MONTH, -2);
		calBoundary.set(Calendar.DAY_OF_MONTH, 1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		calBoundary = new GregorianCalendar();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario28() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_DATA_CP_SEGG_1_DAY_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_DATA_CP_SEGG_1_MONTH_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "MONTH");
		context.setProperty(JobExecutionContext.PLEVEL, "Month");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "200");
		context.setProperty(sourceJobId + "_PLEVEL", "DAY");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		int days = calBoundary.get(Calendar.DAY_OF_MONTH) + 1;
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.add(Calendar.DAY_OF_MONTH, -days);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.MONTH, -1);
		calBoundary.set(Calendar.DAY_OF_MONTH, 1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.MONTH, 1);
		calBoundary.add(Calendar.DAY_OF_MONTH, -1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario29() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_MONTH_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_MONTH_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "MONTH");
		context.setProperty(JobExecutionContext.PLEVEL, "Month");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "200");
		context.setProperty(sourceJobId + "_PLEVEL", "MONTH");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.set(Calendar.DAY_OF_MONTH, 1);
		calBoundary.add(Calendar.MONTH, -2);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(1, context.getProperty(JobExecutionContext.RETURN));
		assertEquals("No Complete Data to Export/Aggregate further",
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testScenario30() throws Exception {
		setValuesForBoundary(context, queryExec, jobDictionaryQuery,
				jobDictionary, dataAvailabilityCacheUtil);
		List<String> sourceJobIdList = new ArrayList<String>();
		String sourceJobType = "Aggregation";
		String sourceJobId = "Perf_VOLTE_SEGG_1_MONTH_AggregateJob";
		sourceJobIdList.add(sourceJobId);
		String jobName = "Perf_VOLTE_FAIL_1_MONTH_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		context.setProperty(JobExecutionContext.ALEVEL, "MONTH");
		context.setProperty(JobExecutionContext.PLEVEL, "Month");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, sourceJobType);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(sourceJobId + "_PARTITION_CHECK_REQUIRED", false);
		context.setProperty(sourceJobId + "_RETENTIONDAYS", "200");
		context.setProperty(sourceJobId + "_PLEVEL", "MONTH");
		PowerMockito.when(queryExec.executeMetadatasqlQueryMultiple(
				Mockito.anyString(), Mockito.any())).thenReturn(null);
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
		Calendar calBoundary = Calendar.getInstance();
		calBoundary.set(Calendar.HOUR_OF_DAY, 0);
		calBoundary.set(Calendar.MINUTE, 0);
		calBoundary.set(Calendar.SECOND, 0);
		calBoundary.set(Calendar.MILLISECOND, 0);
		calBoundary.set(Calendar.DAY_OF_MONTH, 1);
		calBoundary.add(Calendar.MONTH, -1);
		Timestamp timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		Boundary boundary = new Boundary();
		boundary.setJobId(sourceJobId);
		boundary.setMaxValue(timeBoundary);
		boundaryList.add(boundary);
		calBoundary.add(Calendar.MONTH, -1);
		calBoundary.set(Calendar.DAY_OF_MONTH, 1);
		timeBoundary = new Timestamp(calBoundary.getTimeInMillis());
		List<Boundary> boundarySourceList = new ArrayList<Boundary>();
		Boundary boundarySourceUB = new Boundary();
		boundarySourceUB.setJobId(jobName);
		boundarySourceUB.setId(1);
		boundarySourceUB.setMaxValue(timeBoundary);
		boundarySourceList.add(boundarySourceUB);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(sourceJobId))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId(jobName))
				.thenReturn(boundarySourceList);
		PowerMockito.when(jobDictionary.getAdaptationId()).thenReturn("SGSN");
		PowerMockito
				.when(dataAvailabilityCacheUtil.getValue(Mockito.anyString()))
				.thenReturn("1");
		assertTrue(calculateLBUB.execute(context));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.LB));
		calBoundary.add(Calendar.MONTH, 1);
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.UB));
		assertEquals(calBoundary.getTimeInMillis(),
				context.getProperty(JobExecutionContext.NEXT_LB));
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
		assertNull(context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testCalculateLBUBForSrcLoading() throws Exception {
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		Calendar calendarUB = new GregorianCalendar();
		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add("Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		srcBoundary.setMaxValue(timeStamp);
		srcBoundaryList.add(boundary);
		usBoundary.setMaxValue(timeStamp);
		usBoundaryList.add(usBoundary);
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		PowerMockito.mockStatic(BoundaryCalculatorFactory.class);
		PowerMockito
				.when(BoundaryCalculatorFactory
						.getBoundaryCalculatorInstance(context))
				.thenReturn(boundaryCalculatorMock);
		PowerMockito
				.when(boundaryQueryMock
						.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
				.thenReturn(srcBoundaryList);
		PowerMockito
				.when(boundaryCalculatorMock.getMaxValueOfJob(boundaryQueryMock,
						"Perf_VLR_1_15MIN_AggregateJob", null))
				.thenReturn(timeStamp);
		PowerMockito
				.when(boundaryQueryMock.retrieveByJobId("Usage_VLR_1_LoadJob"))
				.thenReturn(usBoundaryList);
		PowerMockito.when(boundaryCalculatorMock.calculateLB(null))
				.thenReturn(calendarMock);
		PowerMockito.when(boundaryCalculatorMock.calculateUB(null))
				.thenReturn(calendarUB);
		boolean success = calculateLBUB.execute(context);
		assertTrue(success);
	}

	@Test
	public void testCalculateLBUBForSrcAgg() throws Exception {
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		Calendar calendarUB = new GregorianCalendar();
		boundary.setMaxValue(timeStamp);
		srcBoundary.setMaxValue(timeStamp);
		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add("Perf_VLR_1_15MIN_AggregateJob");
		boundaryList.add(boundary);
		srcBoundaryList.add(srcBoundary);
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		Mockito.when(boundaryQueryMock
				.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
		Mockito.when(boundaryQueryMock
				.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
				.thenReturn(srcBoundaryList);
		Mockito.when(calendarMock.getTimeInMillis())
				.thenReturn(calendar.getTimeInMillis());
		PowerMockito.mockStatic(BoundaryCalculatorFactory.class);
		PowerMockito
				.when(BoundaryCalculatorFactory
						.getBoundaryCalculatorInstance(context))
				.thenReturn(boundaryCalculatorMock);
		PowerMockito
				.when(boundaryQueryMock
						.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
				.thenReturn(srcBoundaryList);
		PowerMockito
				.when(boundaryCalculatorMock.getMaxValueOfJob(boundaryQueryMock,
						"Perf_VLR_1_15MIN_AggregateJob", null))
				.thenReturn(timeStamp);
		PowerMockito.when(boundaryCalculatorMock.calculateLB(null))
				.thenReturn(calendarMock);
		PowerMockito.when(boundaryCalculatorMock.calculateUB(null))
				.thenReturn(calendarUB);
		PowerMockito.when(dayLightSavingUtil
				.getCntIfUBInTimeZoneDST(calendarMock, "RGN1", context))
				.thenReturn(0);
		boolean success = calculateLBUB.execute(context);
		assertTrue(success);
	}
}

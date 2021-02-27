//
//package com.project.rithomas.jobexecution.common;
//
//import static org.junit.Assert.assertTrue;
//import static org.powermock.api.mockito.PowerMockito.mock;
//import static org.powermock.api.mockito.PowerMockito.mockStatic;
//
//import java.math.BigInteger;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.sql.Timestamp;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.GregorianCalendar;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Locale;
//
//import org.hibernate.Session;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import com.project.cem.sai.jdbc.ConnectionManager;
//import com.project.rithomas.common.cache.service.dataavailability.DataAvailabilityCacheService;
//import com.project.rithomas.jobexecution.common.boundary.CalculateLBUB;
//import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
//import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
//import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
//import com.project.rithomas.jobexecution.common.util.GetDBResource;
//import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
//import com.project.rithomas.jobexecution.common.util.LBUBUtil;
//import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
//import com.project.rithomas.jobexecution.common.util.QueryConstants;
//import com.project.rithomas.jobexecution.common.util.QueryExecutor;
//import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
//import com.project.rithomas.sdk.model.meta.Boundary;
//import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
//import com.project.rithomas.sdk.workflow.WorkFlowContext;
//import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
//import com.project.sai.rithomas.scheduler.constants.schedulerConstants;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest(value = { CalculateLBUB.class, GetDBResource.class,
//		ConnectionManager.class, DayLightSavingUtil.class, TimeZoneUtil.class,
//		DataAvailabilityCacheUtil.class,
//		ModifySourceJobList.class,LBUBUtil.class})
//public class CalculateLBUBTest {
//
//	WorkFlowContext context = new JobExecutionContext();
//
//	CalculateLBUB calculateLBUB = null;
//
//	@Mock
//	BoundaryQuery boundQuery;
//
//	@Mock
//	Boundary boundaryMock;
//
//	Boundary boundary = new Boundary();
//
//	Boundary srcBoundary = new Boundary();
//
//	Boundary usBoundary = new Boundary();
//
//	List<Boundary> boundaryList = new ArrayList<Boundary>();
//
//	List<Boundary> srcBoundaryList = new ArrayList<Boundary>();
//
//	List<Boundary> usBoundaryList = new ArrayList<Boundary>();
//
//	Calendar calendar = new GregorianCalendar();
//
//	Calendar boundCalendar = new GregorianCalendar();
//
//	Calendar calendarDSTFor15MIN = new GregorianCalendar();
//
//	Calendar calendarForAPN = new GregorianCalendar();
//
//	List<String> timeZoneRgns = new ArrayList<String>();
//
//	@Mock
//	Calendar calendarMock;
//
//	@Mock
//	Connection mockConnection;
//
//	@Mock
//	Statement mockStatement;
//
//	@Mock
//	ResultSet mockResultSet;
//
//	@Mock
//	QueryExecutor queryExec;
//
//	@Mock
//	Session session;
//
//	@Mock
//	DataAvailabilityCacheService cacheServiceMock;
//
//	@Mock
//	ModifySourceJobList modifySourceJobList;
//
//	@Before
//	public void setUp() throws Exception {
//		calculateLBUB = new CalculateLBUB();
//		// setting parameters in context
//		context.setProperty(JobExecutionContext.SOURCERETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
//		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
//		context.setProperty(JobExecutionContext.SOURCEJOBID,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_VLR_1_15MIN_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		// mocking new object call
//		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
//				.thenReturn(boundQuery);
//		PowerMockito.whenNew(Calendar.class).withNoArguments()
//				.thenReturn(calendarMock);
//		// setting boundary values
//		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
//		boundary.setSourcePartColumn("REPORT_TIME");
//		boundary.setSourceJobType("Aggregation");
//		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
//		boundary.setId(1);
//		// setting source Boundary values
//		srcBoundary.setSourceJobId("Usage_VLR_1_LoadJob");
//		srcBoundary.setSourcePartColumn("REPORT_TIME");
//		srcBoundary.setSourceJobType("Loading");
//		srcBoundary.setJobId("Perf_VLR_1_15MIN_AggregateJob");
//		srcBoundary.setId(1);
//		// setting usage Boundary values
//		usBoundary.setJobId("Usage_VLR_1_LoadJob");
//		usBoundary.setId(1);
//		// Setting calendar object
//		calendar.add(Calendar.HOUR_OF_DAY, -5);
//		calendar.set(Calendar.MINUTE, 0);
//		calendar.set(Calendar.SECOND, 0);
//		calendar.set(Calendar.MILLISECOND, 0);
//		// Setting calendar object for boundary
//		boundCalendar.add(Calendar.HOUR_OF_DAY, -2);
//		boundCalendar.set(Calendar.MINUTE, 0);
//		boundCalendar.set(Calendar.SECOND, 0);
//		boundCalendar.set(Calendar.MILLISECOND, 0);
//		// Setting time zone regions
//		timeZoneRgns.add("RGN1");
//		// timeZoneRgns.add("RGN2");
//		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
//				timeZoneRgns);
//		// Setting calendar object for DST when source is PERFORMANCE(15MIN)
//		calendarDSTFor15MIN.add(Calendar.HOUR_OF_DAY, -5);
//		calendarDSTFor15MIN.set(Calendar.MINUTE, 15);
//		calendarDSTFor15MIN.set(Calendar.SECOND, 0);
//		calendarDSTFor15MIN.set(Calendar.MILLISECOND, 0);
//		// mock static class
//		mockStatic(GetDBResource.class);
//		mockStatic(ConnectionManager.class);
//		mockStatic(DataAvailabilityCacheUtil.class);
//		PowerMockito.when(
//				ConnectionManager
//						.getConnection(schedulerConstants.HIVE_DATABASE))
//				.thenReturn(mockConnection);
//		Mockito.when(mockConnection.createStatement())
//				.thenReturn(mockStatement);
//		calendarForAPN.add(Calendar.DAY_OF_MONTH, -1);
//		calendarForAPN.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForAPN.set(Calendar.MINUTE, 0);
//		calendarForAPN.set(Calendar.SECOND, 0);
//		calendarForAPN.set(Calendar.MILLISECOND, 0);
//		Object[] resultSet = { new Timestamp(calendarForAPN.getTimeInMillis()) };
//		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
//				.thenReturn(queryExec);
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQuery(QueryConstants.LB_APN_EXPORT,
//						context)).thenReturn(resultSet);
//	}
//
//	@Test
//	public void testCalculateLBUBForSrcAgg() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundary.setMaxValue(timeStamp);
//		srcBoundary.setMaxValue(timeStamp);
//		boundaryList.add(boundary);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForSrcLoading() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(boundary);
//		usBoundary.setMaxValue(timeStamp);
//		usBoundaryList.add(usBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_15MIN_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryList);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBWhenLbGreaterThanUB() throws Exception {
//		Timestamp timeStampBound = new Timestamp(new SimpleDateFormat(
//				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
//				"2014.04.04 02:00:00").getTime());
//		boundary.setMaxValue(timeStampBound);
//		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
//				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
//				"2014.04.04 02:00:00").getTime());
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		boundaryList.add(boundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForSecondRunOfAgg() throws Exception {
//		Timestamp timeStampBound = new Timestamp(
//				boundCalendar.getTimeInMillis());
//		boundary.setMaxValue(timeStampBound);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundaryList.add(boundary);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	private List<String> stubTimeZone() {
//		List<String> skipTimeZone = new ArrayList<String>();
//		skipTimeZone.add("Tz1");
//		skipTimeZone.add("Tz2");
//		skipTimeZone.add("Tz3");
//		return skipTimeZone;
//	}
//
//	@Test
//	// Test when lb/ub is null
//	public void testCalculateLBUBWhenLBUBNull()
//			throws WorkFlowExecutionException, ParseException {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundaryList.add(boundary);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
//		context.setProperty(JobExecutionContext.STATUS, "E");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		try {
//			PowerMockito.whenNew(ModifySourceJobList.class)
//					.withArguments(context).thenReturn(modifySourceJobList);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBEqual() throws Exception {
//		Timestamp timeStampBound = new Timestamp(new SimpleDateFormat(
//				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
//				"2014.04.04 01:00:00").getTime());
//		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
//				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
//				"2014.04.04 01:45:00").getTime());
//		boundary.setMaxValue(timeStampBound);
//		boundaryList.add(boundary);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForGreaterJobRetention() throws Exception {
//		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
//				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
//				"2014.04.04 02:00:00").getTime());
//		boundary.setMaxValue(null);
//		boundaryList.add(boundary);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "10");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "15");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
//				.thenReturn(boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_VLR_1_15MIN_AggregateJob"))
//				.thenReturn(srcBoundaryList);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//		// boolean success = calculateLBUB.execute(context);
//		// assertTrue(success);
//	}
//
//	// Testing for TIME_ZONE_SUPPORT
//	@Test
//	public void testCalculateLBUBForSrcAggWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundary.setMaxValue(timeStamp);
//		boundary.setRegionId("RGN1");
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundary.setRegionId("RGN1");
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTFor15MIN.getTime());
//		String region = "RGN1";
//		boundaryList.add(boundary);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Ignore
//	@Test
//	public void testCalculateLBUBForSrcLoadingWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundaryList.add(srcBoundary);
//		usBoundary.setMaxValue(timeStamp);
//		usBoundary.setRegionId("RGN1");
//		usBoundaryList.add(usBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_15MIN_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_VLR_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryList);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendar.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		boolean success = calculateLBUB.execute(context);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBWhenLbGreaterThanUBWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundary.setMaxValue(timeStamp);
//		boundary.setRegionId("RGN1");
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundaryList.add(srcBoundary);
//		boundaryList.add(boundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTFor15MIN.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForSecondRunOfAggWithTZ() throws Exception {
//		Timestamp timeStampBound = new Timestamp(
//				boundCalendar.getTimeInMillis());
//		boundary.setMaxValue(timeStampBound);
//		boundary.setRegionId("RGN1");
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundaryList.add(boundary);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTFor15MIN.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBEqualWithTZ() throws Exception {
//		Timestamp timeStampBound = new Timestamp(calendar.getTimeInMillis());
//		Timestamp timeStamp = new Timestamp(
//				calendar.getTimeInMillis() + 2700000);
//		boundary.setMaxValue(timeStampBound);
//		boundary.setRegionId("RGN1");
//		boundaryList.add(boundary);
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -4);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForGreaterJobRetentionWithTZ()
//			throws Exception {
//		Calendar srcBoundaryCalendar = new GregorianCalendar();
//		srcBoundaryCalendar.add(Calendar.DAY_OF_MONTH, -6);
//		srcBoundaryCalendar.add(Calendar.HOUR_OF_DAY, 0);
//		srcBoundaryCalendar.set(Calendar.MINUTE, 00);
//		srcBoundaryCalendar.set(Calendar.SECOND, 0);
//		srcBoundaryCalendar.set(Calendar.MILLISECOND, 0);
//		Timestamp timeStamp = new Timestamp(
//				srcBoundaryCalendar.getTimeInMillis());
//		boundary.setRegionId("RGN1");
//		boundary.setMaxValue(null);
//		boundaryList.add(boundary);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "15");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Calendar calendarDSTForGreaterJobRetention = new GregorianCalendar();
//		calendarDSTForGreaterJobRetention.add(Calendar.DAY_OF_MONTH, -6);
//		calendarDSTForGreaterJobRetention.add(Calendar.HOUR_OF_DAY, 0);
//		calendarDSTForGreaterJobRetention.set(Calendar.MINUTE, 15);
//		calendarDSTForGreaterJobRetention.set(Calendar.SECOND, 0);
//		calendarDSTForGreaterJobRetention.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForGreaterJobRetention.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBWhenLBUBNullWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundary.setRegionId("RGN1");
//		boundaryList.add(boundary);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundaryList.add(srcBoundary);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
//		context.setProperty(JobExecutionContext.STATUS, "E");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBWhenLbUbNullForAnyOneRgnWithTZ()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		boundary.setRegionId("RGN1");
//		boundaryList.add(boundary);
//		srcBoundary.setRegionId("RGN1");
//		srcBoundary.setMaxValue(timeStamp);
//		srcBoundaryList.add(srcBoundary);
//		// setting boundary values
//		Boundary boundaryWithRgn2 = new Boundary();
//		boundaryWithRgn2.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
//		boundaryWithRgn2.setSourcePartColumn("REPORT_TIME");
//		boundaryWithRgn2.setSourceJobType("Aggregation");
//		boundaryWithRgn2.setJobId("Perf_VLR_1_HOUR_AggregateJob");
//		boundaryWithRgn2.setId(1);
//		boundaryWithRgn2.setRegionId("RGN2");
//		List<Boundary> boundaryListWithRgn2 = new ArrayList<Boundary>();
//		boundaryListWithRgn2.add(boundaryWithRgn2);
//		// setting source Boundary values
//		Boundary srcBoundaryWithRgn2 = new Boundary();
//		srcBoundaryWithRgn2.setSourceJobId("Usage_VLR_1_LoadJob");
//		srcBoundaryWithRgn2.setSourcePartColumn("REPORT_TIME");
//		srcBoundaryWithRgn2.setSourceJobType("Loading");
//		srcBoundaryWithRgn2.setJobId("Perf_VLR_1_15MIN_AggregateJob");
//		srcBoundaryWithRgn2.setId(1);
//		srcBoundaryWithRgn2.setRegionId("RGN2");
//		List<Boundary> srcBoundaryListWithRgn2 = new ArrayList<Boundary>();
//		srcBoundaryListWithRgn2.add(srcBoundaryWithRgn2);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.MAXVALUE, timeStamp);
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_RETENTIONDAYS", "3");
//		context.setProperty("Perf_VLR_1_15MIN_AggregateJob_PLEVEL", "15MIN");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
//		context.setProperty(JobExecutionContext.STATUS, "E");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_VLR_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		timeZoneRgns.add("RGN2");
//		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
//				timeZoneRgns);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				boundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN1")).thenReturn(
//				srcBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_HOUR_AggregateJob", "RGN2")).thenReturn(
//				boundaryListWithRgn2);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_VLR_1_15MIN_AggregateJob", "RGN2")).thenReturn(
//				srcBoundaryListWithRgn2);
//		Mockito.when(calendarMock.getTimeInMillis()).thenReturn(
//				calendar.getTimeInMillis());
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTFor15MIN.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	private HashSet<String> stubBoundrayJobSet(List<Boundary> srcBoundaryList) {
//		HashSet<String> boundaryJobSet = new HashSet<String>();
//		for (Boundary boundary : srcBoundaryList) {
//			boundaryJobSet.add(boundary.getSourceJobId());
//		}
//		return boundaryJobSet;
//	}
//
//	// Test cases for Customized adapters
//	@Test
//	public void testCalculateLBUBForSGSN() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_SGSN_1_LoadJob");
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_SGSN_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForSGSN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForLTE.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_SGSN_FAIL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_SGSN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_SGSN_FAIL_1_HOUR_AggregateJob");
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "Yes" };
//		// Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString())).thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQuery(
//						QueryConstants.LTE_4G_AVAILABLE_DATA, context))
//				.thenReturn(resultSet);
//		List<Object> resultSetSGSN = new ArrayList<Object>();
//		resultSetSGSN.add((Object) "2G");
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQueryMultiple(
//								"select type from es_sgsn_available_data_1 where UPPER(available)='YES' and type in ('2G','3G','Gb','IuPs') and source ='SGSN'",
//								context)).thenReturn(resultSetSGSN);
//		// Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString())).thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		ResultSet mockResultSetForStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForStageType);
//		Mockito.when(mockResultSetForStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForStageType.getString(2)).thenReturn("2G");
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_SGSN_FAIL_1_HOUR_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_LTE_4G_1_LoadJob"))
//				.thenReturn(usBoundaryListForLTE);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SGSN_1_LoadJob"))
//				.thenReturn(usBoundaryListForSGSN);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForSGSNGN() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_SGSN_1_LoadJob");
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_GN_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_SGSN_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForSGSN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForLTE.add(usBoundary2);
//		Boundary usBoundary3 = new Boundary();
//		List<Boundary> usBoundaryListForGN = new ArrayList<Boundary>();
//		usBoundary3.setJobId("Usage_GN_1_LoadJob");
//		usBoundary3.setId(1);
//		usBoundary3.setMaxValue(timeStamp);
//		usBoundaryListForGN.add(usBoundary3);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_SGSN_GN_CELL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_SGSN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_SGSN_GN_CELL_1_HOUR_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		List<Object> resultSetSGSN = new ArrayList<Object>();
//		resultSetSGSN.add((Object) "GN");
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQueryMultiple(
//								"select source from es_sgsn_available_data_1 where source in ('GN','FNG') and type='2G_3G' and upper(available) = 'YES'",
//								context)).thenReturn(resultSetSGSN);
//		ResultSet mockResultSetForGNStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForGNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForGNStageType);
//		Mockito.when(mockResultSetForGNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNStageType.getString(2)).thenReturn("GN");
//		List<Object> resultSetSGSNData = new ArrayList<Object>();
//		resultSetSGSNData.add((Object) "2G");
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQueryMultiple(
//								"select type from es_sgsn_available_data_1 where UPPER(available)='YES' and type in ('2G','3G','Gb','IuPs') and source='SGSN'",
//								context)).thenReturn(resultSetSGSN);
//		ResultSet mockResultSetForSGSNStageType = mock(ResultSet.class);
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForSGSNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForSGSNStageType);
//		Mockito.when(mockResultSetForSGSNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForSGSNStageType.getString(2)).thenReturn(
//				"2G");
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_SGSN_GN_CELL_1_HOUR_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_LTE_4G_1_LoadJob"))
//				.thenReturn(usBoundaryListForLTE);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SGSN_1_LoadJob"))
//				.thenReturn(usBoundaryListForSGSN);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_GN_1_LoadJob"))
//				.thenReturn(usBoundaryListForGN);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForHTTP() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_HTTP_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForHTTP4G = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForHTTP4G.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForHTTP = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_HTTP_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForHTTP.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_HTTP_FAIL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_HTTP_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_HTTP_FAIL_1_HOUR_AggregateJob");
//		List<Object> resultSet = new ArrayList<Object>();
//		resultSet.add((Object) "2G_3G");
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQueryMultiple(
//						QueryConstants.HTTP_AVAILABLE_DATA, context))
//				.thenReturn(resultSet);
//		List<Object> resultSet4g = new ArrayList<Object>();
//		resultSet4g.add((Object) "4G");
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQueryMultiple(
//						QueryConstants.HTTP_4G_AVAILABLE_DATA, context))
//				.thenReturn(resultSet);
//		ResultSet mockResultSetForHTTP = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1  From US_HTTP_4G_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' limit 1"))
//				.thenReturn(mockResultSetForHTTP);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1  From US_HTTP_4G_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' limit 1"))
//				.thenReturn(mockResultSetForHTTP);
//		Mockito.when(mockResultSetForHTTP.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForHTTP.getString(2)).thenReturn("1");
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_HTTP_FAIL_1_HOUR_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_LTE_4G_1_LoadJob"))
//				.thenReturn(usBoundaryListForHTTP4G);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_HTTP_1_LoadJob"))
//				.thenReturn(usBoundaryListForHTTP);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForGN() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_GN_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForLTE.add(usBoundary2);
//		Boundary usBoundary3 = new Boundary();
//		List<Boundary> usBoundaryListForGN = new ArrayList<Boundary>();
//		usBoundary3.setJobId("Usage_GN_1_LoadJob");
//		usBoundary3.setId(1);
//		usBoundary3.setMaxValue(timeStamp);
//		usBoundaryListForGN.add(usBoundary3);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_GN_DG_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_GN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_GN_DG_1_HOUR_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "Yes" };
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQuery(
//						QueryConstants.LTE_4G_AVAILABLE_DATA, context))
//				.thenReturn(resultSet);
//		List<Object> resultSetGN = new ArrayList<Object>();
//		resultSetGN.add((Object) "GN");
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQueryMultiple(
//								"select source from es_sgsn_available_data_1 where source in ('GN','FNG') and type='2G_3G' and upper(available) = 'YES'",
//								context)).thenReturn(resultSetGN);
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		ResultSet mockResultSetForGNStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForGNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' group by stage_type")).thenReturn(
//				mockResultSetForGNStageType);
//		Mockito.when(mockResultSetForGNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNStageType.getString(2)).thenReturn("GN");
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_GN_DG_1_HOUR_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_LTE_4G_1_LoadJob"))
//				.thenReturn(usBoundaryListForLTE);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_GN_1_LoadJob"))
//				.thenReturn(usBoundaryListForGN);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	// to do
//	@Test
//	public void testCalculateLBUBForAPNWhenLBNull()
//			throws WorkFlowExecutionException, ParseException, SQLException {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		sourceJobIdList.add("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForAPN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForAPN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForSGSN.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		currentBoundary.setId(1);
//		// currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(
//				"Perf_APNHLR_AC_1_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_APNHLR_AC_1_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(
//				"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForAPN);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForSGSN);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForAPNWhenLBNotNull()
//			throws WorkFlowExecutionException, ParseException, SQLException {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendarForAPN.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		sourceJobIdList.add("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForAPN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForAPN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForSGSN.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(
//				"Perf_APNHLR_AC_1_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_APNHLR_AC_1_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(
//				"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForAPN);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForSGSN);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForCEITTDay() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendarForAPN.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_CEI_TT_1_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForCEITTHour = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_CEI_TT_1_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForCEITTHour.add(usBoundary1);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_CEI_TT_1_1_DAY_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_CEI_TT_1_1_HOUR_AggregateJob_RETENTIONDAYS",
//				"1");
//		context.setProperty("Perf_CEI_TT_1_1_HOUR_AggregateJob_PLEVEL", "HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_CEI_TT_1_1_DAY_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_CEI_TT_1_1_DAY_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobId("Perf_CEI_TT_1_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForCEITTHour);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForCEIOVERALLINDEX() throws Exception {
//		Timestamp timeStamp = new Timestamp(boundCalendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_CEI_INDEX_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForCEIINDEXHOUR = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_CEI_INDEX_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForCEIINDEXHOUR.add(usBoundary1);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_CEI_O_INDEX_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_CEI_INDEX_1_HOUR_AggregateJob_RETENTIONDAYS",
//				"1");
//		context.setProperty("Perf_CEI_INDEX_1_HOUR_AggregateJob_PLEVEL", "HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_CEI_O_INDEX_1_HOUR_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_CEI_O_INDEX_1_HOUR_AggregateJob"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery
//						.retrieveByJobId("Perf_CEI_INDEX_1_HOUR_AggregateJob"))
//				.thenReturn(usBoundaryListForCEIINDEXHOUR);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	// Test cases for Customized adapters with TIMEZONE
//	@Test
//	public void testCalculateLBUBForSGSNWithTZ() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -6);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_SGSN_1_LoadJob");
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_SGSN_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForSGSN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundary2.setRegionId("RGN1");
//		usBoundaryListForLTE.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_SGSN_FAIL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_SGSN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_SGSN_FAIL_1_HOUR_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "Yes" };
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		PowerMockito.when(
//				queryExec.executeMetadatasqlQuery(
//						QueryConstants.LTE_4G_AVAILABLE_DATA, context))
//				.thenReturn(resultSet);
//		List<Object> resultSetGN = new ArrayList<Object>();
//		resultSetGN.add((Object) "2G");
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQueryMultiple(
//								"select type from es_sgsn_available_data_1 where UPPER(available)='YES' and type in ('2G','3G','Gb','IuPs') and source ='SGSN'",
//								context)).thenReturn(resultSetGN);
//		ResultSet mockResultSetForStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForStageType);
//		Mockito.when(mockResultSetForStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForStageType.getString(2)).thenReturn("2G");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_SGSN_FAIL_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_LTE_4G_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForLTE);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_SGSN_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForSGSN);
//		ResultSet mockResultSet1 = mock(ResultSet.class);
//		Mockito.when(mockResultSet1.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select count(*) from es_rgn_tz_info_1 where tmz_region='RGN1' and ((trunc(start_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD')) OR (trunc(end_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD'))) and uf_trunc(start_time,'HH24') <= '"
//								+ ub_date
//								+ "' and  str_to_date(end_time,'yyyy-MM-dd HH:mm:ss') > '"
//								+ ub_date + "'")).thenReturn(mockResultSet1);
//		Mockito.when(mockResultSet1.getInt(1)).thenReturn(0);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForSGSNGNWithTZ() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_SGSN_1_LoadJob");
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_GN_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_SGSN_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForSGSN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundaryListForLTE.add(usBoundary2);
//		usBoundary2.setRegionId("RGN1");
//		Boundary usBoundary3 = new Boundary();
//		List<Boundary> usBoundaryListForGN = new ArrayList<Boundary>();
//		usBoundary3.setJobId("Usage_GN_1_LoadJob");
//		usBoundary3.setId(1);
//		usBoundary3.setMaxValue(timeStamp);
//		usBoundary3.setRegionId("RGN1");
//		usBoundaryListForGN.add(usBoundary3);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_SGSN_GN_CELL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_SGSN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_SGSN_GN_CELL_1_HOUR_AggregateJob");
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery(QueryConstants.LTE_4G_AVAILABLE_DATA))
//				.thenReturn(mockResultSet);
//		Mockito.when(mockResultSet.getString(1)).thenReturn("Yes");
//		ResultSet mockResultSetForGNData = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select source from es_sgsn_available_data_1 where source in ('GN','FNG') and type='2G_3G' and upper(available) = 'YES'"))
//				.thenReturn(mockResultSetForGNData);
//		Mockito.when(mockResultSetForGNData.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNData.getString(1)).thenReturn("GN");
//		ResultSet mockResultSetForGNStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForGNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForGNStageType);
//		Mockito.when(mockResultSetForGNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNStageType.getString(2)).thenReturn("GN");
//		ResultSet mockResultSetForSGSNData = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select type from es_sgsn_available_data_1 where UPPER(available)='YES' and type in ('2G','3G','Gb','IuPs') and source='SGSN'"))
//				.thenReturn(mockResultSetForSGSNData);
//		Mockito.when(mockResultSetForSGSNData.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForSGSNData.getString(1)).thenReturn("2G");
//		ResultSet mockResultSetForSGSNStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForSGSNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type from US_SGSN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForSGSNStageType);
//		Mockito.when(mockResultSetForSGSNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForSGSNStageType.getString(2)).thenReturn(
//				"2G");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_SGSN_GN_CELL_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_LTE_4G_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForLTE);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_SGSN_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForSGSN);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_GN_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForGN);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -6);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select count(*) from es_rgn_tz_info_1 where tmz_region='RGN1' and ((trunc(start_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD')) OR (trunc(end_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD'))) and uf_trunc(start_time,'HH24') <= '"
//								+ ub_date
//								+ "' and  str_to_date(end_time,'yyyy-MM-dd HH:mm:ss') > '"
//								+ ub_date + "'")).thenReturn(mockResultSet);
//		Mockito.when(mockResultSet.getInt(1)).thenReturn(0);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForHTTPWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_HTTP_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForHTTP4G = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForHTTP4G.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForHTTP = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_HTTP_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundary2.setRegionId("RGN1");
//		usBoundaryListForHTTP.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_HTTP_FAIL_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_HTTP_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_HTTP_FAIL_1_HOUR_AggregateJob");
//		ResultSet mockResultSetForHTTPData = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement.executeQuery(QueryConstants.HTTP_AVAILABLE_DATA))
//				.thenReturn(mockResultSetForHTTPData);
//		Mockito.when(mockResultSetForHTTPData.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForHTTPData.getString(1)).thenReturn("2G_3G");
//		ResultSet mockResultSetForHTTP4GData = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery(QueryConstants.HTTP_4G_AVAILABLE_DATA))
//				.thenReturn(mockResultSetForHTTP4GData);
//		Mockito.when(mockResultSetForHTTP4GData.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForHTTP4GData.getString(1)).thenReturn("4G");
//		ResultSet mockResultSetForHTTP = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1  From US_HTTP_4G_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' and tz='RGN1' limit 1")).thenReturn(
//				mockResultSetForHTTP);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1  From US_HTTP_4G_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' and tz='RGN1' limit 1")).thenReturn(
//				mockResultSetForHTTP);
//		Mockito.when(mockResultSetForHTTP.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForHTTP.getString(2)).thenReturn("1");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_HTTP_FAIL_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_LTE_4G_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForHTTP4G);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_HTTP_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForHTTP);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendar.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select count(*) from es_rgn_tz_info_1 where tmz_region='RGN1' and ((trunc(start_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD')) OR (trunc(end_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD'))) and uf_trunc(start_time,'HH24') <= '"
//								+ ub_date
//								+ "' and  str_to_date(end_time,'yyyy-MM-dd HH:mm:ss') > '"
//								+ ub_date + "'")).thenReturn(mockResultSet);
//		Mockito.when(mockResultSet.getInt(1)).thenReturn(0);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForGNTZ() throws Exception {
//		PowerMockito.mockStatic(CalculateLBUB.class);
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis()
//				- (3600000 + 3600000));
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_LTE_4G_1_LoadJob");
//		sourceJobIdList.add("Usage_GN_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForLTE = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Usage_LTE_4G_1_LoadJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForLTE.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForGN = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Usage_GN_1_LoadJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundary2.setRegionId("RGN1");
//		usBoundaryListForGN.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Loading");
//		currentBoundary.setJobId("Perf_GN_DG_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_LTE_4G_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty("Usage_GN_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_GN_DG_1_HOUR_AggregateJob");
//		Mockito.when(CalculateLBUB.populateStageType(Mockito.anyString(), Mockito.anyString()))
//				.thenReturn("'S1MME','S11','FNS_4G','FNG_4G'");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery(QueryConstants.LTE_4G_AVAILABLE_DATA))
//				.thenReturn(mockResultSet);
//		Mockito.when(mockResultSet.getString(1)).thenReturn("Yes");
//		ResultSet mockResultSetForGNData = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select source from es_sgsn_available_data_1 where source in ('GN','FNG') and type='2G_3G' and upper(available) = 'YES'"))
//				.thenReturn(mockResultSetForGNData);
//		Mockito.when(mockResultSetForGNData.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNData.getString(1)).thenReturn("GN");
//		ResultSet mockResultSetForGNStageType = mock(ResultSet.class);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ (calendar.getTimeInMillis() - 3600000)
//								+ "' and dt < '"
//								+ calendar.getTimeInMillis()
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForGNStageType);
//		Mockito.when(
//				mockStatement
//						.executeQuery("Select 1,stage_type  From US_GN_1 where dt >= '"
//								+ calendar.getTimeInMillis()
//								+ "' and dt < '"
//								+ (calendar.getTimeInMillis() + 3600000)
//								+ "' and tz='RGN1' group by stage_type"))
//				.thenReturn(mockResultSetForGNStageType);
//		Mockito.when(mockResultSetForGNStageType.next()).thenReturn(true)
//				.thenReturn(false);
//		Mockito.when(mockResultSetForGNStageType.getString(2)).thenReturn("GN");
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_GN_DG_1_HOUR_AggregateJob", "RGN1")).thenReturn(
//				currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_LTE_4G_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForLTE);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId("Usage_GN_1_LoadJob",
//						"RGN1")).thenReturn(usBoundaryListForGN);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -6);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Mockito.when(
//				mockStatement
//						.executeQuery("select count(*) from es_rgn_tz_info_1 where tmz_region='RGN1' and ((trunc(start_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD')) OR (trunc(end_time,'DD')= trunc('"
//								+ ub_date
//								+ "','DD'))) and uf_trunc(start_time,'HH24') <= '"
//								+ ub_date
//								+ "' and  str_to_date(end_time,'yyyy-MM-dd HH:mm:ss') > '"
//								+ ub_date + "'")).thenReturn(mockResultSet);
//		Mockito.when(mockResultSet.getInt(1)).thenReturn(0);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForAPNWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		sourceJobIdList.add("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForAPN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForAPN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundary2.setRegionId("RGN1");
//		usBoundaryListForSGSN.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setRegionId("RGN1");
//		// currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(
//				"Perf_APNHLR_AC_1_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_APNHLR_AC_1_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(
//				"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_APNHLR_PC_1_1_DAY_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_APNHLR_AC_1_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(usBoundaryListForAPN);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(usBoundaryListForSGSN);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -4);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForAPNWithTZWhenLBNotNull() throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendarForAPN.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		sourceJobIdList.add("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForAPN = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_APNHLR_AC_1_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundary1.setRegionId("RGN1");
//		usBoundaryListForAPN.add(usBoundary1);
//		Boundary usBoundary2 = new Boundary();
//		List<Boundary> usBoundaryListForSGSN = new ArrayList<Boundary>();
//		usBoundary2.setJobId("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob");
//		usBoundary2.setId(1);
//		usBoundary2.setMaxValue(timeStamp);
//		usBoundary2.setRegionId("RGN1");
//		usBoundaryListForSGSN.add(usBoundary2);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty(
//				"Perf_APNHLR_AC_1_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_APNHLR_AC_1_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(
//				"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_RETENTIONDAYS", "1");
//		context.setProperty("Perf_SGSN_GN_CITY_1_HOUR_AggregateJob_PLEVEL",
//				"HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_APNHLR_PC_1_1_DAY_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_APNHLR_PC_1_1_DAY_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_APNHLR_AC_1_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(usBoundaryListForAPN);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_SGSN_GN_CITY_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(usBoundaryListForSGSN);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -4);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForCEIOVERALLINDEXWithTZ() throws Exception {
//		Timestamp timeStamp = new Timestamp(boundCalendar.getTimeInMillis());
//		Timestamp lbTimeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Perf_CEI_INDEX_1_HOUR_AggregateJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "1");
//		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
//		Boundary usBoundary1 = new Boundary();
//		List<Boundary> usBoundaryListForCEIINDEXHOUR = new ArrayList<Boundary>();
//		usBoundary1.setJobId("Perf_CEI_INDEX_1_HOUR_AggregateJob");
//		usBoundary1.setId(1);
//		usBoundary1.setRegionId("RGN1");
//		usBoundary1.setMaxValue(timeStamp);
//		usBoundaryListForCEIINDEXHOUR.add(usBoundary1);
//		List<Boundary> currentBoundaryList = new ArrayList<Boundary>();
//		Boundary currentBoundary = new Boundary();
//		currentBoundary.setSourcePartColumn("REPORT_TIME");
//		currentBoundary.setSourceJobType("Aggregation");
//		currentBoundary.setJobId("Perf_CEI_O_INDEX_1_HOUR_AggregateJob");
//		currentBoundary.setId(1);
//		currentBoundary.setRegionId("RGN1");
//		currentBoundary.setMaxValue(lbTimeStamp);
//		currentBoundaryList.add(currentBoundary);
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
//		context.setProperty("Perf_CEI_INDEX_1_HOUR_AggregateJob_RETENTIONDAYS",
//				"1");
//		context.setProperty("Perf_CEI_INDEX_1_HOUR_AggregateJob_PLEVEL", "HOUR");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"Perf_CEI_O_INDEX_1_HOUR_AggregateJob");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_CEI_O_INDEX_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(currentBoundaryList);
//		Mockito.when(
//				boundQuery.retrieveByJobIdAndRegionId(
//						"Perf_CEI_INDEX_1_HOUR_AggregateJob", "RGN1"))
//				.thenReturn(usBoundaryListForCEIINDEXHOUR);
//		Calendar calendarDSTForHR = new GregorianCalendar();
//		calendarDSTForHR.add(Calendar.HOUR_OF_DAY, -1);
//		calendarDSTForHR.set(Calendar.MINUTE, 00);
//		calendarDSTForHR.set(Calendar.SECOND, 0);
//		calendarDSTForHR.set(Calendar.MILLISECOND, 0);
//		String ub_date = DateFunctionTransformation.getInstance()
//				.getFormattedDate(calendarDSTForHR.getTime());
//		Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(false);
//		Object[] resultSet = new String[] { "0" };
//		String region = "RGN1";
//		PowerMockito
//				.when(queryExec
//						.executeMetadatasqlQuery(
//								"select count(*) from saidata.es_rgn_tz_info_1 ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
//										+ ub_date
//										+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp'"
//										+ ub_date
//										+ "'))) and date_trunc('hour',start_time) <= timestamp '"
//										+ ub_date
//										+ "' and end_time > timestamp '"
//										+ ub_date
//										+ "'and  where tmz_region='"
//										+ region + "'", context)).thenReturn(
//						resultSet);
//		HashSet<String> boundaryJobSet = stubBoundrayJobSet(srcBoundaryList);
//		PowerMockito
//				.whenNew(ModifySourceJobList.class)
//				.withArguments(Mockito.any(), Mockito.anyString(),
//						Mockito.any()).thenReturn(modifySourceJobList);
//		PowerMockito.when(modifySourceJobList.modifySourceJobListForAgg())
//				.thenReturn(boundaryJobSet);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWhenLBNull()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForVLR = new ArrayList<Boundary>();
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		Calendar calendarForSMS = new GregorianCalendar();
//		calendarForSMS.add(Calendar.DAY_OF_MONTH, -1);
//		calendarForSMS.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSMS.add(Calendar.MINUTE, 0);
//		calendarForSMS.add(Calendar.SECOND, 0);
//		calendarForSMS.add(Calendar.MILLISECOND, 0);
//		Timestamp timestampForSMS = new Timestamp(
//				calendarForSMS.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForVLR = new Boundary();
//		usBoundaryForVLR.setMaxValue(timeStamp);
//		usBoundaryListForVLR.add(usBoundaryForVLR);
//		Boundary usBoundaryForSMS = new Boundary();
//		usBoundaryForSMS.setMaxValue(timestampForSMS);
//		usBoundaryListForSMS.add(usBoundaryForSMS);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryListForVLR);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWhenLBNotNull()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForVLR = new ArrayList<Boundary>();
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		Calendar calendarForSMS = new GregorianCalendar();
//		calendarForSMS.add(Calendar.DAY_OF_MONTH, -1);
//		calendarForSMS.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSMS.add(Calendar.MINUTE, 0);
//		calendarForSMS.add(Calendar.SECOND, 0);
//		calendarForSMS.add(Calendar.MILLISECOND, 0);
//		Timestamp timestampForSMS = new Timestamp(
//				calendarForSMS.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		Calendar calendarForSubscriberCnt = new GregorianCalendar();
//		calendarForSubscriberCnt.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSubscriberCnt.add(Calendar.MINUTE, 0);
//		calendarForSubscriberCnt.add(Calendar.SECOND, 0);
//		calendarForSubscriberCnt.add(Calendar.MILLISECOND, 0);
//		Timestamp timestampForSubscriberCnt = new Timestamp(
//				calendarForSubscriberCnt.getTimeInMillis());
//		boundary.setMaxValue(timestampForSubscriberCnt);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForVLR = new Boundary();
//		usBoundaryForVLR.setMaxValue(timeStamp);
//		usBoundaryListForVLR.add(usBoundaryForVLR);
//		Boundary usBoundaryForSMS = new Boundary();
//		usBoundaryForSMS.setMaxValue(timestampForSMS);
//		usBoundaryListForSMS.add(usBoundaryForSMS);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryListForVLR);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWhenUBNull()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForVLR = new ArrayList<Boundary>();
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		Calendar calendarForSMS = new GregorianCalendar();
//		calendarForSMS.add(Calendar.DAY_OF_MONTH, -1);
//		calendarForSMS.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSMS.add(Calendar.MINUTE, 0);
//		calendarForSMS.add(Calendar.SECOND, 0);
//		calendarForSMS.add(Calendar.MILLISECOND, 0);
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForVLR = new Boundary();
//		// usBoundaryForVLR.setMaxValue(timeStamp);
//		usBoundaryListForVLR.add(usBoundaryForVLR);
//		Boundary usBoundaryForSMS = new Boundary();
//		// usBoundaryForSMS.setMaxValue(timestampForSMS);
//		usBoundaryListForSMS.add(usBoundaryForSMS);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryListForVLR);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Ignore
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWhenUBNullForOneOfTheSource()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForVLR = new ArrayList<Boundary>();
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForVLR = new Boundary();
//		usBoundaryForVLR.setMaxValue(timeStamp);
//		usBoundaryListForVLR.add(usBoundaryForVLR);
//		Boundary usBoundaryForSMS = new Boundary();
//		usBoundaryListForSMS.add(usBoundaryForSMS);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryListForVLR);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		boolean success = calculateLBUB.execute(context);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWhenUBNullForFirstSource()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForVLR = new ArrayList<Boundary>();
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		Calendar calendarForSMS = new GregorianCalendar();
//		calendarForSMS.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSMS.add(Calendar.MINUTE, 0);
//		calendarForSMS.add(Calendar.SECOND, 0);
//		calendarForSMS.add(Calendar.MILLISECOND, 0);
//		Timestamp timestampForSMS = new Timestamp(
//				calendarForSMS.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_VLR_1_LoadJob");
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForVLR = new Boundary();
//		usBoundaryForVLR.setJobId("Usage_VLR_1_LoadJob");
//		usBoundaryForVLR.setId(1);
//		usBoundaryListForVLR.add(usBoundaryForVLR);
//		Boundary usBoundaryForSMS = new Boundary();
//		usBoundaryForSMS.setJobId("Usage_SMS_1_LoadJob");
//		usBoundaryForSMS.setId(2);
//		usBoundaryForSMS.setMaxValue(timestampForSMS);
//		usBoundaryListForSMS.add(usBoundaryForSMS);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_VLR_1_LoadJob"))
//				.thenReturn(usBoundaryListForVLR);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//
//	@Test
//	public void testCalculateLBUBForDistinctSubscriberCountWithTZSrc()
//			throws Exception {
//		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
//		List<Boundary> usBoundaryListForSMS = new ArrayList<Boundary>();
//		Calendar calendarForSMS = new GregorianCalendar();
//		calendarForSMS.add(Calendar.DAY_OF_MONTH, -1);
//		calendarForSMS.add(Calendar.HOUR_OF_DAY, 0);
//		calendarForSMS.add(Calendar.MINUTE, 0);
//		calendarForSMS.add(Calendar.SECOND, 0);
//		calendarForSMS.add(Calendar.MILLISECOND, 0);
//		Timestamp timestampForSMS = new Timestamp(
//				calendarForSMS.getTimeInMillis());
//		List<String> sourceJobIdList = new ArrayList<String>();
//		sourceJobIdList.add("Usage_SMS_1_LoadJob");
//		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
//		srcBoundaryList.add(boundary);
//		Boundary usBoundaryForSMS1 = new Boundary();
//		usBoundaryForSMS1.setId(2);
//		usBoundaryForSMS1.setJobId("Usage_SMS_1_LoadJob");
//		usBoundaryForSMS1.setMaxValue(timestampForSMS);
//		Boundary usBoundaryForSMS2 = new Boundary();
//		usBoundaryForSMS2.setId(3);
//		usBoundaryForSMS2.setJobId("Usage_SMS_1_LoadJob");
//		usBoundaryForSMS2.setMaxValue(timeStamp);
//		usBoundaryListForSMS.add(usBoundaryForSMS1);
//		usBoundaryListForSMS.add(usBoundaryForSMS2);
//		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
//		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
//		context.setProperty(JobExecutionContext.JOB_NAME,
//				"DISTINCT_SUBSCRIBER_COUNT");
//		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
//		PowerMockito.when(boundaryMock.getMaxValue()).thenReturn(timeStamp);
//		Mockito.when(boundQuery.retrieveByJobId("DISTINCT_SUBSCRIBER_COUNT"))
//				.thenReturn(srcBoundaryList);
//		Mockito.when(boundQuery.retrieveByJobId("Usage_SMS_1_LoadJob"))
//				.thenReturn(usBoundaryListForSMS);
//		List<String> skipTimeZone = stubTimeZone();
//		PowerMockito.whenNew(ModifySourceJobList.class).withArguments(context)
//				.thenReturn(modifySourceJobList);
//		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
//				.thenReturn(skipTimeZone);
//		boolean success = calculateLBUB.execute(context);
//		assertTrue(success);
//	}
//}
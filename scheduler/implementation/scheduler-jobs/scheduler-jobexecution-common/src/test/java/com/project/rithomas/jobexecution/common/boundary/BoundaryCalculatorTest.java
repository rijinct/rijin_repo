package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.hibernate.type.descriptor.java.CalendarDateTypeDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DataAvailabilityCacheUtil;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.boundary.BoundaryCalculator;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { BoundaryCalculator.class, GetDBResource.class,
		ConnectionManager.class, DayLightSavingUtil.class, TimeZoneUtil.class})
public class BoundaryCalculatorTest {
	
	WorkFlowContext context = new JobExecutionContext();

	
	@Mock
	BoundaryQuery boundaryQueryMock;

	@Mock
	Boundary boundaryMock;
	
	BoundaryQuery boundaryQuery = new BoundaryQuery();

	Boundary boundary = new Boundary();
	
	Calendar calendar = new GregorianCalendar();
	
	List<Boundary> boundaryList = new ArrayList<Boundary>();

	@Mock
	private WorkFlowContext mockworkFlowContext;
	
	@Mock
	LBUBUtil mocklbutil;
	
	@Mock
	JobPropertyRetriever jobPropertyRetrieverMock;
	
	@Mock
	RuntimePropertyRetriever runTimePropertyRetriever;
	
	@Mock
	Calendar calendarMock;
	
	@Before
	public void setUp() throws Exception {

		// mocking new object call
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQueryMock);		
		PowerMockito.whenNew(JobPropertyRetriever.class).withAnyArguments()
		.thenReturn(jobPropertyRetrieverMock);
		
		PowerMockito.whenNew(RuntimePropertyRetriever.class).withAnyArguments()
		.thenReturn(runTimePropertyRetriever);
		
		PowerMockito.whenNew(Calendar.class).withNoArguments()
		.thenReturn(calendarMock);
		
		PowerMockito.whenNew(WorkFlowContext.class).withNoArguments()
		.thenReturn(mockworkFlowContext);
		
		calendarMock.add(Calendar.HOUR_OF_DAY, -5);
		calendarMock.set(Calendar.MINUTE, 0);
		calendarMock.set(Calendar.SECOND, 0);
		calendarMock.set(Calendar.MILLISECOND, 0);
		

	}	
	@Test
	public void testGetMaxValueOfJobWhenRegionNotNull() {
		//Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		BoundaryCalculator boundaryCalculator = new MultiSourceWithCache(mockworkFlowContext);
		Timestamp timeStamp = new Timestamp(calendar.getTimeInMillis());
		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setSourceJobType("Aggregation");
		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
		boundary.setId(1);
		boundary.setMaxValue(timeStamp);
		boundary.setRegionId("RGN1");
		boundaryList.add(boundary);		
		
		Mockito.when(
				boundaryQueryMock.retrieveByJobIdAndRegionId(
						"Perf_VLR_1_HOUR_AggregateJob", "RGN1")).thenReturn(
				boundaryList); 
		Timestamp maxValue = boundaryCalculator.getMaxValueOfJob(boundaryQueryMock,"Perf_VLR_1_HOUR_AggregateJob","RGN1"); 
		assertEquals(boundary.getMaxValue(), maxValue); 
	}
	
	@Test
	public void testgetSourceBoundaryValueWhenBoundaryNotNull() throws WorkFlowExecutionException, Exception {
		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
				"2019.03.12 00:00:00").getTime());
		Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2019-03-12 00:00:00");
		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setSourceJobType("Aggregation");
		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
		boundary.setId(1);
		boundary.setMaxValue(timeStamp);
		boundary.setRegionId("RGN1");
		boundaryList.add(boundary);	
		
		Calendar calendar = new GregorianCalendar();
		Mockito.when(mockworkFlowContext.getProperty(JobExecutionContext.SOURCE_JOB_NAME)).thenReturn("Perf_VLR_1_HOUR_AggregateJob");
		BoundaryCalculator boundaryCalculator = new MultiSourceWithCache(mockworkFlowContext);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob")).thenReturn(boundaryList);
		calendar = boundaryCalculator.getSourceBoundaryValue("RGN1");
		assertEquals(dateVal, calendar.getTime());
	}
	
	@Test
	public void testgetSourceBoundaryValueWhenBoundaryIsNull() throws WorkFlowExecutionException {
		Calendar calendar = new GregorianCalendar();
		Mockito.when(mockworkFlowContext.getProperty(JobExecutionContext.SOURCE_JOB_NAME)).thenReturn("Perf_VLR_1_HOUR_AggregateJob");
		BoundaryCalculator boundaryCalculator = new MultiSourceWithCache(mockworkFlowContext);
		PowerMockito.when(boundaryQueryMock.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob")).thenReturn(boundaryList);
		calendar = boundaryCalculator.getSourceBoundaryValue("RGN1");
		assertEquals(null, calendar);
	}
	
	@Test
	public void testGetFinalUBOfSrcs() throws Exception {
		String stageTypes[] = new String[] {"FNG_4G","FNS_4G","S11","S1MME"};
		List<String> listOfAllAvailableStageTypes = Arrays.asList(stageTypes);
		Map<String, Long> leastReportTimeMap = new HashMap<String, Long>();
		Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2018-12-03 12:00:00");
		Date datenextVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
				Locale.ENGLISH).parse("2018-12-03 11:00:00");
		Long ubVal = DateFunctionTransformation.getInstance().getTrunc(
				dateVal.getTime(), "HOUR");
		Long nextUbVal =  DateFunctionTransformation.getInstance().getTrunc(
				datenextVal.getTime(), "HOUR");
		
		leastReportTimeMap.put("FNG_4G",ubVal);
		leastReportTimeMap.put("FNS_4G",nextUbVal);
		leastReportTimeMap.put("S11",null);
		leastReportTimeMap.put("S1MME",null);
		
		Calendar calendar = new GregorianCalendar();
		BoundaryCalculator boundaryCalculator = new MultiSourceWithCache(mockworkFlowContext);
		PowerMockito.when(mockworkFlowContext.getProperty(JobExecutionContext.WEEK_START_DAY)).
		thenReturn("0");
		PowerMockito.when(mockworkFlowContext.getProperty(JobExecutionContext.PLEVEL)).
		thenReturn("HOUR");
		PowerMockito.when(jobPropertyRetrieverMock.getPartitionLevel()).
		thenReturn("HOUR");
		PowerMockito.when(mockworkFlowContext.getProperty("LB_RGN1")).thenReturn(ubVal );
		calendar = boundaryCalculator.getFinalUBOfSrcs("RGN1", listOfAllAvailableStageTypes,leastReportTimeMap); 
		assertEquals(dateVal, calendar.getTime());
		
	}
}

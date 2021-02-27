
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { LBUBUtil.class, TimeZoneUtil.class,
		BoundaryQuery.class, Boundary.class })
public class LBUBUtilTest {

	WorkFlowContext context = new JobExecutionContext();

	LBUBUtil util = new LBUBUtil();

	@Mock
	BoundaryQuery boundaryQuery;

	@Mock
	Boundary boundary;

	public static void setValue(WorkFlowContext context) {
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "YES");
		context.setProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC, "no");
		mockStatic(TimeZoneUtil.class);
	}

	@Test
	public void testChecks() throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 2);
		context.setProperty("LB_Region", Long.parseLong("1581276900000"));
		context.setProperty("UB_Region", Long.parseLong("1581276900000"));
		context.setProperty("NEXT_LB_Region", Long.parseLong("1581276700000"));
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "No Complete Data to Export/Aggregate further for region Region";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testChecksWithDiffBoundInTimeZone()
			throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 2);
		context.setProperty("LB_Region", Long.parseLong("1581276900000"));
		context.setProperty("UB_Region", Long.parseLong("1581276900000"));
		context.setProperty("NEXT_LB_Region", Long.parseLong("1581276700000"));
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "No Complete Data to Export/Aggregate further for region Region";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testChecksWithNOBoundInTimeZone()
			throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 2);
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "Either of the boundary value is null.Hence not aggregating/exporting further for region Region";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testChecksWithNORegionCount()
			throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 0);
		context.setProperty("LB_Region", Long.parseLong("1592276900000"));
		context.setProperty("UB_Region", Long.parseLong("1591276700000"));
		context.setProperty("NEXT_LB_Region", Long.parseLong("1581276700000"));
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		LBUBUtil.lbubChecks(context, "Region");
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
	}

	@Test
	public void testChecksWithNOLbub() throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.LBUB_SECOND_TIME_EXEC, "YES");
		context.setProperty(JobExecutionContext.REGION_COUNT_TO_AGG, 2);
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		LBUBUtil.lbubChecks(context, "Region");
		assertEquals(0, context.getProperty(JobExecutionContext.RETURN));
	}

	@Test
	public void testCheckswithNoBoundValues()
			throws WorkFlowExecutionException {
		setValue(context);
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context)).thenReturn(true);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "Either of the boundary value is null.Hence not aggregating/exporting further";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testChecksWithSameBoundValues()
			throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.LB,
				Long.parseLong("1581276900000"));
		context.setProperty(JobExecutionContext.UB,
				Long.parseLong("1581276900000"));
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context)).thenReturn(true);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "No Complete Data to Export/Aggregate further";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	@Test
	public void testChecksWithDiffBoundValues()
			throws WorkFlowExecutionException {
		setValue(context);
		context.setProperty(JobExecutionContext.LB,
				Long.parseLong("1581276900000"));
		context.setProperty(JobExecutionContext.UB,
				Long.parseLong("1581276900000"));
		context.setProperty(JobExecutionContext.NEXT_LB,
				Long.parseLong("1591276900000"));
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context)).thenReturn(true);
		LBUBUtil.lbubChecks(context, "Region");
		String description = "No Complete Data to Export/Aggregate further";
		assertEquals(description,
				context.getProperty(JobExecutionContext.DESCRIPTION));
	}

	public static List<Boundary> setValuesForBoundary(WorkFlowContext context,
			BoundaryQuery boundaryQuery) throws Exception {
		Timestamp stamp = new Timestamp(Long.parseLong("202006221010105"));
		context.setProperty(JobExecutionContext.MAXVALUE, stamp);
		Boundary boundary = new Boundary();
		boundary.setRegionId("Region1");
		boundary.setMaxValue(stamp);
		List<Boundary> boundaryList = new ArrayList<Boundary>();
		boundaryList.add(boundary);
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundaryQuery);
		PowerMockito.when(boundaryQuery.retrieveByJobId(Mockito.any()))
				.thenReturn(boundaryList);
		PowerMockito.when(boundaryQuery.retrieveByJobIdLike(Mockito.any()))
				.thenReturn(boundaryList);
		return boundaryList;
	}

	@Test
	public void testGetSourceBoundaryList() throws Exception {
		List<Boundary> boundaryList = setValuesForBoundary(context,
				boundaryQuery);
		List<String> jobList = new ArrayList<String>();
		jobList.add("_1_KPIJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, jobList);
		context.setProperty(JobExecutionContext.JOB_NAME, "UTILjob");
		List<Boundary> expected = new ArrayList<Boundary>();
		expected = util.getSourceBoundaryList(context);
		assertEquals(boundaryList, expected);
	}

	@Test
	public void testGetSourceBoundaryListProfile() throws Exception {
		List<Boundary> boundaryList = setValuesForBoundary(context,
				boundaryQuery);
		List<String> jobList = new ArrayList<String>();
		jobList.add("_1_ProfileJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, jobList);
		context.setProperty(JobExecutionContext.JOB_NAME, "UTILjob");
		List<Boundary> expected = new ArrayList<Boundary>();
		expected = util.getSourceBoundaryList(context);
		assertEquals(boundaryList, expected);
	}

	@Test
	public void testGetSourceBoundaryNoProfile() throws Exception {
		List<Boundary> boundaryList = setValuesForBoundary(context,
				boundaryQuery);
		List<String> jobList = new ArrayList<String>();
		jobList.add("randomJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, jobList);
		context.setProperty(JobExecutionContext.JOB_NAME, "UTILjob");
		List<Boundary> expected = new ArrayList<Boundary>();
		expected = util.getSourceBoundaryList(context);
		assertEquals(boundaryList, expected);
	}

	@Test
	public void testSpecIDVerFromJobName() {
		List<String> listJob = new ArrayList<String>();
		listJob.add("SMS_SEGG");
		listJob.add("1");
		List<String> expected = new ArrayList<String>();
		expected = LBUBUtil.getSpecIDVerFromJobName(
				"Perf_SMS_SEGG_1_HOUR_AggregationJob", "Aggregation");
		assertEquals(listJob, expected);
	}

	@Test
	public void testSpecIDVerFromUsageJob() {
		List<String> listJob = new ArrayList<String>();
		listJob.add("VOICE");
		listJob.add("1");
		List<String> expected = new ArrayList<String>();
		expected = LBUBUtil.getSpecIDVerFromJobName(
				"Usage_VOICE_1_LoadingJob", "Loading");
		assertEquals(listJob, expected);
	}

	@Test
	public void testCalenderLBWithNullValue() {
		Calendar calendar = new GregorianCalendar();
		Calendar calendarLB = new GregorianCalendar();
		String maxvalue = "202006221010105";
		assertNotNull(LBUBUtil.getCalendarLb(context, maxvalue, calendar,
				calendarLB));
	}

	@Test
	public void testCalenderLB() {
		Timestamp stamp = new Timestamp(Long.parseLong("202006221010105"));
		Calendar calendar = new GregorianCalendar();
		Calendar calendarLB = new GregorianCalendar();
		String maxvalue = "202006221010105";
		context.setProperty(maxvalue, stamp);
		assertNotNull(LBUBUtil.getCalendarLb(context, maxvalue, calendar,
				calendarLB));
	}

	@Test
	public void testSetCalendarLBForAPN() throws WorkFlowExecutionException {
		Timestamp stamp = new Timestamp(Long.parseLong("202006221010105"));
		String maxvalue = "202006221010105";
		Calendar calendar = new GregorianCalendar();
		Calendar calendarLB = new GregorianCalendar();
		context.setProperty(maxvalue, stamp);
		LBUBUtil.setCalendarLBForAPN(context, maxvalue, calendar, calendarLB);
		assertNotNull(context.getProperty(maxvalue));
	}
}

package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { CalculateLBUB.class,BoundaryCalculator.class,JobPropertyRetriever.class,
		BoundaryCalculatorFactory.class,BoundaryQuery.class, TimeZoneUtil.class, DayLightSavingUtil.class})
public class JobDetailsTest {
	
	//WorkFlowContext context = new JobExecutionContext();


	WorkFlowContext context = new JobExecutionContext();
	
	

	
	@Before
	public void setUp() throws Exception {

		context.setProperty(JobExecutionContext.SOURCERETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCE_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS, "10");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "10");
		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add("Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty("Usage_VLR_1_LoadJob_RETENTIONDAYS", "3");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_15MIN_AggregateJob");
	}	

@Test
public void testgetSourceJobType () {

    
	JobDetails jobDetails = new JobDetails(context);
	String getSourceJobType = jobDetails.getSourceJobType();
	assertEquals("Loading", getSourceJobType);
}

@Test
public void testgetSourceJobName() {

    
	JobDetails jobDetails = new JobDetails(context);
	String getSourceJobName = jobDetails.getSourceJobName();
	assertEquals("Perf_VLR_1_HOUR_AggregateJob", getSourceJobName);
}

@Test
public void testGetUpperBound() throws Exception {

	JobDetails jobDetails = new JobDetails(context);
	Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
			Locale.ENGLISH).parse("2019-03-12 00:00:00");
	Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
			dateVal.getTime(), "HOUR");
	jobDetails.setUpperBound("RGN1",lbVal);
	Long lbValActual = jobDetails.getUpperBound("RGN1");
	assertEquals(lbVal, lbValActual);
}

@Test
public void testGetLBINIT() throws Exception {

	JobDetails jobDetails = new JobDetails(context);
	Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
			Locale.ENGLISH).parse("2019-03-12 00:00:00");
	Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
			dateVal.getTime(), "HOUR");
	jobDetails.setLBINIT("RGN1", lbVal);
	Long lbValActual = jobDetails.getLBINIT("RGN1");
	assertEquals(lbVal, lbValActual);
}

@Test
public void testGetMaxValue() throws Exception {
	
	Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
			"2019.03.12 00:00:00").getTime());
	JobDetails jobDetails = new JobDetails(context);
	jobDetails.setMaxValue("RGN1", timeStamp);
	Timestamp maxValue = jobDetails.getMaxValue("RGN1");
	assertEquals(timeStamp, maxValue);
}

@Test
public void testGetUbDstCount() throws Exception {
	
	JobDetails jobDetails = new JobDetails(context);
	jobDetails.setUbDstCount("RGN1", 5);
	long getUbDstCount = jobDetails.getUbDstCount("RGN1");
	
	assertEquals(5, getUbDstCount);
}

@Test
public void testGetNextLB() throws Exception  {
	
	JobDetails jobDetails = new JobDetails(context);
	Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
			Locale.ENGLISH).parse("2019-03-12 00:00:00");
	Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
			dateVal.getTime(), "HOUR");
	jobDetails.setNextLB("RGN1", lbVal);
	Long nextLB = jobDetails.getNextLB("RGN1");
	assertEquals(lbVal, nextLB);
}

@Test
public void testGetNextLBWithoutRegion() throws Exception  {
	
	JobDetails jobDetails = new JobDetails(context);
	Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
			Locale.ENGLISH).parse("2019-03-12 00:00:00");
	Long lbVal = DateFunctionTransformation.getInstance().getTrunc(
			dateVal.getTime(), "HOUR");
	jobDetails.setNextLB(null, lbVal);
	Long nextLB = jobDetails.getNextLB(null);
	assertEquals(lbVal, nextLB);
}

@Test
public void testGetSourceJobListForRegion()  {
	
	JobDetails jobDetails = new JobDetails(context);
	List<String> sourceJobIdList = new ArrayList<String>();
	sourceJobIdList.add("Usage_VLR_1_LoadJob");
	jobDetails.setSourceJobList("RGN1", sourceJobIdList);
	List<String> getSourceJobList = jobDetails.getSourceJobList("RGN1");
	assertEquals(getSourceJobList, sourceJobIdList);
}

@Test
public void testGetSourceJobList()  {
	
	JobDetails jobDetails = new JobDetails(context);
	List<String> sourceJobIdList = new ArrayList<String>();
	sourceJobIdList.add("Usage_VLR_1_LoadJob");
	jobDetails.setSourceJobList(null, sourceJobIdList);
	List<String> getSourceJobList = jobDetails.getSourceJobList(null);
	assertEquals(getSourceJobList, sourceJobIdList);
}

@Test
public void testgetSourceBoundListForRegion()  {
	
	JobDetails jobDetails = new JobDetails(context);
	List<Boundary> srcBoundaryList =  new ArrayList<Boundary>();
	Boundary srcBoundary = new Boundary();
	srcBoundary.setSourceJobType("Loading");
	srcBoundary.setJobId("Usage_VOICE_1_LoadJob");
	srcBoundaryList.add(srcBoundary);
	jobDetails.setSourceBoundList("RGN1", srcBoundaryList);
	List<Boundary> getSourceBoundListForRegion  = jobDetails.getSourceBoundList("RGN1");
	assertEquals(srcBoundaryList, getSourceBoundListForRegion);
}

@Test
public void testgetSourceBoundList()  {
	
	JobDetails jobDetails = new JobDetails(context);
	List<Boundary> srcBoundaryList =  new ArrayList<Boundary>();
	Boundary srcBoundary = new Boundary();
	srcBoundary.setSourceJobType("Loading");
	srcBoundary.setJobId("Usage_VOICE_1_LoadJob");
	srcBoundaryList.add(srcBoundary);
	jobDetails.setSourceBoundList(null, srcBoundaryList);
	List<Boundary> getSourceBoundList  = jobDetails.getSourceBoundList(null);
	assertEquals(srcBoundaryList, getSourceBoundList);
}

@Test
public void testGetUsageSpecId()  {
	
	JobDetails jobDetails = new JobDetails(context);
	String getUsageSpecId = jobDetails.getUsageSpecId("USage_SMS_1");
	assertEquals("SMS", getUsageSpecId);
}

@Test
public void testGetSpecIdVersion()  {
	
	JobDetails jobDetails = new JobDetails(context);
	String getUsageSpecId = jobDetails.getSpecIdVersion("USage_SMS_1");
	assertEquals("SMS_1", getUsageSpecId);
}

}



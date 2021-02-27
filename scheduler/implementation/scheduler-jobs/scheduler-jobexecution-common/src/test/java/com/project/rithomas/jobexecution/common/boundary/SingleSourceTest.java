package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DayLightSavingUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { SingleSource.class, ModifySourceJobList.class})
public class SingleSourceTest {
	
	//WorkFlowContext context = new JobExecutionContext();


	WorkFlowContext context = new JobExecutionContext();
	
	Boundary boundary = new Boundary();
	
	List<Boundary> sourceBoundList = new ArrayList<Boundary>();
	
	@Mock
	ModifySourceJobList modifySourceJobListMock;
	
	@Mock
	JobDetails jobDetailsMock;
	

	
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
		
		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setSourceJobType("Aggregation");
		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
		boundary.setId(1);
		sourceBoundList.add(boundary);
		
		//context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
		
		PowerMockito.whenNew(ModifySourceJobList.class).withAnyArguments().thenReturn(modifySourceJobListMock);
		
		PowerMockito.whenNew(JobDetails.class).withAnyArguments().thenReturn(jobDetailsMock);
	}	

@Test
public void testCalculateUBWhenBoundaryIsNull() throws Exception {
	
	SingleSource singleSource = new SingleSource(context);
	String regionId = "RGN1";
	
	PowerMockito.when(jobDetailsMock.getSourceBoundList(regionId)).thenReturn(sourceBoundList);
	Calendar calendar = singleSource.calculateUB(regionId);
	assertEquals(null, calendar);
}

@Test
public void testCalculateUBWhenBoundaryNotNull() throws Exception {
	
	Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
			"2019.03.12 00:00:00").getTime());
	Date dateVal =   new SimpleDateFormat(JobExecutionContext.DATEFORMAT,
			Locale.ENGLISH).parse("2019-03-12 00:00:00");
	SingleSource singleSource = new SingleSource(context);
	String regionId = "RGN1";
	boundary.setMaxValue(timeStamp);
	sourceBoundList.add(boundary);
	
	PowerMockito.when(jobDetailsMock.getSourceBoundList(regionId)).thenReturn(sourceBoundList);
	Calendar calendar = singleSource.calculateUB(regionId);
	assertEquals(dateVal, calendar.getTime());
}
}



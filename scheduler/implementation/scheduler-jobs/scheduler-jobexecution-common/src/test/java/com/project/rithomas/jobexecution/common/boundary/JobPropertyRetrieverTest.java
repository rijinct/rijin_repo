package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { JobPropertyRetriever.class})
public class JobPropertyRetrieverTest {
	
	//WorkFlowContext context = new JobExecutionContext();


	WorkFlowContext context = new JobExecutionContext();
	
	BoundaryQuery boundaryQuery = new BoundaryQuery();

	Boundary boundary = new Boundary();
	
	Calendar calendar = new GregorianCalendar();
	
	List<Boundary> boundaryList = new ArrayList<Boundary>();

	
	@Before
	public void setUp() throws Exception {

		List<String> sourceJobIdList = new ArrayList<String>();
		sourceJobIdList.add("Usage_VLR_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCERETENTIONDAYS, "5");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCE_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS, "10");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "10");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);

	}	
	@Test
	public void testGetAggRetentionDaysWhenMaxValueIsNotNull() {
		Timestamp maxValue = new Timestamp(calendar.getTimeInMillis());
		context.setProperty(JobExecutionContext.MAXVALUE, maxValue);
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		int retentionDays = jobPropertyRetriever.getAggRetentionDays();
		assertEquals(10, retentionDays);
	}
	
	@Test
	public void testGetAggRetentionDaysWhenMaxValueIsNull() {
		context.setProperty(JobExecutionContext.MAXVALUE, null);
		List<String> sourceJobIdList = Arrays.asList("US_SGSN_1", "US_LTE_4G_1");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobIdList);
		context.setProperty("US_SGSN_1_RETENTIONDAYS", "5");
		context.setProperty("US_LTE_4G_1_RETENTIONDAYS", "5");
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		int retentionDays = jobPropertyRetriever.getAggRetentionDays();;
		assertEquals(5, retentionDays);
	}
	
	@Test
	public void testIsPlevelHourOr15Min() {
		context.setProperty(JobExecutionContext.PLEVEL, "15MIN");
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		assertTrue(jobPropertyRetriever.isPlevelHourOr15Min());
	}
	
	@Test
	public void testGetAggregationLevel() {
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		String alevel = jobPropertyRetriever.getAggregationLevel();
		assertEquals("HOUR", alevel);
	}
	
	@Test
	public void testGetPartitionLevel() {
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		String plevel = jobPropertyRetriever.getPartitionLevel();
		assertEquals("HOUR", plevel);
	}
	
	@Test
	public void testGetAggregationRetentionDays() {
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		context.setProperty(JobExecutionContext.AGG_RETENTIONDAYS, 20);
		int aggRetentiondays = jobPropertyRetriever.getAggregationRetentionDays();
		assertEquals(20, aggRetentiondays);
	}
	
	@Test
	public void testArePartitionsLoadedInCouchbase() {
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(context);
		context.setProperty("Usage_VLR_1_LoadJob_PARTITION_CHECK_REQUIRED", true);
		context.setProperty("Usage_VLR_1_LoadJob_PARTITION_UPDATE_FORMULA", "{HH24-5 20},{MIN15-5 20},{HH24 96},{MIN15 96}");
		assertTrue(jobPropertyRetriever.arePartitionsLoadedInCouchbase());
		
	}
}


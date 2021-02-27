
package com.project.rithomas.jobexecution.common.util;

import static org.mockito.Mockito.mock;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { UsageAggregationStatusUtil.class })
public class UsageAggregationStatusUtilTest {

	GeneratorWorkFlowContext context;

	String usageJobId;

	String perfJobId;

	String reportTime;

	String upperBound;

	QueryExecutor mockQueryExecutor = mock(QueryExecutor.class);

	Session mockSession = mock(Session.class);

	UsageAggregationStatusUtil usageAggregationStatusUtil = new UsageAggregationStatusUtil();

	@Before
	public void setUp() throws Exception {
		context = new GeneratorWorkFlowContext();
		usageJobId = "Usage_SMS_1_LoadJob";
		perfJobId = "Perf_SMS_1_HOUR_AggregateJob";
		reportTime = "2013-08-25 02:00:00";
		upperBound = "2013-08-25 10:00:00";
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
	}

	@Test
	public void testGetAggregatedRecordCount() throws JobExecutionException {
		String query = QueryConstants.GET_AGGREGATED_RECORD_COUNT
				.replace(QueryConstants.USAGE_JOB_ID, usageJobId)
				.replace(QueryConstants.PERF_JOB_ID, perfJobId)
				.replace(QueryConstants.REPORT_TIME, reportTime)
				.replace(QueryConstants.UPPER_BOUND, upperBound)
				.replace(QueryConstants.STATUS, "true");
		Object[] value = new Object[] { "3" };
		PowerMockito.when(
				mockQueryExecutor.executeMetadatasqlQuery(query, context))
				.thenReturn(value);
		Long aggCount = usageAggregationStatusUtil.getAggregatedRecordCount(
				context, usageJobId, perfJobId, reportTime, upperBound, null);
		// assertEquals(3, aggCount);
	}

	@Test
	public void testGetNonAggregatedRecordCount() throws JobExecutionException {
		String query = QueryConstants.GET_AGGREGATED_RECORD_COUNT
				.replace(QueryConstants.USAGE_JOB_ID, usageJobId)
				.replace(QueryConstants.PERF_JOB_ID, perfJobId)
				.replace(QueryConstants.REPORT_TIME, reportTime)
				.replace(QueryConstants.UPPER_BOUND, upperBound)
				.replace(QueryConstants.STATUS, "false");
		Object[] value = new Object[] { "3" };
		PowerMockito.when(
				mockQueryExecutor.executeMetadatasqlQuery(query, context))
				.thenReturn(value);
		Long nonAggCount = usageAggregationStatusUtil
				.getNonAggregatedRecordCount(context, usageJobId, perfJobId,
						reportTime, upperBound, null);
	}

	@Test
	public void testInsertIntoUsageAggList() throws JobExecutionException {
		usageAggregationStatusUtil.insertIntoUsageAggList(context, usageJobId,
				perfJobId, reportTime, 23l, null);
	}

	@Test
	public void testUpdateUsageAggList() throws JobExecutionException {
		usageAggregationStatusUtil.updateUsageAggList(context, perfJobId,
				reportTime, upperBound, null);
	}
}

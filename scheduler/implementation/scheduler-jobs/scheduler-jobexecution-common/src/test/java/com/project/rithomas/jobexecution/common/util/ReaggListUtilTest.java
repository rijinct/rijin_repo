
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = ReaggregationListUtil.class)
public class ReaggListUtilTest {

	ReaggregationListUtil reaggListUtil = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	QueryExecutor queryExecutor;

	@Before
	public void setUp() throws Exception {
		reaggListUtil = new ReaggregationListUtil();
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExecutor);
		Mockito.doThrow(new JobExecutionException("Test exception"))
				.when(queryExecutor)
				.executePostgresqlUpdate(
						"UPDATE rithomas.reagg_list SET status = 'Completed' WHERE provider_jobname = 'Perf_SMS_1_HOUR_ReaggregateJob' AND requestor_jobname IN ('Usage_SMS_1_LoadJob') AND status IN ('Completed_Part)",
						context);
		Mockito.doThrow(new JobExecutionException("Test exception"))
				.when(queryExecutor)
				.executePostgresqlUpdate(
						"UPDATE rithomas.reagg_list SET status = 'Completed' WHERE provider_jobname = 'Perf_SMS_1_HOUR_ReaggregateJob' AND requestor_jobname IN ('Usage_SMS_1_LoadJob') AND status IN ('Completed_Part) AND region_id='Region11'",
						context);
		Object[] value = new Object[] { "Dummy" };
		Mockito.when(
				queryExecutor
						.executeMetadatasqlQuery(
								"SELECT provider_jobname FROM rithomas.reagg_list r WHERE r.requestor_jobname = 'Usage_SMS_1_LoadJob' AND r.provider_jobname = 'Perf_SMS_1_HOUR_ReaggregateJob' AND r.report_time = '2013-06-03 00:00:00' AND r.status <> 'Completed' and r.status <> 'Completed_Part'",
								context)).thenReturn(value);
	}

	@Test
	public void testUpdateReaggregationList() throws JobExecutionException {
		reaggListUtil.updateReaggregationList(null,
				"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
				"2013-06-03 00:00:00", "Completed_Part", "Initialized",null);
		reaggListUtil.updateReaggregationList(null,
				"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
				null, "Completed", "Completed_Part",null);
		try {
			reaggListUtil.updateReaggregationList(null,
					"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
					null, "Completed", "'Completed_Part",null);
			// assertTrue(false);
		} catch (JobExecutionException ex) {
			assertEquals("Test exception", ex.getMessage());
		}
	}

	@Test
	public void testUpdateReaggregationListWithRegion()
			throws JobExecutionException {
		reaggListUtil.updateReaggregationList(null,
				"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
				"2013-06-03 00:00:00", "Completed_Part", "Initialized", "Region11");
		reaggListUtil.updateReaggregationList(null,
				"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
				null, "Completed", "Completed_Part", "Region11");
		reaggListUtil.updateReaggregationList(null,
				"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
				null, "Completed", "Completed_Part", null);
		try {
			reaggListUtil.updateReaggregationList(null,
					"Perf_SMS_1_HOUR_ReaggregateJob", "'Usage_SMS_1_LoadJob'",
					null, "Completed", "'Completed_Part", "Region11");
			// assertTrue(false);
		} catch (JobExecutionException ex) {
			assertEquals("Test exception", ex.getMessage());
		}
	}

	@Test
	public void testCheckRecordExists() throws JobExecutionException {
		boolean recordExists = reaggListUtil.recordExists(null,
				"Usage_SMS_1_LoadJob", "Perf_SMS_1_HOUR_ReaggregateJob",
				"2013-06-03 00:00:00", null);
		// assertEquals(true, recordExists);
		recordExists = reaggListUtil.recordExists(null, "Usage_SMS_2_LoadJob",
				"Perf_SMS_1_HOUR_ReaggregateJob", "2013-06-03 00:00:00", null);
		assertEquals(false, recordExists);
		recordExists = reaggListUtil.recordExists(null, "Usage_SMS_1_LoadJob",
				"Perf_SMS_1_HOUR_ReaggregateJob", "2013-06-03 00:00:00",
				"Region11");
		assertEquals(false, recordExists);
	}

	@Test
	public void testInsertIntoReaggList() throws JobExecutionException {
		boolean inserted = reaggListUtil.insertIntoReaggList(null,
				"2013-06-09 00:00:00", "2013-06-03 00:00:00", "HOUR",
				"Perf_SMS_1_HOUR_ReaggregateJob", "Usage_SMS_1_LoadJob", null,
				"Initialized");
		assertEquals(true, inserted);
		inserted = reaggListUtil.insertIntoReaggList(null,
				"2013-06-09 00:00:00", "2013-06-03 00:00:00", "HOUR",
				"Perf_SMS_1_HOUR_ReaggregateJob", "Usage_SMS_1_LoadJob",
				"Region11", "Initialized");
		assertEquals(true, inserted);
	}

	@Test
	public void testGetReaggregatedReportTimes() throws JobExecutionException {
		Session mockSession = mock(Session.class);
		String query = QueryConstants.GET_REPORT_TIMES.replace(
				QueryConstants.PROVIDER_JOB, "ProviderJob").replace(
				QueryConstants.SOURCE_JOB_NAMES, "'Job1','Job2','Job3'");
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		List data = new ArrayList<Object>();
		data.add(new Object());
		data.add(new Object());
		data.add(new Object());
		PowerMockito.when(
				queryExecutor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(data);
		List<String> reportTimeList = reaggListUtil.getReaggregatedReportTimes(
				context, "ProviderJob", "'Job1','Job2','Job3'");
		assertEquals(data.size(), reportTimeList.size());
	}
	
	@Test
	public void testIsAnyReaggDependentJobsActiveWhenNoActiveJobs()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		int retentionDays = 12;
		boolean IsAnyReaggDependentJobsActive = reaggListUtil
				.isAnyReaggDependentJobsActive(context, retentionDays);
		assertEquals(false, IsAnyReaggDependentJobsActive);
	}

	@Test
	public void testIsAnyReaggDependentJobsActive()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		int retentionDays = 12;
		String jobName = "Perf_SMS_1_HOUR_AggregateJob";
		context.setProperty(JobExecutionContext.JOB_NAME,
				jobName);
		context.setProperty(JobExecutionContext.PLEVEL, "HOUR");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		List list = new ArrayList<>();
		list.add("dummy");
		String reportTime = DateFunctionTransformation.getInstance()
				.getFormattedDate(DateFunctionTransformation
						.getDateBySubtractingDays(retentionDays, context)
						.getTime());
		String sql = String.format(QueryConstants.ACTIVE_DEPENDENT_REAGG_JOBS,
				jobName, reportTime);
		PowerMockito.when(queryExecutor.executeMetadatasqlQueryMultiple(sql, context)).thenReturn(list );

		boolean IsAnyReaggDependentJobsActive = reaggListUtil
				.isAnyReaggDependentJobsActive(context, retentionDays);
		assertEquals(true, IsAnyReaggDependentJobsActive);
	}

	@Test
	public void testIsJobDisabledWhenPropertiesAreNotSetInDB()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		String providerAggJob = "Perf_SMS_1_HOUR_AggregateJob";
		String providerJob = "Perf_SMS_1_HOUR_ReaggregateJob";
		assertFalse(reaggListUtil.isJobDisabled(context, providerAggJob,
				providerJob));
		
	}

	@Test
	public void testIsJobDisabledWhenReaggJobIsDisabled()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		String providerAggJob = "Perf_SMS_1_HOUR_AggregateJob";
		String providerJob = "Perf_SMS_1_HOUR_ReaggregateJob";
		String jobsEnableQuery = QueryConstants.GET_JOB_ENABLED_QUERY
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.PROVIDER_AGG_JOB, providerAggJob);
		List value = new ArrayList<>();
		String[] property = { "REAGG_JOB_ENABLED", "NO" };
		value.add(property);
		PowerMockito.when(queryExecutor.executeMetadatasqlQueryMultiple(jobsEnableQuery, context)).thenReturn(value );
		assertTrue(reaggListUtil.isJobDisabled(context, providerAggJob,
				providerJob));
	}

	@Test
	public void testIsJobDisabledWhenBothAreDisabled()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		String providerAggJob = "Perf_SMS_1_HOUR_AggregateJob";
		String providerJob = "Perf_SMS_1_HOUR_ReaggregateJob";
		String jobsEnableQuery = QueryConstants.GET_JOB_ENABLED_QUERY
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.PROVIDER_AGG_JOB, providerAggJob);
		List value = new ArrayList<>();
		String[] reagg_property = { "JOB_ENABLED", "NO" };
		String[] agg_property = { "REAGG_JOB_ENABLED", "NO" };
		value.add(reagg_property);
		value.add(agg_property);
		PowerMockito.when(queryExecutor
				.executeMetadatasqlQueryMultiple(jobsEnableQuery, context))
				.thenReturn(value);
		assertTrue(reaggListUtil.isJobDisabled(context, providerAggJob,
				providerJob));
	}

	@Test
	public void testIsJobDisabledWhenBothAreEnabled()
			throws JobExecutionException {
		GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();
		String providerAggJob = "Perf_SMS_1_HOUR_AggregateJob";
		String providerJob = "Perf_SMS_1_HOUR_ReaggregateJob";
		String jobsEnableQuery = QueryConstants.GET_JOB_ENABLED_QUERY
				.replace(QueryConstants.PROVIDER_JOB, providerJob)
				.replace(QueryConstants.PROVIDER_AGG_JOB, providerAggJob);
		List value = new ArrayList<>();
		String[] reagg_property = { "JOB_ENABLED", "YES" };
		String[] agg_property = { "REAGG_JOB_ENABLED", "YES" };
		value.add(reagg_property);
		value.add(agg_property);
		PowerMockito.when(queryExecutor
				.executeMetadatasqlQueryMultiple(jobsEnableQuery, context))
				.thenReturn(value);
		assertFalse(reaggListUtil.isJobDisabled(context, providerAggJob,
				providerJob));
	}
}


package com.project.rithomas.jobexecution.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockitoSession;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.NativeQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveHibernateUtil;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;

import junit.framework.Assert;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor(value = {
		"com.rijin.scheduler.jobexecution.hive.query.HiveHibernateUtil",
		"com.project.rithomas.jobexecution.common.util.MetadataHibernateUtil" })
@PrepareForTest(value = { JobTriggerPrecheck.class, TimeZoneUtil.class,
		MetadataHibernateUtil.class, HiveHibernateUtil.class,
		Configuration.class, QueryExecutor.class, JobTypeDictionary.class,
		ModifySourceJobList.class, DateFunctionTransformation.class })
public class JobTriggerPrecheckTest {

	JobTriggerPrecheck jobTriggerPrecheck;

	private final WorkFlowContext context = new JobExecutionContext();

	List depJobs = new ArrayList();

	@Mock
	QueryExecutor executor;

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	Session session;

	@Mock
	Configuration configuration;

	@Mock
	JobTypeDictionary jobTypeDictionary;

	@Mock
	TimeZoneUtil timeZoneUtil;

	SessionFactory mockSessionFactory = mock(SessionFactory.class);

	Session mockSession = mock(Session.class);

	@Mock
	private NativeQuery mockSQLQuery;

	@Mock
	ModifySourceJobList modifySourceJobList;

	@Mock
	DateFunctionTransformation dateFunctionTransformation;

	@Before
	public void setUp() throws Exception {
		jobTriggerPrecheck = new JobTriggerPrecheck();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_MONTH_ReaggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Reaggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_SMS_1_DAY_ReaggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Reaggregation");
		context.setProperty(JobExecutionContext.ALEVEL, "MONTH");
		// Mock instantiation of query executor
		PowerMockito.mockStatic(HiveHibernateUtil.class);
		PowerMockito.when(HiveHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.mockStatic(MetadataHibernateUtil.class);
		PowerMockito.when(MetadataHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.when(HiveHibernateUtil.getSessionFactory())
				.thenReturn(mockSessionFactory);
		PowerMockito.when(mockSessionFactory.openSession())
				.thenReturn(mockSession);
		PowerMockito.when(mockSession.createSQLQuery(Mockito.anyString()))
				.thenReturn(mockSQLQuery);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(executor);
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		depJobs.add("Perf_SMS_1_DAY_ReaggregateJob");
		Object[] objects = new Object[] { 0 };
		Mockito.when(executor.executeMetadatasqlQueryMultiple(
				"SELECT DISTINCT provider_jobname AS jobid FROM rithomas.reagg_list, rithomas.job_dictionary, rithomas.job_prop WHERE reagg_list.requestor_jobname = 'Perf_SMS_1_HOUR_AggregateJob' AND reagg_list.status NOT IN ('Pending', 'Completed', 'Completed_Part') AND job_dictionary.jobid = reagg_list.provider_jobname AND job_dictionary.id = job_prop.jobid AND job_prop.paramname = 'REAGG_JOB_ENABLED' AND job_prop.paramvalue = 'YES'",
				context)).thenReturn(depJobs);
		Mockito.when(executor.executeMetadatasqlQuery(
				"select count(*) from rithomas.etl_status where job_name='Perf_SMS_1_DAY_ReaggregateJob' and status='R'",
				context)).thenReturn(objects);
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		jobTriggerPrecheck.execute(context);
		List<String> jobs = (List<String>) context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED);
		assertTrue(jobs.isEmpty());
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		assertTrue(jobTriggerPrecheck.execute(context));
		jobs = (List<String>) context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED);
		assertTrue(!jobs.isEmpty());
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Loading");
		jobTriggerPrecheck.execute(context);
		jobs = (List<String>) context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED);
		assertTrue(jobs.isEmpty());
	}

	@Test
	public void testAddDependentJobs()
			throws WorkFlowExecutionException, JobExecutionException {
		context.setProperty(JobExecutionContext.AUTO_TRIGGER_ENABLED, "YES");
		context.setProperty(JobExecutionContext.JOBTYPE, "IMPORT_JOB");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_TNP_VOICE_1_ImportJob");
		context.setProperty(JobExecutionContext.PLEVEL, "15MIN");
		context.setProperty(JobExecutionContext.UB, 1422508800000L);
		String queryTemplate = QueryConstants.FETCH_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace(QueryConstants.JOB_ID,
				"Usage_TNP_VOICE_1_ImportJob");
		List<Object[]> dependentJobs = getDependentJobs();
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(dependentJobs);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobs = (List<String>) context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED);
		assertTrue(!jobs.isEmpty());
	}

	private List<Object[]> getDependentJobs() {
		List<Object[]> dependentJobs = new ArrayList<Object[]>();
		Object[] row1 = { "PS_TNP_TNP_VOICE_1_Z15MI_1_KPIJob",
				"2015-01-29 10:30:00", "15MIN" };
		Object[] row2 = { "PS_TNP_TNP_VOICE_1_Z15MI_2_KPIJob", null, "15MIN" };
		Object[] row3 = { "PS_TNP_TNP_VOICE_1_Z15MI_3_KPIJob", null, "15MIN" };
		Object[] row4 = { "PS_TNP_TNP_VOICE_1_Z5MIN_1_KPIJob", null, "5MIN" };
		dependentJobs.add(row1);
		dependentJobs.add(row2);
		dependentJobs.add(row3);
		dependentJobs.add(row4);
		return dependentJobs;
	}

	@Test
	public void testPerfJobAutoTrigger1()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_SMS_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_SMS_1_DAY_AggregateJob",
				"2013-07-27 19:00:00.0", "DAY", null };
		List list = new ArrayList();
		list.add(object1);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(list);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobList = new ArrayList<String>();
		jobList.add("Perf_SMS_1_DAY_AggregateJob");
		assertEquals(jobList,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test
	public void testPerfJobAutoTrigger2()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_CEI_SMS_2_1_HOUR_AggregateJob",
				"2013-07-27 19:00:00.0", "HOUR", "" };
		Object[] object2 = new Object[] { "Perf_CEI_SMS_1_1_DAY_AggregateJob",
				null, "DAY", null };
		List<Object> list = new ArrayList<Object>();
		list.add(object1);
		list.add(object2);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(list);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobList = new ArrayList<String>();
		jobList.add("Perf_CEI_SMS_2_1_HOUR_AggregateJob");
		jobList.add("Perf_CEI_SMS_1_1_DAY_AggregateJob");
		assertEquals(jobList,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test
	public void testPerfJobAutoTrigger15min()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_SMS_TEST_1_15MIN_AggregateJob",
				null, "15MIN", null };
		List<Object> list = new ArrayList<Object>();
		list.add(object1);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(list);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobList = new ArrayList<String>();
		jobList.add("Perf_SMS_TEST_1_15MIN_AggregateJob");
		assertEquals(jobList,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	public static void setValues(WorkFlowContext context,
			ModifySourceJobList modifySourceJobList, NativeQuery mockSQLQuery,
			Session mockSession) throws Exception {
		PowerMockito.whenNew(ModifySourceJobList.class).withAnyArguments()
				.thenReturn(modifySourceJobList);
		mockStatic(TimeZoneUtil.class);
		Mockito.when(TimeZoneUtil.isTimeZoneEnabled(context)).thenReturn(true);
		Mockito.when(TimeZoneUtil.isTimeZoneAgnostic(context))
				.thenReturn(false);
		List<String> listRegion = new ArrayList<String>();
		listRegion.add("Region1");
		listRegion.add("Region2");
		Mockito.when(modifySourceJobList.getListOfSkipTimeZoneForAgnosticJobs())
				.thenReturn(listRegion);
		HashMap<String, Long> RegionMap = new HashMap<String, Long>();
		RegionMap.put("Region2", Long.parseLong("1374931800000"));
		RegionMap.put("Region1", Long.parseLong("1373931800000"));
		HashMap<String, Long> RegionMapUp = new HashMap<String, Long>();
		RegionMapUp.put("Region1", Long.parseLong("1424931800000"));
		RegionMapUp.put("Region2", Long.parseLong("1423931800000"));
		context.setProperty(JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION,
				RegionMap);
		context.setProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION,
				RegionMapUp);
		List<String> listAggRegion = new ArrayList<String>();
		listAggRegion.add("Region1");
		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
				listAggRegion);
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String[] job1 = { "2019-11-21 10:10:10", "2019-11-21 10:10:10",
				"2019-11-21 10:10:10", "2019-11-21 10:10:10" };
		String[] job2 = { "2019-11-21 10:10:10", "2019-11-21 10:10:10",
				"2019-11-21 10:10:10", "2019-11-21 10:10:10" };
		String[] job3 = { "2019-11-21 10:10:10", "2019-11-21 10:10:10",
				"2019-11-21 10:10:10", "2019-11-21 10:10:10" };
		String[] job4 = { "2019-11-21 10:10:10", "2019-11-21 10:10:10",
				"2019-11-21 10:10:10", "2019-11-21 10:10:10" };
		List<Object> jobOfData = new ArrayList<Object>();
		jobOfData.add(job1);
		jobOfData.add(job2);
		jobOfData.add(job3);
		jobOfData.add(job4);
		PowerMockito.when(mockSession.createSQLQuery(Mockito.anyString()))
				.thenReturn(mockSQLQuery);
		PowerMockito.when(mockSQLQuery.list()).thenReturn(jobOfData);
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME, "abc");
		context.setProperty(JobExecutionContext.JOBTYPE, "aggregation");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobTypeDictionary.PERF_JOB_TYPE, "Reaggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
	}

	@Test
	public void TestMapValues() throws Exception {
		setValues(context, modifySourceJobList, mockSQLQuery, mockSession);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> list = new ArrayList<String>();
		assertEquals(list,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test
	public void TestJobBoundaryMap() throws Exception {
		setValues(context, modifySourceJobList, mockSQLQuery, mockSession);
		context.setProperty(JobExecutionContext.JOBTYPE, "LOADING");
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		context.setProperty(JobExecutionContext.LB, 1374953400000L);
		assertTrue(jobTriggerPrecheck.execute(context));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testLowerAndUpperBoundValuesWithException() throws Exception {
		setValues(context, modifySourceJobList, mockSQLQuery, mockSession);
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		String[] job1 = { "Region1", "Region1", "Region1", "Region1" };
		String[] job2 = { "Region1", "Region1", "Region1", "Region1" };
		String[] job3 = { "Region1", "Region1", "Region1", "Region1" };
		String[] job4 = { "Region1", "Region1", "Region1", "Region1" };
		List<Object> jobOfData = new ArrayList<Object>();
		jobOfData.add(job1);
		jobOfData.add(job2);
		jobOfData.add(job3);
		jobOfData.add(job4);
		PowerMockito.when(mockSQLQuery.list()).thenReturn(jobOfData);
		depJobs.add("Perf_SMS_1_DAY_ReaggregateJob");
		Mockito.when(executor.executeMetadatasqlQueryMultiple(Mockito.any(),
				Mockito.any())).thenReturn(depJobs);
		jobTriggerPrecheck.execute(context);
	}

	public static void setValuesForRegionAndBound(String[] job1, String[] job2,
			String[] job3, String[] job4, NativeQuery mockSQLQuery,
			QueryExecutor executor) throws JobExecutionException {
		Mockito.when(TimeZoneUtil.getZoneId(Mockito.any(), Mockito.any()))
				.thenReturn("zone1");
		List<Object> jobOfData = new ArrayList<Object>();
		jobOfData.add(job1);
		jobOfData.add(job2);
		jobOfData.add(job3);
		jobOfData.add(job4);
		PowerMockito.when(mockSQLQuery.list()).thenReturn(jobOfData);
		Mockito.when(executor.executeMetadatasqlQueryMultiple(Mockito.any(),
				Mockito.any())).thenReturn(jobOfData);
	}

	@Test
	public void testLowerAndUpperBoundValues() throws Exception {
		setValues(context, modifySourceJobList, mockSQLQuery, mockSession);
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		String[] job1 = { "PS_TNP_TNP_VOICE_1_Z5MIN_1_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "Region1" };
		String[] job2 = { "PS_TNP_TNP_VOICE_1_Z5MIN_2_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "Region1" };
		String[] job3 = { "PS_TNP_TNP_VOICE_1_Z5MIN_3_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "Region1" };
		String[] job4 = { "PS_TNP_TNP_VOICE_1_Z5MIN_4_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "Region1" };
		setValuesForRegionAndBound(job1, job2, job3, job4, mockSQLQuery,
				executor);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobList = new ArrayList<String>();
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_1_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_2_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_3_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_4_KPIJob");
		assertEquals(jobList,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test
	public void testWhenNoRegion() throws Exception {
		setValues(context, modifySourceJobList, mockSQLQuery, mockSession);
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		String[] job1 = { "PS_TNP_TNP_VOICE_1_Z5MIN_1_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "" };
		String[] job2 = { "PS_TNP_TNP_VOICE_1_Z5MIN_2_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "" };
		String[] job3 = { "PS_TNP_TNP_VOICE_1_Z5MIN_3_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "" };
		String[] job4 = { "PS_TNP_TNP_VOICE_1_Z5MIN_4_KPIJob",
				"2014-02-26 11:53:20.150", "Hour", "" };
		setValuesForRegionAndBound(job1, job2, job3, job4, mockSQLQuery,
				executor);
		HashMap<String, Long> RegionMapUp = new HashMap<String, Long>();
		RegionMapUp.put("Region1", Long.parseLong("1424931800000"));
		context.setProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION,
				RegionMapUp);
		assertTrue(jobTriggerPrecheck.execute(context));
		List<String> jobList = new ArrayList<String>();
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_1_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_2_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_3_KPIJob");
		jobList.add("PS_TNP_TNP_VOICE_1_Z5MIN_4_KPIJob");
		assertEquals(jobList,
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testWorkFlowExecutionException()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_15MIN_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID, null);
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object object1 = new Object();
		List list = new ArrayList();
		list.add(object1);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(list);
		jobTriggerPrecheck.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testParseException()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_CEI_SMS_2_1_HOUR_AggregateJob",
				"2013-07-27 19:00:00", "HOUR" };
		Object[] object2 = new Object[] { "Perf_CEI_SMS_1_1_DAY_AggregateJob",
				null, "DAY" };
		List list = new ArrayList();
		list.add(object1);
		list.add(object2);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(list);
		jobTriggerPrecheck.execute(context);
	
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testJobExecutionException()
			throws WorkFlowExecutionException, JobExecutionException {
		String queryTemplate = QueryConstants.AGG_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_CEI_SMS_1_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_CEI_SMS_2_1_HOUR_AggregateJob",
				"2013-07-27 19:00:00", "HOUR" };
		Object[] object2 = new Object[] { "Perf_CEI_SMS_1_1_DAY_AggregateJob",
				null, "DAY" };
		List<Object> list = new ArrayList<Object>();
		list.add(object1);
		list.add(object2);
		PowerMockito
				.when(executor.executeMetadatasqlQueryMultiple(query, context))
				.thenThrow(new JobExecutionException(""));
		jobTriggerPrecheck.execute(context);
		
	}

	@Test
	public void testAutoTriggerDisabled() throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.AUTO_TRIGGER_ENABLED, "NO");
		context.setProperty(JobExecutionContext.QS_AUTO_TRIGGER_ENABLED, "NO");
		assertTrue(jobTriggerPrecheck.execute(context));
	}

	@Test
	public void testQSJobsAutoTriggerForAgg()
			throws JobExecutionException, WorkFlowExecutionException {
		String queryTemplate = QueryConstants.QS_DEPENDENT_JOBS_QUERY;
		String query = queryTemplate.replace("$JOB_ID",
				"Perf_SMS_1_DAY_AggregateJob");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_SMS_1_DAY_AggregateJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.SOURCEJOBID,
				"Perf_SMS_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.LB_INIT, 1374931800000L);
		context.setProperty(JobExecutionContext.UB, 1374953400000L);
		Object[] object1 = new Object[] { "Perf_SMS_1_DAY_QSJob" };
		List<String> jobs = new ArrayList<String>(
				Arrays.asList("Perf_SMS_1_DAY_QSJob"));
		PowerMockito.when(executor.executeMetadatasqlQuery(query, context))
				.thenReturn(object1);
		NativeQuery sqlQuery = mock(NativeQuery.class);
		PowerMockito.when(mockSession.createSQLQuery(query))
				.thenReturn(sqlQuery);
		PowerMockito.when(sqlQuery.list()).thenReturn(jobs);
		assertTrue(jobTriggerPrecheck.execute(context));
		Assert.assertEquals(Arrays.asList("Perf_SMS_1_DAY_QSJob"),
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}

	@Test
	public void testQSJobsAutoTriggerForEntity()
			throws JobExecutionException, WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		context.setProperty(JobExecutionContext.JOBTYPE, "Correlation");
		assertTrue(jobTriggerPrecheck.execute(context));
		Assert.assertEquals(Arrays.asList("Entity_SMS_TYPE_1_QSJob"),
				context.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED));
	}
}

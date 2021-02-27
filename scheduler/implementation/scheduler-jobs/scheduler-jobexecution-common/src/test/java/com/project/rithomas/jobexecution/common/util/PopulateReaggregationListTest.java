package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.PopulateReaggregationList.JobRetention;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;


@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { PopulateReaggregationList.class })
public class PopulateReaggregationListTest {

	private static final String AGG_JOB_NAME = "Perf_VOICE_SEGG_1_HOUR_AggregateJob";
	private static final String REAGG_JOB_NAME = "Perf_VOICE_SEGG_1_HOUR_ReaggregateJob";

	JobExecutionContext context = new JobExecutionContext();
	
	TreeNode<JobRetention> parent;
	
	Map<String, TreeNode<JobRetention>> jobMap = new HashMap<>();
	
	@Mock
	QueryExecutor queryExec;

	@Mock
	JobDictionaryQuery jdQuery;
	
	@Mock
	JobPropertyQuery jpQuery;
	
	@Before
	public void setUp() throws Exception {
		JobRetention jr = new JobRetention(REAGG_JOB_NAME, 10, 123);
		parent = new TreeNode<>(jr);
		jobMap.put(REAGG_JOB_NAME, parent);
		context.setProperty(JobExecutionContext.JOB_NAME, REAGG_JOB_NAME);
		context.setProperty(JobExecutionContext.WEEK_START_DAY, "MONDAY");
		JobDictionary aggJob = new JobDictionary(1);
		JobDictionary reaggJob = new JobDictionary(2);
		PowerMockito.whenNew(JobDictionaryQuery.class).withNoArguments().thenReturn(jdQuery);
		PowerMockito.whenNew(JobPropertyQuery.class).withNoArguments().thenReturn(jpQuery);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments().thenReturn(queryExec);
		String query = "SELECT jd.jobid AS provider_jobid, jp.paramvalue AS alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.boundary bi WHERE  j.jobid = jd.id AND "
				+ "j.paramname = 'PERF_JOB_NAME' AND  j.paramvalue = bi.jobid AND  bi.maxvalue IS NOT NULL) as maxvalue, (SELECT j.paramvalue FROM rithomas.job_prop j, "
				+ "rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name  FROM rithomas.job_dictionary jd, "
				+ "rithomas.job_prop jp, rithomas.boundary b WHERE  b.sourcejobid = 'Perf_VOICE_SEGG_1_HOUR_ReaggregateJob' AND b.jobid = jd.jobid AND  jd.id = jp.jobid "
				+ "AND  jp.paramname = 'ALEVEL' AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation') UNION "
				+ "SELECT b.jobid as provider_jobid, jp.paramvalue as alevel, (SELECT bi.maxvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd, rithomas.boundary bi "
				+ "WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME' AND j.paramvalue = bi.jobid AND bi.maxvalue IS NOT NULL) as maxvalue,"
				+ "(SELECT j.paramvalue FROM rithomas.job_prop j, rithomas.job_dictionary jd  WHERE jd.jobid = b.jobid AND j.jobid = jd.id AND j.paramname = 'PERF_JOB_NAME') as perf_job_name "
				+ " FROM rithomas.job_dict_dependency c, rithomas.job_list j, rithomas.boundary b, rithomas.job_prop jp, rithomas.job_dictionary jd "
				+ " WHERE j.jobid = 'Perf_VOICE_SEGG_1_HOUR_ReaggregateJob' AND c.source_jobid = j.id AND b.id = c.bound_jobid AND b.jobid = jd.jobid AND jp.jobid = jd.id "
				+ " AND jp.paramname = 'ALEVEL' AND jd.typeid=(select id from rithomas.job_type_dictionary where jobtypeid='Reaggregation')";
		String[] depJob1 = new String[] {"Perf_VOICE_SEGG_1_DAY_ReaggregateJob","DAY","2019-05-01 00:00:00"};
		String[] depJob2 = new String[] {"Perf_VOICE_L2_1_HOUR_ReaggregateJob","HOUR","2019-05-01 22:00:00"};
		List<Object[]> resultList = new ArrayList<>();
		resultList.add(depJob1);
		resultList.add(depJob2);
		when(queryExec.executeMetadatasqlQueryMultiple(query, context))
				.thenReturn(null).thenReturn(resultList);
		when(jdQuery.retrieve(AGG_JOB_NAME)).thenReturn(aggJob);
		when(jdQuery.retrieve(REAGG_JOB_NAME)).thenReturn(reaggJob);
	}

	@Test
	public void testBuildHierarchy() {
		try {
			JobDictionary reaggJob1 = new JobDictionary(3);
			JobDictionary reaggJob2 = new JobDictionary(4);
			JobDictionary aggJob1 = new JobDictionary(5);
			JobDictionary aggJob2 = new JobDictionary(6);
			PopulateReaggregationList populateReaggregationList = new PopulateReaggregationList();
			when(jdQuery.retrieve("Perf_VOICE_L2_1_HOUR_ReaggregateJob")).thenReturn(reaggJob1);
			when(jdQuery.retrieve("Perf_VOICE_SEGG_1_DAY_ReaggregateJob")).thenReturn(reaggJob2);
			when(jdQuery.retrieve("Perf_VOICE_L2_1_HOUR_AggregateJob")).thenReturn(aggJob1);
			when(jdQuery.retrieve("Perf_VOICE_SEGG_1_DAY_AggregateJob")).thenReturn(aggJob2);
			JobProperty retention1 = new JobProperty("RETENTIONDAYS", "5");
			JobProperty aggJobName1 = new JobProperty("PERF_JOB_NAME", "Perf_VOICE_L2_1_HOUR_AggregateJob");
			JobProperty retention2 = new JobProperty("RETENTIONDAYS", "15");
			JobProperty aggJobName2 = new JobProperty("PERF_JOB_NAME", "Perf_VOICE_SEGG_1_DAY_AggregateJob");
			when(jpQuery.retrieve(3, "PERF_JOB_NAME")).thenReturn(aggJobName1);
			when(jpQuery.retrieve(4, "PERF_JOB_NAME")).thenReturn(aggJobName2);
			when(jpQuery.retrieve(5, "RETENTIONDAYS")).thenReturn(retention1);
			when(jpQuery.retrieve(6, "RETENTIONDAYS")).thenReturn(retention2);
			populateReaggregationList.buildHierarchy(context,
					REAGG_JOB_NAME, null, parent, jobMap);
			assertTrue(parent.getChildren().isEmpty());
			populateReaggregationList.buildHierarchy(context,
					REAGG_JOB_NAME, null, parent, jobMap);
			assertEquals(2, parent.getChildren().size());
			assertTrue(parent.toString().contains(
					"Name: Perf_VOICE_L2_1_HOUR_ReaggregateJob, Retention: 5"));
			assertTrue(parent.toString().contains(
					"Name: Perf_VOICE_SEGG_1_DAY_ReaggregateJob, Retention: 15"));
		} catch (JobExecutionException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUpdateHierarchy() {
		JobRetention jr = new JobRetention(REAGG_JOB_NAME, 10, 123);
		PopulateReaggregationList populateReaggregationList = new PopulateReaggregationList();
		TreeNode<JobRetention> node1 = new TreeNode<>(jr);
		jr = new JobRetention("Child11", 2, 123);
		TreeNode<JobRetention> child11 = new TreeNode<>(jr);
		jr = new JobRetention("Child12", 8, 123);
		TreeNode<JobRetention> child12 = new TreeNode<>(jr);
		jr = new JobRetention("Child13", 13, 123);
		TreeNode<JobRetention> child13 = new TreeNode<>(jr);
		jr = new JobRetention("Child21", 5, 123);
		TreeNode<JobRetention> child21 = new TreeNode<>(jr);
		jr = new JobRetention("Child22", 12, 123);
		TreeNode<JobRetention> child22 = new TreeNode<>(jr);
		node1.addChild(child11);
		node1.addChild(child12);
		node1.addChild(child13);
		child11.addChild(child21);
		child11.addChild(child22);
		assertEquals(10, node1.getData().getRetention());
		assertEquals(2, child11.getData().getRetention());
		assertEquals(8, child12.getData().getRetention());
		populateReaggregationList.updateHierarchyRetention(node1);
		assertEquals(13, node1.getData().getRetention());
		assertEquals(12, child11.getData().getRetention());
		assertEquals(8, child12.getData().getRetention());
	}
}

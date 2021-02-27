
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;

public class JobDescriptorDecoratorTest {

	JobDescriptorDecorator decorator;

	private static final String OPERATION = "create";

	private static final String JOB_NAME = "Perf_VLR_1_DAY_AggregateJob";

	private static final String JOB_GROUP = "VLR_1";

	private static final String JOB_CLASS = "com.project.rithomas.jobexecution.JobExecutor";

	@Before
	public void setUp() throws Exception {
		decorator = new JobDescriptorDecorator();
	}

	@Test
	public void testCreateScheduleDefinitionListOfJob() {
		List<Job> jobDefinitions = new ArrayList<>();
		Job jobDefinition = new Job();
		jobDefinition.setName(JOB_NAME);
		jobDefinition.setJobGroup(JOB_GROUP);
		jobDefinition.setJobClass(JOB_CLASS);
		jobDefinitions.add(jobDefinition);
		Schedule expected = new Schedule();
		expected.setJobs(jobDefinitions);
		Schedule actual = decorator.createScheduleDefinition(jobDefinitions);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetDefinitionJobDescriptor() {
		JobDescriptor jobDescriptor = new JobDescriptor();
		jobDescriptor.setJobName(JOB_NAME);
		jobDescriptor.setJobGroup(JOB_GROUP);
		jobDescriptor.setOperation(OPERATION);
		Job expected = new Job();
		expected.setName(JOB_NAME);
		expected.setJobGroup(JOB_GROUP);
		Job actual = decorator.getDefinition(jobDescriptor);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetDefinitionStore() {
		assertThat(decorator.getDefinitionStore(), instanceOf(ArrayList.class));
	}
}

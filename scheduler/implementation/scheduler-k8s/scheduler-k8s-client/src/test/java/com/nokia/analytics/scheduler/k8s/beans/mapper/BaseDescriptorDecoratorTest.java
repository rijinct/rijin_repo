
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.rijin.analytics.scheduler.beans.Cron;
import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.CronJobDescriptor;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;

public class BaseDescriptorDecoratorTest {

	private static final String JOB_NAME = "Perf_VLR_1_DAY_AggregateJob";

	private static final String JOB_GROUP = "VLR_1";

	private static final String CRON_EXPRESSION = "0 0 * * *";

	BaseDescriptorDecorator<JobDescriptor, Job> jobsDecorator;

	BaseDescriptorDecorator<CronJobDescriptor, Trigger> triggersDecorator;

	@Before
	public void setUp() throws Exception {
		jobsDecorator = new JobDescriptorDecorator();
		triggersDecorator = new CronJobDescriptorDecorator();
	}

	@Test
	public void testToSchedulingDataJobs() {
		String operation = "create";
		List<JobDescriptor> jobDescriptors = new ArrayList<>();
		JobDescriptor jobDescriptor = new JobDescriptor();
		jobDescriptor.setJobName(JOB_NAME);
		jobDescriptor.setJobGroup(JOB_GROUP);
		jobDescriptor.setOperation(operation);
		jobDescriptors.add(jobDescriptor);
		JobSchedulingData expected = new JobSchedulingData();
		expected.setOperation(operation);
		Schedule schedule = new Schedule();
		List<Job> jobs = new ArrayList<>();
		Job job = new Job();
		job.setName(JOB_NAME);
		job.setJobGroup(JOB_GROUP);
		jobs.add(job);
		schedule.setJobs(jobs);
		expected.setSchedule(schedule);
		JobSchedulingData actual = jobsDecorator
				.toSchedulingData(jobDescriptors, operation);
		assertEquals(expected, actual);
	}

	@Test
	public void testToSchedulingDataTriggers() {
		String operation = "schedule";
		List<CronJobDescriptor> jobDescriptors = new ArrayList<>();
		CronJobDescriptor cronJobDescriptor = new CronJobDescriptor();
		cronJobDescriptor.setJobName(JOB_NAME);
		cronJobDescriptor.setJobGroup(JOB_GROUP);
		cronJobDescriptor.setCronExpression(CRON_EXPRESSION);
		cronJobDescriptor.setOperation(operation);
		jobDescriptors.add(cronJobDescriptor);
		JobSchedulingData expected = new JobSchedulingData();
		expected.setOperation(operation);
		Schedule schedule = new Schedule();
		List<Trigger> triggers = new ArrayList<>();
		Trigger trigger = new Trigger();
		Cron cron = new Cron();
		cron.setJobName(JOB_NAME);
		cron.setJobGroup(JOB_GROUP);
		cron.setCronExpression(CRON_EXPRESSION);
		trigger.setCron(cron);
		triggers.add(trigger);
		schedule.setTrigger(triggers);
		expected.setSchedule(schedule);
		JobSchedulingData actual = triggersDecorator
				.toSchedulingData(jobDescriptors, operation);
		assertEquals(expected, actual);
	}
}

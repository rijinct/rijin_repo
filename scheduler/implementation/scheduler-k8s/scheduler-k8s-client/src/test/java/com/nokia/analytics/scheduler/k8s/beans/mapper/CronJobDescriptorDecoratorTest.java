
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.rijin.analytics.scheduler.beans.Cron;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.CronJobDescriptor;

public class CronJobDescriptorDecoratorTest {

	CronJobDescriptorDecorator decorator;

	@Before
	public void setUp() throws Exception {
		decorator = new CronJobDescriptorDecorator();
	}

	@Test
	public void testGetDefinition() {
		String jobName = "Perf_VLR_1_DAY_AggregateJob";
		String jobGroup = "VLR_1";
		String operation = "SCHEDULE";
		String cronExpression = "0 0 * * *";
		CronJobDescriptor jobDescriptor = new CronJobDescriptor();
		jobDescriptor.setJobName(jobName);
		jobDescriptor.setJobGroup(jobGroup);
		jobDescriptor.setOperation(operation);
		jobDescriptor.setCronExpression(cronExpression);
		Trigger expected = new Trigger();
		Cron cron = new Cron();
		cron.setJobName(jobName);
		cron.setJobGroup(jobGroup);
		cron.setCronExpression(cronExpression);
		expected.setCron(cron);
		Trigger actual = decorator.getDefinition(jobDescriptor);
		assertEquals(expected, actual);
	}
}

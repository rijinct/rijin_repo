
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.rijin.analytics.scheduler.beans.Cron;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.CronJobDescriptor;

public class TriggerDescriptorDecoratorTest {

	private static final String JOB_NAME = "Perf_VLR_1_DAY_AggregateJob";

	private static final String JOB_GROUP = "VLR_1";

	private static final String CRON_EXPRESSION = "0 0 * * *";

	TriggerDescriptorDecorator<CronJobDescriptor> decorator;

	@Before
	public void setUp() throws Exception {
		decorator = new CronJobDescriptorDecorator();
	}

	@Test
	public void testInitializeTriggerDefinitions() {
		decorator.initializeTriggerDefinitions();
		assertThat(decorator.triggerDefinitions, instanceOf(ArrayList.class));
	}

	@Test
	public void testInitializeTriggerDefinitionsPreinitialized() {
		decorator.triggerDefinitions = new ArrayList<Trigger>();
		decorator.initializeTriggerDefinitions();
		assertThat(decorator.triggerDefinitions, instanceOf(ArrayList.class));
	}

	@Test
	public void testCreateScheduleDefinitionListOfTrigger() {
		List<Trigger> triggerDefinitions = new ArrayList<>();
		Trigger trigger = new Trigger();
		Cron cron = new Cron();
		cron.setJobName(JOB_NAME);
		cron.setJobGroup(JOB_GROUP);
		cron.setCronExpression(CRON_EXPRESSION);
		trigger.setCron(cron);
		triggerDefinitions.add(trigger);
		Schedule expected = new Schedule();
		expected.setTrigger(triggerDefinitions);
		Schedule actual = decorator
				.createScheduleDefinition(triggerDefinitions);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetDefinitionStore() {
		assertThat(decorator.getDefinitionStore(), instanceOf(ArrayList.class));
	}
}


package com.rijin.analytics.scheduler.k8s.beans.mapper;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.SimpleJobDescriptor;

public class SimpleJobDescriptorDecoratorTest {

	SimpleJobDescriptorDecorator decorator;

	@Before
	public void setUp() throws Exception {
		decorator = new SimpleJobDescriptorDecorator();
	}

	@Test
	public void testGetDefinitionSimpleJobDescriptor() {
		String jobName = "Perf_VLR_1_DAY_AggregateJob";
		String jobGroup = "VLR_1";
		String operation = "SCHEDULE";
		String startTime = "2012.11.02 11:15:00";
		SimpleJobDescriptor jobDescriptor = new SimpleJobDescriptor();
		jobDescriptor.setJobName(jobName);
		jobDescriptor.setJobGroup(jobGroup);
		jobDescriptor.setOperation(operation);
		jobDescriptor.setStartTime(startTime);
		Trigger expected = new Trigger();
		Simple simple = new Simple();
		simple.setJobName(jobName);
		simple.setJobGroup(jobGroup);
		simple.setStartTime(startTime);
		expected.setSimple(simple);
		Trigger actual = decorator.getDefinition(jobDescriptor);
		assertEquals(expected, actual);
	}
}

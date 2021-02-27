
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import java.util.Iterator;
import java.util.List;

import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;

public abstract class BaseDescriptorDecorator<I extends JobDescriptor, O> {

	public JobSchedulingData toSchedulingData(List<I> jobDescriptors,
			String operation) {
		JobSchedulingData schedulingData = new JobSchedulingData();
		schedulingData.setOperation(operation);
		Iterator<I> availableDescriptors = jobDescriptors.iterator();
		List<O> jobDefinitions = getDefinitionStore();
		while (availableDescriptors.hasNext()) {
			I descriptor = availableDescriptors.next();
			O definition = getDefinition(descriptor);
			jobDefinitions.add(definition);
		}
		schedulingData.setSchedule(createScheduleDefinition(jobDefinitions));
		return schedulingData;
	}

	protected abstract Schedule createScheduleDefinition(
			List<O> jobDefinitions);

	protected abstract O getDefinition(I jobDescriptor);

	protected abstract List<O> getDefinitionStore();
}

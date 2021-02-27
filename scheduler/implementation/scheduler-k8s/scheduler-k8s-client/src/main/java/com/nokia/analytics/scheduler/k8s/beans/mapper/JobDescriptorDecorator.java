
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.scheduler.beans.Job;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobDescriptorDecorator
		extends BaseDescriptorDecorator<JobDescriptor, Job> {

	@Override
	protected Schedule createScheduleDefinition(List<Job> jobDefinitions) {
		Schedule schedule = new Schedule();
		schedule.setJobs(jobDefinitions);
		return schedule;
	}

	@Override
	protected Job getDefinition(JobDescriptor jobDescriptor) {
		Job jobDefinition = new Job();
		jobDefinition.setName(jobDescriptor.getJobName());
		jobDefinition.setJobGroup(jobDescriptor.getJobGroup());
		return jobDefinition;
	}

	@Override
	protected List<Job> getDefinitionStore() {
		return new ArrayList<Job>();
	}
}

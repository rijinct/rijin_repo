
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.SimpleJobDescriptor;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SimpleJobDescriptorDecorator
		extends TriggerDescriptorDecorator<SimpleJobDescriptor> {

	@Override
	protected Trigger getDefinition(SimpleJobDescriptor jobDescriptor) {
		Trigger triggerDefinition = new Trigger();
		Simple simpleJobDefinition = new Simple();
		simpleJobDefinition.setJobName(jobDescriptor.getJobName());
		simpleJobDefinition.setJobGroup(jobDescriptor.getJobGroup());
		simpleJobDefinition.setStartTime(jobDescriptor.getStartTime());
		triggerDefinition.setSimple(simpleJobDefinition);
		return triggerDefinition;
	}
}

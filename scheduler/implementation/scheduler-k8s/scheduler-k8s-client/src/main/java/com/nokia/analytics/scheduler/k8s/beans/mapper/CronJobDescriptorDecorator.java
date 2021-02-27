
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.scheduler.beans.Cron;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.CronJobDescriptor;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CronJobDescriptorDecorator
		extends TriggerDescriptorDecorator<CronJobDescriptor> {

	@Override
	protected Trigger getDefinition(CronJobDescriptor jobDescriptor) {
		Trigger triggerDefinition = new Trigger();
		Cron cronJobDefinition = new Cron();
		cronJobDefinition.setJobName(jobDescriptor.getJobName());
		cronJobDefinition.setJobGroup(jobDescriptor.getJobGroup());
		cronJobDefinition.setCronExpression(jobDescriptor.getCronExpression());
		triggerDefinition.setCron(cronJobDefinition);
		return triggerDefinition;
	}
}

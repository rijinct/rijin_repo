
package com.rijin.analytics.scheduler.k8s.beans.mapper;

import java.util.ArrayList;
import java.util.List;

import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;

public abstract class TriggerDescriptorDecorator<T extends JobDescriptor>
		extends BaseDescriptorDecorator<T, Trigger> {

	protected List<Trigger> triggerDefinitions = null;

	protected void initializeTriggerDefinitions() {
		if (triggerDefinitions == null) {
			triggerDefinitions = new ArrayList<Trigger>();
		}
	}

	@Override
	protected Schedule createScheduleDefinition(
			List<Trigger> triggerDefinitions) {
		Schedule schedule = new Schedule();
		schedule.setTrigger(triggerDefinitions);
		return schedule;
	}

	@Override
	protected List<Trigger> getDefinitionStore() {
		return new ArrayList<Trigger>();
	}
}

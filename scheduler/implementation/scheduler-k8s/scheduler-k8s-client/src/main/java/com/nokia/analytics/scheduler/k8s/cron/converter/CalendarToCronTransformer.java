
package com.rijin.analytics.scheduler.k8s.cron.converter;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalendarToCronTransformer extends BaseCronTransformer {

	@Override
	protected void transform() {
		String updatedValue = String
				.valueOf(calendarInstance.get(calendarField));
		LOGGER.debug(
				"Updating cron field at position {} with {}, using calendar field {}",
				cronFieldPosition, updatedValue, calendarField);
		cronParts[cronFieldPosition] = updatedValue;
	}
}

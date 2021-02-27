
package com.rijin.analytics.scheduler.k8s.cron.converter;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CronToCalendarTransformer extends BaseCronTransformer {

	@Override
	protected void transform() {
		LOGGER.debug(
				"Setting field '{}' from cron found at position '{}' to calendar field '{}'",
				cronFieldValue, cronFieldPosition, calendarField);
		calendarInstance.set(calendarField, cronFieldValue);
	}
}

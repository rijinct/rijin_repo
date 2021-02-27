
package com.rijin.analytics.scheduler.k8s.cron.converter;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

import lombok.Setter;

public class CronConverter {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronConverter.class);

	private static final String CRON_FIELDS_SEPARATOR = " ";

	private String[] cronParts;

	private Calendar fromCalendar;

	private String sourceCron;

	private ZoneId sourceZoneId;

	private ZoneId targetZoneId;

	@Setter
	CronToCalendarTransformer toCalendarConverter = new CronToCalendarTransformer();

	@Setter
	CalendarToCronTransformer toCronConverter = new CalendarToCronTransformer();

	public CronConverter using(String cronExpression) {
		this.sourceCron = cronExpression;
		cronParts = cronExpression.split(CRON_FIELDS_SEPARATOR);
		LOGGER.debug("Cron '{}' split into {}", cronExpression, cronParts);
		return this;
	}

	public CronConverter from(ZoneId zoneId) {
		sourceZoneId = zoneId;
		fromCalendar = getCalendar(zoneId);
		toCalendarConverter.apply(cronParts, fromCalendar);
		LOGGER.debug("Calendar object built using cron :{} and zoneID {} => {}",
				cronParts, zoneId, fromCalendar);
		return this;
	}

	public CronConverter to(ZoneId zoneId) {
		targetZoneId = zoneId;
		Calendar toCalendar = getCalendar(zoneId);
		toCalendar.setTimeInMillis(fromCalendar.getTimeInMillis());
		LOGGER.debug(
				"Calendar object built from calendar {} and zoneID {} => {}",
				fromCalendar, zoneId, toCalendar);
		toCronConverter.apply(cronParts, toCalendar);
		LOGGER.debug("cron after applying calendar {} => {}", toCalendar,
				cronParts);
		return this;
	}

	public String convert() {
		String targetCron = StringUtils.join(cronParts, CRON_FIELDS_SEPARATOR);
		LOGGER.info("Converted CRON -- {} :[{}] => {} :[{}]", sourceZoneId,
				sourceCron, targetZoneId, targetCron);
		return targetCron;
	}

	private Calendar getCalendar(ZoneId id) {
		return Calendar.getInstance(TimeZone.getTimeZone(id));
	}
}

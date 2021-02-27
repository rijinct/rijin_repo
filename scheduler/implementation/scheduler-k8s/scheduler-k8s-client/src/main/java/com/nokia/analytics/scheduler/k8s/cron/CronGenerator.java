
package com.rijin.analytics.scheduler.k8s.cron;

import static com.cronutils.model.field.expression.FieldExpressionFactory.on;
import static org.apache.commons.lang.StringUtils.isEmpty;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.cronutils.builder.CronBuilder;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class CronGenerator {

	private static final int INCREMENT_MINS = 2;

	private static final String START_TIME_FORMATTER_PATTERN = "yyyy.MM.dd HH:mm:ss";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronGenerator.class);

	public String getCronExpression(LocalDateTime dateTime) {
		CronDefinition cronDefinition = CronDefinitionBuilder
				.instanceDefinitionFor(CronType.UNIX);
		Cron cron = CronBuilder.cron(cronDefinition)
				.withMinute(on(dateTime.getMinute()))
				.withHour(on(dateTime.getHour()))
				.withDoM(on(dateTime.getDayOfMonth()))
				.withMonth(on(dateTime.getMonth().getValue()))
				.withDoW(on(dateTime.getDayOfWeek().getValue() % 7)).instance();
		LOGGER.debug("Cron expression : {}", cron);
		return cron.asString();
	}

	public String getCron(String dateTime) {
		return getCronExpression(getTime(dateTime));
	}

	LocalDateTime getTime() {
		Instant instant = Instant.now();
		LOGGER.debug("Current time : {}", instant);
		instant = instant.plus(Duration.ofMinutes(INCREMENT_MINS));
		LOGGER.debug("current time + {} min : {}", INCREMENT_MINS, instant);
		LocalDateTime timeInSystemZone = LocalDateTime.ofInstant(instant,
				ZoneOffset.UTC);
		LOGGER.debug("current time + {} min in UTC:  {}", INCREMENT_MINS,
				timeInSystemZone);
		return timeInSystemZone;
	}

	public LocalDateTime getTime(String dateTime) {
		if (isEmpty(dateTime)) {
			return getTime();
		}
		DateTimeFormatter formatter = DateTimeFormatter
				.ofPattern(START_TIME_FORMATTER_PATTERN);
		ZonedDateTime zonedTime = LocalDateTime.parse(dateTime, formatter)
				.atZone(ZoneId.systemDefault());
		ZonedDateTime utcZoned = zonedTime.withZoneSameInstant(ZoneOffset.UTC);
		return utcZoned.toLocalDateTime();
	}
}

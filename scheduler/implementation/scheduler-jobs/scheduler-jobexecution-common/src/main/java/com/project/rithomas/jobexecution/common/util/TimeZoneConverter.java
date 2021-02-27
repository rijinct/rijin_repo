
package com.project.rithomas.jobexecution.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeZoneConverter {

	public static final String UTC = "UTC";

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss");

	private LocalDateTime localDateTime;

	private ZonedDateTime targetDateTime;

	public TimeZoneConverter dateTime(String dateTime) {
		localDateTime = LocalDateTime.parse(dateTime, FORMATTER);
		return this;
	}

	public TimeZoneConverter from(String zone) {
		targetDateTime = ZonedDateTime.of(localDateTime, ZoneId.of(zone));
		return this;
	}

	public TimeZoneConverter to(String zone) {
		targetDateTime = ZonedDateTime.ofInstant(targetDateTime.toInstant(),
				ZoneId.of(zone));
		return this;
	}

	public long epoch() {
		return targetDateTime.toInstant().toEpochMilli();
	}

	public String convert() {
		return targetDateTime.format(FORMATTER);
	}
	
	public ZonedDateTime value() {
		return targetDateTime;
	}
}

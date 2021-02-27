
package com.rijin.analytics.scheduler.k8s.cron;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rijin.analytics.scheduler.k8s.cron.converter.CalendarToCronTransformer;
import com.rijin.analytics.scheduler.k8s.cron.converter.CronConverter;
import com.rijin.analytics.scheduler.k8s.cron.converter.CronToCalendarTransformer;

@RunWith(Parameterized.class)
public class CronConverterTest {

	CronConverter cronConverter = spy(new CronConverter());

	private String timezone;

	private String inputCronExpression;

	private String expectedCronExpression;

	public CronConverterTest(String timezone, String inputCronExpression,
			String expectedCronExpression) {
		this.timezone = timezone;
		this.inputCronExpression = inputCronExpression;
		this.expectedCronExpression = expectedCronExpression;
	}

	@Parameterized.Parameters
	public static Collection cronExpressions() {
		return Arrays.asList(new Object[][] {
				{ "Pacific/Pago_Pago", "15 * * * *", "15 * * * *" },
				{ "Antarctica/Casey", "? * * * *", "? * * * *" },
				{ "Antarctica/Troll", "45 * * * *", "45 * * * *" },
				{ "Pacific/Chatham", "15 * * * *", "30 * * * *" },
				{ "Asia/Colombo", "45 * * ? *", "15 * * ? *" },
				{ "Asia/Colombo", "0/45 * * ? *", "0/45 * * ? *" },
				{ "Australia/Eucla", "13 * * ? *", "28 * * ? *" },
				{ "America/St_Johns", "0 0/15 * * * ?", "30 0/15 * * * ?" },
				{ "America/St_Johns", "0 8 10 3 ?", "30 10 10 3 ?" },
				{ "America/St_Johns", "0 8 5 11 ?", "30 11 5 11 ?" },
				{ "America/St_Johns", "0 0/1 * * ?", "30 0/1 * * ?" },
				{ "America/St_Johns", "20 0 10 3 ?", "50 2 10 3 ?" },
				{ "America/St_Johns", "20 0 5 11 ?", "50 3 5 11 ?" },
				{ "Asia/Kolkata", "20 0 * * ?", "50 18 * * ?" }, });
	}

	@Test
	public void testCronConverterBuilder() {
		cronConverter.setToCalendarConverter(new CronToCalendarTransformer());
		cronConverter.setToCronConverter(new CalendarToCronTransformer());
		assertEquals(expectedCronExpression,
				cronConverter.using(inputCronExpression)
						.from(ZoneId.of(timezone)).to(ZoneId.of("UTC"))
						.convert());
	}
}

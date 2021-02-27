
package com.rijin.analytics.scheduler.k8s.cron;

import static org.junit.Assert.assertEquals;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;

import org.exparity.hamcrest.date.LocalDateTimeMatchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CronGeneratorTest {

	private CronGenerator generator = new CronGenerator();
	
	@Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
        	     { "2018.12.31 23:59:59", "29 18 31 12 1" }, //Last second of year (Localtime)
        	     { "2019.01.01 00:00:00", "30 18 31 12 1" }, //First second of new year (Localtime)
        	     { "2019.01.01 05:30:00", "0 0 1 1 2" }, //First second of new year (UTC)
        	     { "2019.01.26 11:15:00", "45 5 26 1 6" }, //Saturday 
        	     { "2019.01.27 00:00:00", "30 18 26 1 6" }, //First second of sunday (Localtime)
        	     { "2019.01.27 05:30:00", "0 0 27 1 0" }, //First second of sunday (UTC)
        	     { "2019.01.27 11:15:00", "45 5 27 1 0" }, //Sunday
        	     { "2019.01.27 23:59:59", "29 18 27 1 0" }, //Last second of Sunday (Localtime)
                 { "2019.01.28 05:30:00", "0 0 28 1 1" }, //First second of Monday (UTC)
                 { "2019.01.28 11:15:00", "45 5 28 1 1" }, //Monday
                 { "2019.02.30 11:15:00", "45 5 28 2 4" }, //Invalid Feb-30
                 { "2019.09.31 11:15:00", "45 5 30 9 1" }, //Invalid Sep-31
           });
    }
    
	@Parameter
	public String timeInput;

	@Parameter(1)
	public String expectedCron;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGetCron() {
		assertEquals(expectedCron, generator.getCron(timeInput));
	}
	
	@Test
	public void testGetCronExpression() {
		String str = "2018.07.12 11:15:00";
		DateTimeFormatter formatter = DateTimeFormatter
				.ofPattern("yyyy.MM.dd HH:mm:ss");
		LocalDateTime dateTime = LocalDateTime.parse(str, formatter);
		assertEquals("15 11 12 7 4", generator.getCronExpression(dateTime));
	}

	@Ignore
	public void testGetTime() {
		LocalDateTime expectedDateTime = LocalDateTime.now(Clock.systemUTC());
		assertEquals(LocalDateTimeMatchers.sameInstant(expectedDateTime),
				generator.getTime());
	}

	@Test
	public void testGetTimeString() {
		assertEquals("2018-07-12T05:45",
				generator.getTime("2018.07.12 11:15:00").toString());
	}
}

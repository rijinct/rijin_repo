
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TimeZoneConverterTest {

	TimeZoneConverter converter;

	private String sourceDateTime;

	private String srcZone;

	private String targetZone;

	private String expected;

	private long epoch;

	private String targetDateTime;

	public TimeZoneConverterTest(String dateTime, String srcZone,
			String targetZone, String expected, long epoch,
			String targetDateTime) {
		this.sourceDateTime = dateTime;
		this.srcZone = srcZone;
		this.targetZone = targetZone;
		this.expected = expected;
		this.epoch = epoch;
		this.targetDateTime = targetDateTime;
	}
	
	@Parameterized.Parameters
	public static Collection cronExpressions() {
		return Arrays.asList(new Object[][] {
				{"2019-06-17 18:45:46", "Asia/Calcutta", "UTC", "2019-06-17 13:15:46", 1560777346000l, "2019-06-17T13:15:46Z[UTC]" },
				{"2019-06-17 13:15:46", "UTC", "Asia/Calcutta", "2019-06-17 18:45:46", 1560777346000l, "2019-06-17T18:45:46+05:30[Asia/Calcutta]"},
				{"2019-06-17 13:15:46", "UTC", "America/Montreal", "2019-06-17 09:15:46", 1560777346000l, "2019-06-17T09:15:46-04:00[America/Montreal]"},
				
				{"2019-03-10 01:59:00", "America/New_York", "UTC", "2019-03-10 06:59:00", 1552201140000l, "2019-03-10T06:59Z[UTC]"},
				{"2019-03-10 02:00:00", "America/New_York", "UTC", "2019-03-10 07:00:00", 1552201200000l, "2019-03-10T07:00Z[UTC]"},
				{"2019-03-10 02:01:00", "America/New_York", "UTC", "2019-03-10 07:01:00", 1552201260000l, "2019-03-10T07:01Z[UTC]"},
				{"2019-03-10 06:59:00", "UTC", "America/New_York", "2019-03-10 01:59:00", 1552201140000l, "2019-03-10T01:59-05:00[America/New_York]"},
				{"2019-03-10 07:00:00", "UTC", "America/New_York", "2019-03-10 03:00:00", 1552201200000l, "2019-03-10T03:00-04:00[America/New_York]"},
				{"2019-03-10 07:01:00", "UTC", "America/New_York", "2019-03-10 03:01:00", 1552201260000l, "2019-03-10T03:01-04:00[America/New_York]"},
				
				{"2019-10-27 03:59:00", "Europe/Tallinn", "UTC", "2019-10-27 00:59:00", 1572137940000l, "2019-10-27T00:59Z[UTC]"},
				{"2019-10-27 04:00:00", "Europe/Tallinn", "UTC", "2019-10-27 02:00:00", 1572141600000l, "2019-10-27T02:00Z[UTC]"},
				{"2019-10-27 04:01:00", "Europe/Tallinn", "UTC", "2019-10-27 02:01:00", 1572141660000l, "2019-10-27T02:01Z[UTC]"},
				{"2019-10-27 00:59:00", "UTC", "Europe/Tallinn", "2019-10-27 03:59:00", 1572137940000l, "2019-10-27T03:59+03:00[Europe/Tallinn]"},
				{"2019-10-27 01:00:00", "UTC", "Europe/Tallinn", "2019-10-27 03:00:00", 1572138000000l, "2019-10-27T03:00+02:00[Europe/Tallinn]"},
				{"2019-10-27 01:01:00", "UTC", "Europe/Tallinn", "2019-10-27 03:01:00", 1572138060000l, "2019-10-27T03:01+02:00[Europe/Tallinn]"}});
		}

	@Before
	public void setUp() throws Exception {
		converter = new TimeZoneConverter();
	}

	@Test
	public void testConverterUsingDateTime() {
		assertEquals(expected, converter.dateTime(sourceDateTime).from(srcZone)
				.to(targetZone).convert());
		assertEquals(epoch, converter.epoch());
		assertEquals(targetDateTime, converter.value().toString());
	}

}

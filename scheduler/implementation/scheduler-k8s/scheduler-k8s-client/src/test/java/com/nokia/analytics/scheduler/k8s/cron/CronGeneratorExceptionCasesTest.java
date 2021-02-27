
package com.rijin.analytics.scheduler.k8s.cron;

import static org.junit.Assert.*;

import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CronGeneratorExceptionCasesTest {

	private CronGenerator generator = new CronGenerator();
	
	@Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 { "2019.13.31 11:15:00", DateTimeParseException.class }, //Invalid month
                 { "2019.00.31 11:15:00", DateTimeParseException.class }, //Invalid month
                 { "2019.01.27 24:59:59", DateTimeParseException.class }, //Invalid hour
                 { "2019.01.27 23:69:59", DateTimeParseException.class }, //Invalid minute
                 { "2019.01.27 23:59:69", DateTimeParseException.class }, //Invalid second
                 { "20190.01.27 23:59:59", DateTimeParseException.class }, //Invalid year
           });
    }
    
	@Parameter
	public String timeInput;

	@Parameter(1)
	public Class<? extends Exception> expectedException;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGetCron() {
		if (expectedException != null) {
			thrown.expect(expectedException);
		}
		assertEquals(null, generator.getCron(timeInput));
	}
}

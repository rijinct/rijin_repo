
package com.project.rithomas.jobexecution.common.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class HiveQueryExceptionTest {

	@Test
	public void testHiveQueryExceptionString() {
		HiveQueryException hiveQueryException = new HiveQueryException(
				"Test Exception");
		assertEquals(
				"com.project.rithomas.jobexecution.common.exception.HiveQueryException: Test Exception",
				hiveQueryException.toString());
		assertEquals(null, hiveQueryException.getCause());
		assertEquals("Test Exception", hiveQueryException.getMessage());
		assertEquals("Test Exception", hiveQueryException.getLocalizedMessage());
		assertNotNull(hiveQueryException.getStackTrace());
	}

	@Test
	public void testHiveQueryExceptionStringThrowable() throws Throwable {
		Throwable throwable = new Throwable("detailMessage0", (Throwable) null);
		HiveQueryException hiveQueryException = new HiveQueryException(
				"Test Exception", throwable);
		assertEquals(
				"com.project.rithomas.jobexecution.common.exception.HiveQueryException: Test Exception",
				hiveQueryException.toString());
		assertEquals(throwable, hiveQueryException.getCause());
		assertEquals("Test Exception", hiveQueryException.getMessage());
		assertEquals("Test Exception", hiveQueryException.getLocalizedMessage());
		assertNotNull(hiveQueryException.getStackTrace());
	}
}

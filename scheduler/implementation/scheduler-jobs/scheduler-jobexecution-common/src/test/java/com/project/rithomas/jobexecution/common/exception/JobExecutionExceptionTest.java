
package com.project.rithomas.jobexecution.common.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class JobExecutionExceptionTest {

	@Test
	public void testJobExecutionExceptionStringWithMsg() {
		JobExecutionException jobExecutionException = new JobExecutionException(
				"Test Exception");
		assertEquals(
				"com.project.rithomas.jobexecution.common.exception.JobExecutionException: Test Exception",
				jobExecutionException.toString());
		assertEquals(null, jobExecutionException.getCause());
		assertEquals("Test Exception", jobExecutionException.getMessage());
		assertEquals("Test Exception",
				jobExecutionException.getLocalizedMessage());
		assertNotNull(jobExecutionException.getStackTrace());
	}

	@Test
	public void testJobExecutionExceptionStringWithNull() {
		JobExecutionException jobExecutionException = new JobExecutionException(
				null);
		assertEquals(
				"com.project.rithomas.jobexecution.common.exception.JobExecutionException",
				jobExecutionException.toString());
		assertEquals(null, jobExecutionException.getCause());
		assertEquals(null, jobExecutionException.getMessage());
		assertEquals(null, jobExecutionException.getLocalizedMessage());
		assertNotNull(jobExecutionException.getStackTrace());
	}

	@Test
	public void testJobExecutionExceptionStringThrowableWithInput() {
		Throwable throwable = new Throwable("detailMessage0", (Throwable) null);
		JobExecutionException jobExecutionException = new JobExecutionException(
				"Test Exception", throwable);
		assertEquals(
				"com.project.rithomas.jobexecution.common.exception.JobExecutionException: Test Exception",
				jobExecutionException.toString());
		assertEquals(throwable, jobExecutionException.getCause());
		assertEquals("Test Exception", jobExecutionException.getMessage());
		assertEquals("Test Exception",
				jobExecutionException.getLocalizedMessage());
		assertNotNull(jobExecutionException.getStackTrace());
	}
}

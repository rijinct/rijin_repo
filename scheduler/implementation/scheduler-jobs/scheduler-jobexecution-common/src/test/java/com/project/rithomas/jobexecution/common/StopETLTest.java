
package com.project.rithomas.jobexecution.common;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { StopETL.class })
public class StopETLTest {

	StopETL stopETL = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	UpdateJobStatus updateJobStatus;

	@Before
	public void setUp() throws Exception {
		stopETL = new StopETL();
	}

	@Test
	public void testUpdateFinalJobStatus() throws Exception {
		// mocking new object call
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		context.setProperty(JobExecutionContext.AGGREGATIONDONEDATE,
				"2012-12-03 00:00:00");
		boolean success = stopETL.execute(context);
		assertEquals(true, success);
	}
}

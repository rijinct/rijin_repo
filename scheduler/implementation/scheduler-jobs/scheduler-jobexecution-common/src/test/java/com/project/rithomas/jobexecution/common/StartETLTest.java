
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
@PrepareForTest(value = { StartETL.class })
public class StartETLTest {

	StartETL startEtl = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	UpdateJobStatus updateJobStatus;

	@Before
	public void setUp() throws Exception {
		startEtl = new StartETL();
	}

	@Test
	public void testInsertETLStatus() throws Exception {
		// mocking new object call
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		boolean success = startEtl.execute(context);
		assertEquals(true, success);
	}
}

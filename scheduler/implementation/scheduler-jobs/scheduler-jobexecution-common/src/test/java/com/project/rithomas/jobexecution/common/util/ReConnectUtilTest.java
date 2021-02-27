
package com.project.rithomas.jobexecution.common.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ReConnectUtil.class })
public class ReConnectUtilTest {

	ReConnectUtil reconnectUtil;

	@Before
	public void setup() {
		reconnectUtil = new ReConnectUtil();
	}

	@Test
	public void updateRetryAttemptsFirst() throws JobExecutionException {
		reconnectUtil = new ReConnectUtil(2, 3000);
		reconnectUtil.updateRetryAttempts();
	}

	@Test(expected = JobExecutionException.class)
	public void updateRetryAttemptsThrowException()
			throws JobExecutionException {
		reconnectUtil = new ReConnectUtil(1, 3000);
		reconnectUtil.updateRetryAttempts();
	}
}

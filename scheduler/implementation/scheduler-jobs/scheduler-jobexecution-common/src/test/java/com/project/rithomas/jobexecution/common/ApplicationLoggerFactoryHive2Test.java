
package com.project.rithomas.jobexecution.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


import junit.framework.Assert;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {})
public class ApplicationLoggerFactoryHive2Test {


	@Before
	public void setup() throws Exception {
	}

	@Test
	public void getInstanceHiverServer2() throws Exception {
		ApplicationLoggerUtilInterface applicationLoggerUtilInterface = ApplicationLoggerFactory
				.getApplicationLogger();
		Assert.assertTrue(applicationLoggerUtilInterface instanceof ApplicationIdLoggerHiveServer2Util);
	}
}

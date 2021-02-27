
package com.project.rithomas.jobexecution.core;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import com.project.rithomas.jobexecution.exception.JobExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { schedulerSparkLauncher.class, State.class })
public class schedulerSparkLauncherTest {

	schedulerSparkLauncher launcher = new schedulerSparkLauncher();

	@Mock
	SparkLauncher sparkLauncher;

	@Mock
	ProtectionDomain mockProtectionDomain;

	@Mock
	CodeSource mockCodeSource;

	@Mock
	SparkAppHandle mockSparkAppHandle;

	@Mock
	State mockState;

	@Mock
	File file;

	@Mock
	URL mockURL;

	List<String> sparkSetting = new ArrayList<String>();
	{
		sparkSetting.add("SET spark.dynamicAllocation.enabled=true");
		sparkSetting.add("SET spark.master=yarn");
		sparkSetting.add("SET spark.shuffle.service.enabled=true");
		sparkSetting.add("select * from tab");
	}

	@Before
	public void setUp() throws Exception {
		PowerMockito.whenNew(SparkLauncher.class).withNoArguments()
				.thenReturn(sparkLauncher);
		mockStatic(File.class);
		PowerMockito.whenNew(File.class).withArguments(Mockito.anyString())
				.thenReturn(file);
		Mockito.when(file.listFiles()).thenReturn(new File[] { file });
		mockStatic(schedulerSparkLauncher.class);
		PowerMockito.when(schedulerSparkLauncher.class.getProtectionDomain())
				.thenReturn(mockProtectionDomain);
		PowerMockito.when(mockProtectionDomain.getCodeSource())
				.thenReturn(mockCodeSource);
		PowerMockito.when(mockCodeSource.getLocation()).thenReturn(mockURL);
		PowerMockito.when(mockURL.toString()).thenReturn("http://localhost/");
		mockStatic(SparkLauncher.class);
		PowerMockito.when(sparkLauncher.setAppResource(Mockito.anyString()))
				.thenReturn(sparkLauncher);
		PowerMockito.when(sparkLauncher.setMainClass(Mockito.anyString()))
				.thenReturn(sparkLauncher);
		PowerMockito.when(sparkLauncher.setMaster(Mockito.anyString()))
				.thenReturn(sparkLauncher);
		PowerMockito.when(sparkLauncher.setDeployMode(Mockito.anyString()))
				.thenReturn(sparkLauncher);
		PowerMockito.when(sparkLauncher.setSparkHome(Mockito.anyString()))
				.thenReturn(sparkLauncher);
		String[] arguments = sparkSetting.toArray(new String[0]);
		PowerMockito.when(sparkLauncher.addAppArgs(arguments))
				.thenReturn(sparkLauncher);
		PowerMockito.when(sparkLauncher.setConf("spark.query.result.dir", ""))
				.thenReturn(sparkLauncher);
		mockStatic(SparkAppHandle.class);
		PowerMockito.when(sparkLauncher.startApplication())
				.thenReturn(mockSparkAppHandle);
		mockStatic(State.class);
		PowerMockito.when(mockState.isFinal()).thenReturn(true)
				.thenReturn(false);
	}

	@Test
	public void testLaunchSparkSubmitJob() throws JobExecutionException {
		PowerMockito.when(mockSparkAppHandle.getState())
				.thenReturn(State.FINISHED);
		assertTrue(launcher.launchSparkSubmitJob(sparkSetting, null));
		PowerMockito.when(mockSparkAppHandle.getState())
				.thenReturn(State.FAILED);
		assertFalse(launcher.launchSparkSubmitJob(sparkSetting, null));
	}
}

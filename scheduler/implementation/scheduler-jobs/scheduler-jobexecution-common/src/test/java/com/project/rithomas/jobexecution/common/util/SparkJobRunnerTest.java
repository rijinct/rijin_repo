package com.project.rithomas.jobexecution.common.util;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.schedulerSparkLauncher;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { SparkJobRunner.class,JobExecutionUtil.class })
public class SparkJobRunnerTest {

	@Mock
	private schedulerSparkLauncher sparkLauncher;

	private SparkJobRunner sparkJobRunner;

	@Before
	public void setUpBeforeClass() throws Exception {
		sparkJobRunner = new SparkJobRunner();
		sparkJobRunner.setQueryHint("SET MAPRED.MEMORY='true'");
		whenNew(schedulerSparkLauncher.class).withNoArguments().thenReturn(sparkLauncher);
	}

	@Test
	public void testRunQuery_WithQueryAsParameter_AndExecutionSuccess() throws JobExecutionException  {
		doReturn(true).when(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(), Mockito.anyObject());
		sparkJobRunner.runQuery("QUERY");
		verify(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(), Mockito.anyObject());
	}

	@Test
	public void testRunQuery_WithQueryAsParameter_AndExecutionFail()  {
		try {
			doReturn(false).when(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(),
					Mockito.anyObject());
			sparkJobRunner.runQuery("QUERY");
			Mockito.verify(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(), Mockito.anyObject());
		} catch (JobExecutionException e) {
			Assert.assertTrue(true);
		}
	}
	
	@Test
	public void testRunQuery_WithParameters_AndExecutionSuccess() throws JobExecutionException  {
		doReturn(true).when(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(), Mockito.anyObject());
		mockStatic(JobExecutionUtil.class);
		when(JobExecutionUtil.readSparkResultFile(Mockito.anyObject(), Mockito.anyString())).thenReturn(Collections.EMPTY_LIST);
		sparkJobRunner.runQuery("QUERY","",null);
		verify(sparkLauncher).launchSparkSubmitJob(Mockito.anyList(), Mockito.anyObject());
	}

}


package com.project.rithomas.jobexecution.usage;

import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.AutoTriggerJobs;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { AutoTriggerJobs.class, LoopBack.class })
public class LoopBackTest {

	WorkFlowContext context = new JobExecutionContext();

	Calendar calendar;

	private String usageRetriggerDuration;

	private Date startDate;

	private String jobName;

	@Mock
	AutoTriggerJobs autoTriggerObject;

	@Before
	public void setup() throws Exception {
		autoTriggerObject = PowerMockito.mock(AutoTriggerJobs.class);
		PowerMockito.whenNew(AutoTriggerJobs.class).withNoArguments()
				.thenReturn(autoTriggerObject);
		usageRetriggerDuration = "1";
		jobName = "Usage_VOICE_1_LoadJob";
		calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR, -2);
		startDate = calendar.getTime();
		context.setProperty(JobExecutionContext.CONTINUOUS_LOADING, "TRUE");
		context.setProperty(JobExecutionContext.USAGE_RETRIGGER_DURATION,
				usageRetriggerDuration);
		context.setProperty(
				JobExecutionContext.JOB_START_DATE,
				DateFunctionTransformation.getInstance().getFormattedDate(
						startDate));
		context.setProperty(JobExecutionContext.JOB_NAME, jobName);
	}

	@Test
	public void testContinuousJobAutoTriggerDateChange() throws Exception {
		LoopBack loopBack = new LoopBack();
		PowerMockito.when(autoTriggerObject.execute(context)).thenReturn(true);
		assertTrue(loopBack.execute(context));
	}

	@Test
	public void testContinuousJobAutoTriggerWithoutDateChange()
			throws Exception {
		LoopBack loopBack = new LoopBack();
		context.setProperty(
				JobExecutionContext.JOB_START_DATE,
				DateFunctionTransformation.getInstance().getFormattedDate(
						new Date()));
		PowerMockito.when(autoTriggerObject.execute(context)).thenReturn(true);
		assertTrue(loopBack.execute(context));
	}

	@Test
	public void testContinuousJobAutoTriggerNonContinuous() throws Exception {
		LoopBack loopBack = new LoopBack();
		context.setProperty(
				JobExecutionContext.JOB_START_DATE,
				DateFunctionTransformation.getInstance().getFormattedDate(
						new Date()));
		context.setProperty(JobExecutionContext.CONTINUOUS_LOADING, "FALSE");
		assertTrue(loopBack.execute(context));
	}
}
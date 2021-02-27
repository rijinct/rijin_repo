package com.project.rithomas.jobexecution.common.script;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderExecuter;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { AggScriptExecutor.class, ScriptExecutor.class, schedulerJobRunnerfactory.class,
		HiveTableQueryUtil.class, TimeZoneUtil.class })
public class AggScriptExecutorTest {

	WorkFlowContext context = new JobExecutionContext();

	AggScriptExecutor executor = new AggScriptExecutor();

	@Mock
	HiveJobRunner runner;

	@Mock
	ProcessBuilderExecuter pbExec;

	@Mock
	UpdateBoundary update;

	@Before
	public void setUp() throws Exception {
		context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.LB, 1572287400000L);
		context.setProperty(JobExecutionContext.NEXT_LB, 1572373800000L);
		context.setProperty(JobExecutionContext.UB, 1572373800000L);
		context.setProperty(JobExecutionContext.PLEVEL, "DAY");
		context.setProperty(JobExecutionContext.SOURCE, "PS_PCEI_VOICE_L1_1_DAY,PS_PCEI_DATA_CP_L1_SUB_1_DAY");
		context.setProperty(JobExecutionContext.TARGET, "PS_PCEI_INDEX_1_DAY");
		context.setProperty("SCRIPT_NAME", "/mnt/pcei_calc.sh");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		PowerMockito.mockStatic(schedulerJobRunnerfactory.class, HiveTableQueryUtil.class, TimeZoneUtil.class);
		when(schedulerJobRunnerfactory.getRunner(schedulerConstants.HIVE_DATABASE, false)).thenReturn(runner);
		when(HiveTableQueryUtil.retrieveHDFSLocationGivenTableName(context, "PS_PCEI_INDEX_1_DAY"))
				.thenReturn(Arrays.asList("/rithomas/ps/PCEI_INDEX/PCEI_INDEX_1_DAY"));
		when(HiveTableQueryUtil.retrieveHDFSLocationGivenTableName(context,
				"PS_PCEI_VOICE_L1_1_DAY,PS_PCEI_DATA_CP_L1_SUB_1_DAY")).thenReturn(Arrays.asList(
						"/rithomas/ps/PCEI_VOICE/PCEI_VOICE_L1_1_DAY,/rithomas/ps/PCEI_DATA_CP/PCEI_DATA_CP_L1_SUB_1_DAY"));
		PowerMockito.whenNew(ProcessBuilderExecuter.class).withNoArguments().thenReturn(pbExec);
		PowerMockito.whenNew(UpdateBoundary.class).withNoArguments().thenReturn(update);
		when(TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)).thenReturn(false);
		when(TimeZoneUtil.isTimeZoneEnabled(context)).thenReturn(false);
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		executor = new AggScriptExecutor();
		executor.execute(context);
	}
	
	@Test
	public void testExecuteWithTz() throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "YES");
		when(TimeZoneUtil.isTimezoneEnabledAndNotAgnostic(context)).thenReturn(true);
		when(TimeZoneUtil.isTimeZoneEnabled(context)).thenReturn(true);
		context.setProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS,
				Arrays.asList("TZ1", "TZ2"));
		context.setProperty(JobExecutionContext.LB+"_TZ1", 1572287400000L);
		context.setProperty(JobExecutionContext.NEXT_LB+"_TZ1", 1572373800000L);
		context.setProperty(JobExecutionContext.UB+"_TZ1", 1572373800000L);
		context.setProperty(JobExecutionContext.LB+"_TZ2", 1572287400000L);
		context.setProperty(JobExecutionContext.NEXT_LB+"_TZ2", 1572373800000L);
		executor = new AggScriptExecutor();
		executor.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testExecuteInterrupted() throws WorkFlowExecutionException {
		executor = new AggScriptExecutor();
		executor.abort(context);
		executor.execute(context);
	}

	@Test(expected = Exception.class)
	public void testJobRunnerNull() throws Exception {
		when(schedulerJobRunnerfactory.getRunner(schedulerConstants.HIVE_DATABASE, false)).thenReturn(null);
		executor = new AggScriptExecutor();
		executor.execute(context);

	}
}

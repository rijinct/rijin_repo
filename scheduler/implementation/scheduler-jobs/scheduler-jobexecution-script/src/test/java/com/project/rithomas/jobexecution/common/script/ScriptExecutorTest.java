
package com.project.rithomas.jobexecution.common.script;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.util.HiveJobRunner;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.DeployerException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderExecuter;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ScriptExecutor.class, schedulerJobRunnerfactory.class,
		HiveTableQueryUtil.class, RetrieveDimensionValues.class })
public class ScriptExecutorTest {

	WorkFlowContext context = new JobExecutionContext();

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Mock
	HiveJobRunner runner;

	@Mock
	ProcessBuilderExecuter pbExec;

	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.SOURCE,
				"PS_PCEI_VOICE_L1_1_DAY,PS_PCEI_DATA_CP_L1_SUB_1_DAY");
		context.setProperty(JobExecutionContext.TARGET, "PS_PCEI_INDEX_1_DAY");
		context.setProperty("SCRIPT_NAME", "/mnt/pcei_calc.sh");
		context.setProperty(JobExecutionContext.IS_CUSTOM_DB_URL, false);
		PowerMockito.mockStatic(schedulerJobRunnerfactory.class,
				HiveTableQueryUtil.class, RetrieveDimensionValues.class);
		when(schedulerJobRunnerfactory
				.getRunner(schedulerConstants.HIVE_DATABASE, false))
						.thenReturn(runner);
		when(HiveTableQueryUtil.retrieveHDFSLocationGivenTableName(context,
				"PS_PCEI_INDEX_1_DAY")).thenReturn(
						Arrays.asList("/rithomas/ps/PCEI_INDEX/PCEI_INDEX_1_DAY"));
		when(HiveTableQueryUtil.retrieveHDFSLocationGivenTableName(context,
				"PS_PCEI_VOICE_L1_1_DAY,PS_PCEI_DATA_CP_L1_SUB_1_DAY"))
						.thenReturn(Arrays.asList(
								"/rithomas/ps/PCEI_VOICE/PCEI_VOICE_L1_1_DAY,/rithomas/ps/PCEI_DATA_CP/PCEI_DATA_CP_L1_SUB_1_DAY"));

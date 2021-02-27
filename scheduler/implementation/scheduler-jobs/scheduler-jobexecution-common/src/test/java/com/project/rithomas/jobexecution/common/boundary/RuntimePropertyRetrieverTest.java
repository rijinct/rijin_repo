package com.project.rithomas.jobexecution.common.boundary;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { RuntimePropertyRetriever.class})
public class RuntimePropertyRetrieverTest {
	
	//WorkFlowContext context = new JobExecutionContext();


	WorkFlowContext context = new JobExecutionContext();

	
	@Before
	public void setUp() throws Exception {

		context.setProperty(JobExecutionContext.WEEK_START_DAY, "0");
		
	}	

@Test
public void testgetWaitTimeToProceedAgg() throws Exception {
	
	RuntimePropertyRetriever runtimePropertyRetriever = new RuntimePropertyRetriever(context);
	
	Long watTimeExpected = 86400000L;
	Long waitTime = runtimePropertyRetriever.getWaitTimeToProceedAgg();
	assertEquals(watTimeExpected, waitTime);
}

@Test
public void testGetWeekStartDay() {
	
	RuntimePropertyRetriever runtimePropertyRetriever = new RuntimePropertyRetriever(context);
	String weekStartDay = runtimePropertyRetriever.getWeekStartDay();
	assertEquals("0", weekStartDay);

}

}

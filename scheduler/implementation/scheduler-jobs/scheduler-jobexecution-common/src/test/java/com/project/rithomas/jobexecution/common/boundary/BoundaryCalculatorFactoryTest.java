package com.project.rithomas.jobexecution.common.boundary;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { BoundaryCalculatorFactory.class})
public class BoundaryCalculatorFactoryTest {
	
	//WorkFlowContext context = new JobExecutionContext();


	WorkFlowContext context = new JobExecutionContext();
	
	@Mock
	JobPropertyRetriever jobPropertyRetrieverMock;

	
	@Before
	public void setUp() throws Exception {
		
		List<String> sourceJobList = Arrays.asList("Usage_SGSN_1");
		
		context.setProperty(JobExecutionContext.SOURCEJOBTYPE, "Loading");
		context.setProperty(JobExecutionContext.SOURCEJOBLIST, sourceJobList);

		
		PowerMockito.whenNew(JobPropertyRetriever.class).withAnyArguments().thenReturn(jobPropertyRetrieverMock);
		
	}	

@Test
public void testgetBoundaryCalculatorInstance() {
	
	BoundaryCalculatorFactory boundaryCalculatorFactory = new BoundaryCalculatorFactory();
	BoundaryCalculator boundaryCalculator = boundaryCalculatorFactory.getBoundaryCalculatorInstance(context);
	assertThat(boundaryCalculator, instanceOf(BoundaryCalculator.class));
}
}
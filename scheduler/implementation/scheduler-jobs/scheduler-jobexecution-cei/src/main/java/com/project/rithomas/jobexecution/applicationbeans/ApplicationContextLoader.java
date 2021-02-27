
package com.project.rithomas.jobexecution.applicationbeans;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.project.rithomas.sdk.workflow.WorkFlow;

public class ApplicationContextLoader {

	private static ApplicationContext applicationContext = initializeContext();

	private static final ApplicationContextLoader CONTEXT_LOADER = new ApplicationContextLoader();

	private ApplicationContextLoader() {
	}

	public static ApplicationContextLoader getInstance() {
		return CONTEXT_LOADER;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	private static ApplicationContext initializeContext() {
		return new ClassPathXmlApplicationContext(
				"beans/perfIndiSpecLogisticFormulaTypeContext.xml");
	}

	public LogisticKPIFormulaTypeProvider getLogisticKPITypeProvider() {
		return (LogisticKPIFormulaTypeProvider) applicationContext
				.getBean("logisticFormulaTypeProvider");
	}

	public WeightedAvgKPIFormulaTypeProvider getWeightedAvgKPITypeProvider() {
		return (WeightedAvgKPIFormulaTypeProvider) applicationContext
				.getBean("weightedAvgFormulaTypeProvider");
	}

	public WorkFlow getWorkFlowProvider(String workFlowId) {
		return (WorkFlow) applicationContext.getBean(workFlowId);
	}
}

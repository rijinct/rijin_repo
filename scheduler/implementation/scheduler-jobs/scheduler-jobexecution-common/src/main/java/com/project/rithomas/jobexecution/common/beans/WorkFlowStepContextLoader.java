
package com.rijin.jobexecution.common.beans;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;

public class WorkFlowStepContextLoader {

	private static ApplicationContext applicationContext = initializeContext();

	private static final WorkFlowStepContextLoader CONTEXT_LOADER = new WorkFlowStepContextLoader();

	private WorkFlowStepContextLoader() {
	}

	public static WorkFlowStepContextLoader getInstance() {
		return CONTEXT_LOADER;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	private static ApplicationContext initializeContext() {
		return new ClassPathXmlApplicationContext(
				"beans/schedulerApplicationContext.xml");
	}

	public AbstractWorkFlowStep getJobWorkFlowStepExecutor(
			String jobWorkFlowStepId) {
		return (AbstractWorkFlowStep) applicationContext
				.getBean(jobWorkFlowStepId);
	}


}

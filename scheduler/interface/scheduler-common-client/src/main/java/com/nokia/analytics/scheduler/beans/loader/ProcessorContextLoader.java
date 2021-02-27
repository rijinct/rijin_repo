package com.rijin.analytics.scheduler.beans.loader;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.rijin.analytics.scheduler.processor.BaseClientProcessor;

public class ProcessorContextLoader {
	
	private static ApplicationContext applicationContext = initializeContext();

	private static volatile ProcessorContextLoader CONTEXT_LOADER;

	private ProcessorContextLoader() {
		
	}
	
	public static ProcessorContextLoader getInstance() {
		if(CONTEXT_LOADER == null) {
			synchronized (ProcessorContextLoader.class) {
				CONTEXT_LOADER = new ProcessorContextLoader();
			}
		}
		
		return CONTEXT_LOADER;
	}
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	private static ApplicationContext initializeContext() {
		return new ClassPathXmlApplicationContext(
				"beans/application-profile-context.xml");
	}

	public BaseClientProcessor getClientProcessor() {
		return (BaseClientProcessor) applicationContext
				.getBean("clientProcessor");
	}
}

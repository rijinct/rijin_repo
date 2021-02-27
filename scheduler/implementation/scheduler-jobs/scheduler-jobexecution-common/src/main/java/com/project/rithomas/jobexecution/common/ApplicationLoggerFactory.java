
package com.project.rithomas.jobexecution.common;

import java.lang.reflect.Constructor;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ApplicationLoggerFactory {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ApplicationLoggerFactory.class);

	public static ApplicationLoggerUtilInterface getApplicationLogger()
			throws WorkFlowExecutionException {
		Class ref = null;
		ApplicationLoggerUtilInterface applicationLoggerUtilInterface = null;
		try {
					ref = Class.forName("com.project.rithomas.jobexecution.common.ApplicationIdLoggerHiveServer2Util");
					Constructor[] constructors = ref.getDeclaredConstructors();
					constructors[0].setAccessible(true);
					applicationLoggerUtilInterface = (ApplicationLoggerUtilInterface) constructors[0]
							.newInstance();
					LOGGER.debug("The object created  is "
							+ applicationLoggerUtilInterface.getClass()
									.getSimpleName());
				
			
			return applicationLoggerUtilInterface;
		} catch (Exception exception) {
			LOGGER.error(exception.getMessage(), exception);
			throw new WorkFlowExecutionException(exception.getMessage());
		}
	}
}

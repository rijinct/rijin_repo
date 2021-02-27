
package com.rijin.analytics.scheduler.k8s.processor;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.rijin.analytics.scheduler.processor.JobProcessor;
import com.project.rithomas.jobexecution.common.AsynchronousScheduleProcessor;
import com.project.sai.rithomas.scheduler.service.schedulerService;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class schedulerK8sProcessor extends BaseClientProcessor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(schedulerK8sProcessor.class);

	protected JobProcessor getJobCreator() {
		return new CronJobCreator();
	}

	protected JobProcessor getJobDeletor() {
		return new CronJobDeletor();
	}

	protected JobProcessor getJobscheduler() {
		return new CronJobscheduler();
	}

	protected JobProcessor getJobUnscheduler() {
		return new CronJobUnscheduler();
	}

	protected JobProcessor getJobAborter() {
		return new CronJobAborter();
	}
	
	protected JobProcessor getAsynchronousScheduleProcessor() {
		return new AsynchronousScheduleProcessor();
	}

	@Override
	protected schedulerService getService() {
		return null;
	}

	@Override
	protected JobProcessor getJobRescheduler() {
		return new CronJobscheduler();
	}

	@Override
	protected void postProcessOperations(
			schedulerManagementOperationResult result) {
		if (hasErrorOccurred(result) && !isAutoTriggerMode) {
			printResult(result);
			throw new InternalServerErrorException();
		}
	}

	private boolean hasErrorOccurred(
			schedulerManagementOperationResult result) {
		return result != null && result.isError();
	}

	private void printResult(schedulerManagementOperationResult result) {
		if (result.getErrorCount() > 0) {
			LOGGER.error("Error Details: {}", result.getErrorMsgs());
		}
	}
}

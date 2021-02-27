
package com.project.rithomas.jobexecution.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.processor.JobProcessor;

public class AsynchronousScheduleProcessor extends JobProcessor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AsynchronousScheduleProcessor.class);

	private static final String DEFAULT_CRON_NAMESPACE = "default";

	private static final String KEY_CRON_NAMESPACE = "CRON_NAMESPACE";

	@Override
	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request) {
		return processRequest(request, new HashMap<>());
	}

	@Override
	public schedulerManagementOperationResult processRequest(
			JobSchedulingData request, Map<String, Object> jobMetaData) {
		Simple simpleTrigger = request.getSchedule().getTrigger().get(0)
				.getSimple();
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					getJobProcessor().processJob(simpleTrigger.getJobName(),
							jobMetaData, simpleTrigger.getGroup(),
							getNamespace());
				} catch (Exception e) {
					LOGGER.error("Asynchronous execution of {} failed",
							simpleTrigger.getJobName(), e);
				}
			}
		}).start();
		return new schedulerManagementOperationResult();
	}

	protected String getNamespace() {
		return StringUtils.defaultString(System.getenv(KEY_CRON_NAMESPACE),
				DEFAULT_CRON_NAMESPACE);
	}

	private BaseJobProcessor getJobProcessor() {
		return new BaseJobProcessor();
	}
}

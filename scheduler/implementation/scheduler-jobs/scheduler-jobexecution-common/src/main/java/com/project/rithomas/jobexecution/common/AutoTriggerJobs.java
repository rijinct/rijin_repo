
package com.project.rithomas.jobexecution.common;

import static org.apache.commons.lang3.BooleanUtils.toBoolean;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.beans.loader.ProcessorContextLoader;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;

public class AutoTriggerJobs extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AutoTriggerJobs.class);

	BaseClientProcessor baseClientProcessor;

	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		List<String> jobsToBeTriggered = (List<String>) context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED);
		Map<Object, Object> mapToPassToDepJobs = (Map<Object, Object>) context
				.getProperty(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS);
		LOGGER.debug("Auto triggering {} jobs with additional configuration {}",
				jobsToBeTriggered, mapToPassToDepJobs);
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		if (jobsToBeTriggered != null && !jobsToBeTriggered.isEmpty()) {
			for (String jobName : jobsToBeTriggered) {
				JobDictionary jobDictionary = jobDictionaryQuery
						.retrieve(jobName);
				if (jobDictionary == null) {
					LOGGER.warn(
							"Job with the name: {} is not present, hence skipping triggering of the same.",
							jobName);
				} else {
					String jobGroup = jobDictionary.getAdaptationId() + "_"
							+ jobDictionary.getAdaptationVersion();
					LOGGER.debug("Autotriggering job with name {} and group {}",
							jobName, jobGroup);
					JobSchedulingData jobSchedulingData = getJobSchedulingData(
							jobName, jobGroup, context);
					LOGGER.debug("Autotrigger request {}", jobSchedulingData);
					schedulerManagementOperationResult result = null;
					baseClientProcessor = ProcessorContextLoader.getInstance()
							.getClientProcessor();
					baseClientProcessor.setAutoTriggerMode(true);
					if (mapToPassToDepJobs != null
							&& !mapToPassToDepJobs.isEmpty()) {
						LOGGER.debug("Processing request with Maps : {}",
								mapToPassToDepJobs);
						Map<String, Object> map = new HashMap<String, Object>();
						map.put(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS,
								mapToPassToDepJobs);
						result = baseClientProcessor
								.processRequest(jobSchedulingData, map);
					} else {
						LOGGER.debug("Processing request : {}",
								jobSchedulingData);
						result = baseClientProcessor
								.processRequest(jobSchedulingData);
					}
					if (result != null) {
						if (result.getErrorCount() > 0) {
							LOGGER.error("Error while triggering the job: {}",
									jobName);
							LOGGER.error("Error Details: {}",
									result.getErrorMsgs());
						} else {
							LOGGER.info(
									"Successfully triggered the job: {} under the group: {}",
									jobName, jobGroup);
						}
					}
				}
			}
		} else {
			LOGGER.debug("No jobs to be triggered.");
		}
		return true;
	}

	private JobSchedulingData getJobSchedulingData(String jobName,
			String jobGroup, WorkFlowContext context) {
		JobSchedulingData jobSchedulingData = new JobSchedulingData();
		Schedule schedule = new Schedule();
		List<Trigger> triggers = new ArrayList<Trigger>();
		Trigger trigger = new Trigger();
		Simple simpleTrigger = new Simple();
		Date startDate = new Date();
		simpleTrigger.setJobGroup(jobGroup);
		simpleTrigger.setJobName(jobName);
		simpleTrigger
				.setName(jobName.concat(String.valueOf(startDate.getTime())));
		simpleTrigger.setGroup(jobGroup);
		trigger.setSimple(simpleTrigger);
		triggers.add(trigger);
		schedule.setTrigger(triggers);
		jobSchedulingData.setOperation(getOperation(context));
		jobSchedulingData.setSchedule(schedule);
		return jobSchedulingData;
	}

	private String getOperation(WorkFlowContext context) {
		String operation = JobSchedulingOperation.SCHEDULE.name();
		String isAsynchExecution = String.valueOf(context
				.getProperty(JobExecutionContext.ASYNCHRONOUS_EXECUTION));
		if (toBoolean(isAsynchExecution)) {
			operation = JobSchedulingOperation.ASYNCHRONOUS_SCHEDULE.name();
		}
		return operation;
	}
}

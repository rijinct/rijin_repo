
package com.rijin.analytics.scheduler.k8s.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.k8s.beans.JobDescriptor;
import com.rijin.analytics.scheduler.k8s.beans.mapper.JobDescriptorDecorator;
import com.rijin.analytics.scheduler.k8s.processor.schedulerK8sBulkOperationsProcessor;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "Job Management", description = "Create and Manage Jobs")
@RequestMapping("/api/manager/v1/scheduler")
@Validated
public class JobManagementController {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobManagementController.class);

	@Autowired
	JobDescriptorDecorator jobDescriptorDecorator;

	@Autowired
	BaseClientProcessor processor;

	@Autowired
	schedulerK8sBulkOperationsProcessor bulkOperationsProcessor;

	@PostMapping(path = "/jobs", produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" }, consumes = "application/vnd.rijin-se-schedules-v1+json")
	@ApiOperation(value = "${job.operation.create}", notes = "${job.operation.create.notes}")
	public void createJob(
			@ApiParam(value = "${parameter.jobdescriptor}") @Valid @RequestBody List<JobDescriptor> jobDescriptors) {
		LOGGER.info("Request received to create job with details {}",
				jobDescriptors);
		JobSchedulingData schedulingData = jobDescriptorDecorator
				.toSchedulingData(jobDescriptors,
						JobSchedulingOperation.CREATE.name());
		processRequest(schedulingData);
		LOGGER.info("Creation of jobs is successful");
	}

	@DeleteMapping(path = "/jobs", produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" }, consumes = "application/vnd.rijin-se-schedules-v1+json")
	@ApiOperation(value = "${job.operation.delete}", notes = "${job.operation.delete.notes}")
	public void deleteJob(
			@ApiParam(value = "${parameter.jobdescriptor}") @Valid @RequestBody List<JobDescriptor> jobDescriptors) {
		LOGGER.info("Request received to delete job with details {}",
				jobDescriptors);
		JobSchedulingData schedulingData = jobDescriptorDecorator
				.toSchedulingData(jobDescriptors,
						JobSchedulingOperation.DELETE.name());
		processRequest(schedulingData);
		LOGGER.info("Deletion of jobs is successful");
	}

	@PatchMapping(path = { "/jobs" }, produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" })
	@ApiOperation(value = "${job.operation.activate}", notes = "${job.operation.activate.notes}")
	public void toggleScheduleState(
			@ApiParam(value = "${jobs.action}", required = true, type = "string", allowableValues = "pause", example = "PAUSE") @RequestParam("action") String action,
			@ApiParam(value = "${jobs.action.jobgroup}", required = false, allowEmptyValue = true, defaultValue = "") @RequestParam("jobGroup") Optional<String> jobGroup) {
		LOGGER.info("Received request to perform operation : {} on group : {}",
				action, jobGroup.orElse("all"));
		bulkOperationsProcessor.processRequest(
				JobSchedulingOperation.valueOf(action.toUpperCase()),
				jobGroup.orElse(null));
	}

	@DeleteMapping(path = "/jobs/schedule", produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" })
	@ApiOperation(value = "${job.operation.abort}", notes = "${job.operation.abort.notes}")
	public void abortJob(
			@ApiParam(value = "${jobs.abort.action}", required = true, type = "string") @RequestParam("jobNames") List<String> jobNames) {
		LOGGER.info("Request received to abort job: {} ", jobNames);
		Map<String, Thread> availableThreads = getAvailableThreads();
		LOGGER.info("Available threads : {}", availableThreads.keySet());
		for (String jobName : jobNames) {
			Thread thread = availableThreads.get(jobName.toLowerCase());
			if (thread != null) {
				LOGGER.info("Interrupting Job Thread {} found ", jobName);
				synchronized (thread) {
					thread.interrupt();
					thread.notifyAll();
				}
			} else {
				LOGGER.info("Interrupting Job Thread {} not found ", jobName);
			}
		}
	}

	private void processRequest(JobSchedulingData schedulingData) {
		LOGGER.info("Processing request with details {}", schedulingData);
		processor.processRequest(schedulingData);
		LOGGER.info("Request finished successfully");
	}

	private Map<String, Thread> getAvailableThreads() {
		Map<String, Thread> availableThreadsAsMap = new HashMap<>();
		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			availableThreadsAsMap.put(thread.getName().toLowerCase(), thread);
		}
		return availableThreadsAsMap;
	}
}

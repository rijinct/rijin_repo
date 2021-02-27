
package com.rijin.analytics.scheduler.k8s.controllers;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.k8s.beans.SimpleJobDescriptor;
import com.rijin.analytics.scheduler.k8s.beans.mapper.SimpleJobDescriptorDecorator;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "Job Management", description = "Create and Manage Jobs")
@RequestMapping("/api/manager/v1/scheduler")
@Validated
public class SimpleJobManagementController {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(SimpleJobManagementController.class);

	@Autowired
	SimpleJobDescriptorDecorator jobDescriptorDecorator;

	@Autowired
	BaseClientProcessor processor;

	@PatchMapping(path = "/jobs/simple", produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" }, consumes = "application/vnd.rijin-se-schedules-v1+json")
	@ApiOperation(value = "${job.operation.simpleschedule}", notes = "${job.operation.simpleschedule.notes}")
	public void scheduleSimpleJob(
			@ApiParam(value = "${parameter.simplejobdescriptor}") @Valid @RequestBody List<SimpleJobDescriptor> jobMetadata) {
		LOGGER.info("Request received to schedule simple jobs with details {}",
				jobMetadata);
		JobSchedulingData schedulingData = jobDescriptorDecorator
				.toSchedulingData(jobMetadata,
						JobSchedulingOperation.SCHEDULE.name());
		processRequest(schedulingData);
		LOGGER.info("Simple schedule successfully updated for the jobs");
	}

	private void processRequest(JobSchedulingData schedulingData) {
		LOGGER.info("Processing request with details {}", schedulingData);
		processor.processRequest(schedulingData);
		LOGGER.info("Request finished successfully");
	}
}

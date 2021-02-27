
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
import com.rijin.analytics.scheduler.k8s.beans.CronJobDescriptor;
import com.rijin.analytics.scheduler.k8s.beans.mapper.CronJobDescriptorDecorator;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.sai.rithomas.scheduler.constants.JobSchedulingOperation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@Api(tags = "Job Management", description = "Create and Manage Jobs")
@RequestMapping("/api/manager/v1/scheduler")
@Validated
public class CronJobManagementController {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CronJobManagementController.class);

	@Autowired
	CronJobDescriptorDecorator jobDescriptorDecorator;

	@Autowired
	BaseClientProcessor processor;

	@PatchMapping(path = "/jobs/cron", produces = {
			"application/vnd.rijin-se-schedules-v1+json",
			"application/vnd.rijin-error-response-v1+json" }, consumes = "application/vnd.rijin-se-schedules-v1+json")
	@ApiOperation(value = "${job.operation.cronschedule}", notes = "${job.operation.cronschedule.notes}")
	public void scheduleCronJob(
			@ApiParam(value = "${parameter.cronjobdescriptor}") @Valid @RequestBody List<CronJobDescriptor> jobDescriptors) {
		LOGGER.info("Request received to schedule cron jobs with details {}",
				jobDescriptors);
		JobSchedulingData schedulingData = jobDescriptorDecorator
				.toSchedulingData(jobDescriptors,
						JobSchedulingOperation.SCHEDULE.name());
		processRequest(schedulingData);
		LOGGER.info("Cron schedule successfully updated for the jobs");
	}

	private void processRequest(JobSchedulingData schedulingData) {
		LOGGER.info("Processing request with details {}", schedulingData);
		processor.processRequest(schedulingData);
		LOGGER.info("Request finished successfully");
	}
}

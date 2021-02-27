
package com.rijin.analytics.scheduler.k8s.controllers;

import java.util.Optional;

import com.rijin.analytics.exceptions.InternalServerErrorException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.converters.MapToJsonConverter;
import com.rijin.analytics.scheduler.k8s.exception.AbortJobException;
import com.project.rithomas.jobexecution.common.BaseJobProcessor;

import springfox.documentation.annotations.ApiIgnore;
@ApiIgnore
@RestController
@RequestMapping("/api/executor/v1/jobs")
@Validated
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobExecutionController {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(JobExecutionController.class);

	@Autowired
	private BaseJobProcessor processor;

	private Thread currentThread;
	
	private boolean isInterrupted;
	
	private boolean isCompleted = false;

	@PostMapping(produces = { "application/vnd.rijin-se-jobs-v1+json", "application/vnd.rijin-error-response-v1+json" })
	public void executeJob(@RequestParam(value = "namespace") String namespace,
			@RequestParam(value = "jobName") String jobName, @RequestParam(value = "jobGroup") String jobGroup,
			@RequestBody Optional<String> properties) {
		try {
			LOGGER.info(
					"Executing job {} in namespace {}, group {} and properties {}",
					jobName, namespace, jobGroup, properties);
			currentThread = Thread.currentThread();
			currentThread.setName(jobName);
			monitorCurrentThreadForInterruption();
			processor.processJob(jobName, MapToJsonConverter.toMap(properties.orElse("{}")), jobGroup, namespace);			
		} catch (Exception exception) {
			if (isInterrupted) {
				LOGGER.error("Job: {}  aborted successfully ", jobName);
				throw new AbortJobException("Job aborted successfully", "abortJob");
			}else {
				LOGGER.error("Exception occurred when trying execute job", exception);
				throw new InternalServerErrorException();
			}
		} finally {
			isCompleted = true;
			synchronized (currentThread) {
				currentThread.notify();
			}
		}
	}
	
	private void monitorCurrentThreadForInterruption() {
		Thread monitoringThread = new Thread(Thread.currentThread().getThreadGroup(), "monitor") {
			public void run() {
				synchronized (currentThread) {
					while (!currentThread.isInterrupted() && !isCompleted) {
						try {
							LOGGER.info("Thread {}, is executing ", currentThread.getName());
							currentThread.wait(300000l);
						} catch (InterruptedException e) {
							LOGGER.error("Thread got interrupted", e);
						}
					}
					if (!isCompleted) {
						try {
							LOGGER.info("Interrupt received for job {}: ",
									currentThread.getName());
							isInterrupted = true;
							processor.interruptJob();
						} catch (Exception e) {
							LOGGER.error("Exception occured", e);
							throw new AbortJobException("Abort Job Failed",
									"abortJob");
						}
					}
				}
			}
		};
		monitoringThread.start();
	}
}


package com.rijin.analytics.scheduler.k8s;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

@SpringBootApplication(scanBasePackages = {
		"com.rijin.analytics.scheduler.k8s.**", "com.project.rithomas.jobexecution.**",
		"com.rijin.analytics.swagger", "com.rijin.analytics.exception",
		"com.rijin.analytics.k8s.client.**", "com.rijin.analytics.logging.**" })
public class schedulerK8sEndpointApplication {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(schedulerK8sEndpointApplication.class);

	@Autowired
	PoolConfigInitializer poolConfigInitializer;

	@Autowired
	JobStatusUpdater jobStatusUpdater;

	@Autowired
	HealthCheck healthCheck;

	public static void main(String[] args) {
		SpringApplication.run(schedulerK8sEndpointApplication.class, args);
	}

	@EventListener(ApplicationReadyEvent.class)
	public void startup() throws Exception {
		LOGGER.info("Initialising the post boot configurations");
		healthCheck.setReady(true);
		poolConfigInitializer.initializePool();
		jobStatusUpdater.updateJobsToError();
	}

	@PreDestroy
	public void shutdown() {
		LOGGER.info("Shutdown called. Continuing with graceful shutdown.");
		healthCheck.setReady(false);
		jobStatusUpdater.updateJobsToError();
	}
}

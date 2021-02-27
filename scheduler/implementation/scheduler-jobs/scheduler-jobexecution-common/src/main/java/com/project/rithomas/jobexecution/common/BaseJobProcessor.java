
package com.project.rithomas.jobexecution.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.ETLStatusUpdater;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BaseJobProcessor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(BaseJobProcessor.class);

	private JobExecutionManager jobExecutionManager;

	private String jobName;

	private String jobGroup;

	private volatile boolean interrupted;

	private volatile Thread thisThread;

	private static final String JOBNAME_TIME = "ROUTINGKEY";

	private static final String JOB_NAME = "JOBNAME";

	private static final String NAMESPACE = "namespace";

	public void processJob(String jobName, Map<String, Object> jobMetaData,
			String jobGroup, String nameSpace)
			throws WorkFlowExecutionException {
		initialize(jobName, jobGroup, nameSpace);
		executeWorkflow(jobMetaData);
	}

	private void initialize(String jobName, String jobGroup, String nameSpace) {
		this.jobName = jobName;
		this.jobGroup = jobGroup;
		thisThread = Thread.currentThread();
		ThreadContext.put(JOB_NAME, this.jobName);
		ThreadContext.put(JOBNAME_TIME, this.jobName + "_" + getDateString());
		ThreadContext.put(NAMESPACE, nameSpace);
		LOGGER.debug(
				"Initializing processor with jobName : {} | jobGroup : {} | namespace: {}",
				jobName, jobGroup, nameSpace);
	}

	private void executeWorkflow(Map<String, Object> jobMetaData)
			throws WorkFlowExecutionException {
		try {
			jobExecutionManager = new JobExecutionManager();
			jobExecutionManager.setWorkflowStepsAndExecute(jobName,
					jobMetaData);
		} catch (Exception exception) {
			LOGGER.error("Unable to process the job request ", exception);
			throw new WorkFlowExecutionException(
					"Unable to process request for the job :" + this.jobName,
					exception);
		} finally {
			if (interrupted) {
				handleInterruptedJob();
			}
		}
	}

	private String getDateString() {
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
		return sdfDate.format(new Date());
	}

	private void handleInterruptedJob() throws WorkFlowExecutionException {
		LOGGER.info("Job {}/{} was interrupted.Hence exiting.", jobName,
				jobGroup);
		ETLStatusUpdater etlStatusUpdate = new ETLStatusUpdater();
		try {
			etlStatusUpdate.updateStatusForRunningJobToError(jobName);
		} catch (WorkFlowExecutionException e) {
			LOGGER.error("Error updating status for job {} to E.", jobName);
			throw new WorkFlowExecutionException(
					"WorkFlowExecutionException : ", e);
		} finally {
			thisThread = null;
			ThreadContext.remove(JOBNAME_TIME);
		}
	}

	public void interruptJob() {
		LOGGER.info("Job {}/{} was interrupted.Trying to abort job.", jobName,
				jobGroup);
		ThreadContext.put(JOBNAME_TIME,
				"Abort_" + jobName + "_" + getDateString());
		interrupted = true;
		if (thisThread == null) {
			return;
		}
		thisThread.interrupt();
		LOGGER.debug(
				"Propagating interrupt to jobexecution manager for job {} ",
				jobName);
		this.jobExecutionManager.interrupt(thisThread.isInterrupted());
		long waitTime = 1000l;
		long totalWait = 0l;
		while (true) {
			if (isThreadAlive()) {
				LOGGER.info("Waiting for the job {} to be aborted.", jobName);
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					LOGGER.warn(
							"InterruptedException occured while waiting for job to be aborted. {} ",
							e);
					break;
				}
				totalWait += waitTime;
				if (totalWait >= 120000l && isThreadAlive()) {
					LOGGER.warn(
							"Could not interrupt job {} in this run after waiting for 2 minutes. Hive jobs might be killed in the background. If the job hasnt been killed, please try aborting again.",
							jobName);
					break;
				}
			} else {
				LOGGER.debug(
						"Thread is not alive.Job {} execution was done/killed.",
						jobName);
				break;
			}
		}
	}

	private boolean isThreadAlive() {
		return (thisThread == null) ? false : thisThread.isAlive();
	}
}

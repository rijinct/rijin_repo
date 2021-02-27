
package com.project.rithomas.jobexecution.common;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class CheckJobStatus extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(CheckJobStatus.class);

	protected final static long MILLISECOND_CONV_FACTOR = 1000;

	protected final static long SECONDS_CONV_FACTOR = 60;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.info("Checking if previous jobs are in R state");
		boolean success = false;
		try {
			GetDBResource.getInstance()
					.retrievePostgresObjectProperties(context);
			String jobName = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			String waitingForFileStatus = "I";
			String runningStatus = "R";
			String commitStatus = "C";
			String jobtype = (String) context
					.getProperty(JobExecutionContext.JOBTYPE);
			UpdateJobStatus updateJobStatus = new UpdateJobStatus();
			Long delay = 0L;
			Long totalTime = 0L;
			boolean isTrue = false;
			Long waitMinute = (Long) context
					.getProperty(JobExecutionContext.WAIT_MINUTE);
			String aLevel = null;
			// According to agg level set the delay and totaltime
			if ((String) context
					.getProperty(JobExecutionContext.ALEVEL) != null) {
				aLevel = (String) context
						.getProperty(JobExecutionContext.ALEVEL);
				if (aLevel.equalsIgnoreCase("HOUR")) {
					delay = 10L;
					totalTime = 60L;
					isTrue = true;
				} else if (aLevel.equalsIgnoreCase("15MIN")) {
					delay = 5L;
					totalTime = 15L;
					isTrue = true;
				}
			}
			boolean isWaiting = false;
			String status = null;
			String sql = QueryConstants.CHECK_JOB_STATUS
					.replace("$JOBNAME", "'" + jobName + "'")
					.replace("$STATUS", "'W'");
			// Query execution using hibernate
			QueryExecutor executor = new QueryExecutor();
			Object[] resultSet = executor.executeMetadatasqlQuery(sql, context);
			if (resultSet != null) {
				status = (String) resultSet[0];
				if (waitingForFileStatus.equalsIgnoreCase(status)) {
					status = "R";
				}
				if (runningStatus.equalsIgnoreCase(status)
						|| commitStatus.equalsIgnoreCase(status)) {
					Long retryAttempt = waitMinute;
					if (JobExecutionContext.AGGREGATION
							.equalsIgnoreCase(jobtype) && isTrue) {
						LOGGER.info(
								"Previous Job is in running state.....Keep polling to check till it get finish");
						while (totalTime - retryAttempt - delay >= 0) {
							try {
								Thread.sleep(MILLISECOND_CONV_FACTOR
										* waitMinute * SECONDS_CONV_FACTOR);
								resultSet = executor
										.executeMetadatasqlQuery(sql, context);
								if (resultSet != null) {
									status = (String) resultSet[0];
									if (!runningStatus
											.equalsIgnoreCase(status)) {
										break;
									}
								}
							} catch (InterruptedException e) {
								LOGGER.error("Exception while retrying {}"
										+ e.getMessage());
								throw new WorkFlowExecutionException(
										"Exception while retrying ", e);
							} finally {
								retryAttempt = retryAttempt + waitMinute;
							}
						}
					}
					if (runningStatus.equalsIgnoreCase(status)
							|| commitStatus.equalsIgnoreCase(status)) {
						LOGGER.info(
								"Previous Job is in running state.....Hence updating the current job to waiting.Need to re-trigger(only aggregation) "
										+ "the current job after the previous job is completed");
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Previous instance of the job is running");
						context.setProperty(JobExecutionContext.STATUS, "W");
						updateJobStatus.insertJobStatus(context);
						context.setProperty(JobExecutionContext.RETURN, 2);
						isWaiting = true;
					} else {
						context.setProperty(JobExecutionContext.PREV_STATUS,
								status);
						context.setProperty(JobExecutionContext.RETURN, 0);
					}
				} else {
					context.setProperty(JobExecutionContext.PREV_STATUS,
							status);
					context.setProperty(JobExecutionContext.RETURN, 0);
				}
			}
			if (!isWaiting) {
				String jobType = (String) context
						.getProperty(JobExecutionContext.JOBTYPE);
				if (JobTypeDictionary.PERF_REAGG_JOB_TYPE.equals(jobType)) {
					LOGGER.debug(
							"Checking the status of the aggregation job before executing re-aggregation..");
					String perfJobName = (String) context
							.getProperty(JobExecutionContext.PERF_JOB_NAME);
					sql = QueryConstants.CHECK_JOB_STATUS
							.replace("$JOBNAME", "'" + perfJobName + "'")
							.replace("$STATUS", "'W'");
					resultSet = executor.executeMetadatasqlQuery(sql, context);
					if (resultSet != null) {
						status = (String) resultSet[0];
						if (runningStatus.equalsIgnoreCase(status)
								|| commitStatus.equalsIgnoreCase(status)) {
							Long retryAttempt = waitMinute;
							LOGGER.info(
									"Previous Aggregation Job is in running state.....Keep polling to check till it get finish");
							while (totalTime - retryAttempt - delay >= 0) {
								try {
									Thread.sleep(MILLISECOND_CONV_FACTOR
											* waitMinute * SECONDS_CONV_FACTOR);
									resultSet = executor
											.executeMetadatasqlQuery(sql,
													context);
									if (resultSet != null) {
										status = (String) resultSet[0];
										if (!runningStatus
												.equalsIgnoreCase(status)) {
											break;
										}
									}
								} catch (InterruptedException e) {
									LOGGER.error("Exception while retrying {}"
											+ e.getMessage());
									throw new WorkFlowExecutionException(
											"Exception while retrying ", e);
								} finally {
									retryAttempt = retryAttempt + waitMinute;
								}
							}
							if (runningStatus.equalsIgnoreCase(status)
									|| commitStatus.equalsIgnoreCase(status)) {
								LOGGER.info(
										"Aggregation Job {} is in running state.....Hence updating the current job to waiting",
										perfJobName);
								context.setProperty(
										JobExecutionContext.DESCRIPTION,
										"Instance of aggregation job is running");
								context.setProperty(JobExecutionContext.STATUS,
										"W");
								updateJobStatus.insertJobStatus(context);
								context.setProperty(JobExecutionContext.RETURN,
										2);
								isWaiting = true;
							}
							LOGGER.debug(
									"Checking if reagg job is in running state or not");
							status = null;
							sql = QueryConstants.CHECK_JOB_STATUS
									.replace("$JOBNAME", "'" + jobName + "'")
									.replace("$STATUS", "'W'");
							resultSet = executor.executeMetadatasqlQuery(sql,
									context);
							if (resultSet != null) {
								status = (String) resultSet[0];
							}
							if (runningStatus.equals(status)) {
								LOGGER.info(
										"Reaggregation Job {} is in running state.....Hence updating the current job to waiting",
										jobName);
								context.setProperty(
										JobExecutionContext.DESCRIPTION,
										"Instance of reaggregation job is running");
								context.setProperty(JobExecutionContext.STATUS,
										"W");
								updateJobStatus.insertJobStatus(context);
								context.setProperty(JobExecutionContext.RETURN,
										2);
								isWaiting = true;
							} else {
								context.setProperty(
										JobExecutionContext.AGG_JOB_STATUS,
										status);
								context.setProperty(JobExecutionContext.RETURN,
										0);
							}
						} else {
							context.setProperty(
									JobExecutionContext.AGG_JOB_STATUS, status);
							context.setProperty(JobExecutionContext.RETURN, 0);
						}
					}
					if (!isWaiting && !isOffPeakHour(context)) {
						LOGGER.info(
								"Re-aggregation job can be run only during off-peak hours. Hence moving the job status to waiting..");
						context.setProperty(JobExecutionContext.DESCRIPTION,
								"Re-agg job can be run only in off peak hours");
						context.setProperty(JobExecutionContext.STATUS, "W");
						updateJobStatus.insertJobStatus(context);
						context.setProperty(JobExecutionContext.RETURN, 2);
					}
				}
			}
			success = true;
			if (context.getProperty(JobExecutionContext.RETURN) == null) {
				context.setProperty(JobExecutionContext.RETURN, 0);
			}
		} catch (JobExecutionException e) {
			LOGGER.error("Exception while executing query {}" + e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while checking job status ", e);
		}
		return success;
	}

	private boolean isOffPeakHour(WorkFlowContext context) {
		boolean isOffPeakHour = false;
		String offPeakHours = (String) context
				.getProperty(JobExecutionContext.OFF_PEAK_HOURS);
		if (offPeakHours == null || offPeakHours.isEmpty()) {
			LOGGER.debug(
					"Off peak hours not configured in the system. Hence continuing with re-aggregation job..");
			isOffPeakHour = true;
		} else {
			List<Integer> offPeakHourList = getOffPeakHourList(offPeakHours);
			Integer hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
			if (!offPeakHourList.contains(hour)) {
				LOGGER.info(
						"Re-aggregation job can be run only during the off-peak hours. {}",
						offPeakHours);
			} else {
				isOffPeakHour = true;
			}
		}
		return isOffPeakHour;
	}

	private List<Integer> getOffPeakHourList(String offPeakHours) {
		String[] offPeakHoursSplit = offPeakHours.split(",");
		Set<Integer> offPeakHoursSet = new HashSet<Integer>();
		for (String offPeakHourRange : offPeakHoursSplit) {
			if (offPeakHourRange.contains("-")) {
				String[] offPeakHourValues = offPeakHourRange.split("-");
				Integer lower = Integer.parseInt(offPeakHourValues[0]);
				Integer upper = Integer.parseInt(offPeakHourValues[1]);
				if (lower < upper) {
					for (Integer offPeakHour = lower; offPeakHour < upper; offPeakHour++) {
						offPeakHoursSet.add(offPeakHour);
					}
				}
			} else {
				offPeakHoursSet.add(Integer.parseInt(offPeakHourRange));
			}
		}
		return new ArrayList<Integer>(offPeakHoursSet);
	}
}

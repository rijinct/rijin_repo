
package com.project.rithomas.jobexecution.common.script;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.util.CommonAggregationUtil;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UsageAggregationStatusUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class AggScriptExecutor extends ScriptExecutor {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(AggScriptExecutor.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {

		String plevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		boolean isTzEnabledAndNotAgnostic = TimeZoneUtil
				.isTimezoneEnabledAndNotAgnostic(context);
		List<String> timeZones = Arrays
				.asList(JobExecutionContext.DEFAULT_TIMEZONE);
		if (isTzEnabledAndNotAgnostic) {
			timeZones = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
		}
		constructMapForQSJobs(context, timeZones);
		ExecutorService executors = Executors
				.newFixedThreadPool(timeZones.size());
		try {
			String additionalArgs = getAdditionalArgs(context);
			context.setProperty(ADDITIONAL_ARGS, additionalArgs);
			List<Future<Void>> futures = new ArrayList<>();
			for (String timezone : timeZones) {
				Long lb = getBoundValue(context, JobExecutionContext.LB,
						timezone);
				Long ub = getBoundValue(context, JobExecutionContext.UB,
						timezone);
				Long nextLb = getBoundValue(context,
						JobExecutionContext.NEXT_LB, timezone);
				if (isValidForAgg(lb, ub, nextLb)) {
					futures.add(doSubmit(executors, context, timezone, lb, ub,
							nextLb, plevel));
				} else {
					LOGGER.debug("For timezone: {}, lb: {}, ub: {}, nextLb: {}. Hence skipping.",
							timezone, lb, ub, nextLb);
				}
			}
			waitForCompletion(futures);
		} catch (Exception e) {
			throw new WorkFlowExecutionException(
					"Script execution failed:" + e.getMessage(), e);
		} finally {
			executors.shutdown();
			LOGGER.debug("Thread pool shutting down");
		}
		return true;
	}

	private void constructMapForQSJobs(WorkFlowContext context,
			List<String> timeZones) {
		Long minLb = getMinOfBoundValue(context, JobExecutionContext.LB,
				timeZones);
		Long maxUb = getMaxOfBoundValue(context, JobExecutionContext.UB,
				timeZones);
		CommonAggregationUtil.constructMapForDependentQSJob(context, minLb,
				maxUb);
	}

	private boolean isValidForAgg(Long lb, Long ub, Long nextLb) {
		return lb != null && ub != null && nextLb != null && nextLb <= ub;
	}

	private void waitForCompletion(List<Future<Void>> futures)
			throws InterruptedException, ExecutionException {
		for (Future<Void> future : futures) {
			future.get();
		}
	}

	private Long getMinOfBoundValue(WorkFlowContext context, String valueType,
			List<String> timezones) {
		List<Long> listOfValues = getListOfValues(context, valueType,
				timezones);
		return Collections.min(listOfValues);
	}
	
	private Long getMaxOfBoundValue(WorkFlowContext context, String valueType,
			List<String> timezones) {
		List<Long> listOfValues = getListOfValues(context, valueType,
				timezones);
		return Collections.max(listOfValues);
	}

	private List<Long> getListOfValues(WorkFlowContext context,
			String valueType, List<String> timezones) {
		List<Long> listOfValues = new ArrayList<>();
		for (String timezone : timezones) {
			Long boundValue = getBoundValue(context, valueType, timezone);
			if (boundValue != null) {
				listOfValues.add(boundValue);
			}
		}
		return listOfValues;
	}

	private Long getBoundValue(WorkFlowContext context, String valueType,
			String timezone) {
		Long value = (Long) context.getProperty(valueType);
		if (StringUtils.isNotEmpty(timezone) && !StringUtils
				.equals(JobExecutionContext.DEFAULT_TIMEZONE, timezone)) {
			value = (Long) context.getProperty(valueType + "_" + timezone);
		}
		return value;
	}

	private Future<Void> doSubmit(ExecutorService executors,
			WorkFlowContext context, String timezone, Long lb, Long ub,
			Long nextLb, String plevel) throws InterruptedException {
		Thread.sleep(2000L);
		ParallelLoading loading = new ParallelLoading(context, lb, ub, nextLb,
				timezone, plevel);
		return executors.submit(loading);
	}

	class ParallelLoading implements Callable<Void> {

		WorkFlowContext context;

		Long lb;

		Long ub;

		Long nextLb;

		String timezone;

		String plevel;

		public ParallelLoading(WorkFlowContext context, Long lb, Long ub,
				Long nextLb, String timezone, String plevel) {
			this.context = context;
			this.lb = lb;
			this.nextLb = nextLb;
			this.ub = ub;
			this.timezone = timezone;
			this.plevel = plevel;
		}

		@Override
		public Void call() throws Exception {
			executeScript(context, lb, ub, nextLb, timezone, plevel);
			return null;
		}

		private void executeScript(WorkFlowContext context, Long lb, Long ub,
				Long nextLb, String timezone, String plevel) throws Exception {
			DateFunctionTransformation dateTransformer = DateFunctionTransformation
					.getInstance();
			ScriptExecutor executor = new ScriptExecutor();
			while (nextLb <= ub && nextLb <= dateTransformer.getSystemTime()) {
				if (!isInterrupted) {
					LOGGER.info("For timezone: {}", timezone);
					LOGGER.info("Lower bound: {}",
							dateTransformer.getFormattedDate(lb));
					LOGGER.info("Next lower bound: {}",
							dateTransformer.getFormattedDate(nextLb));
					executor.execute(context, lb, nextLb, timezone);
					addHivePartition(context);
					context.setProperty(JobExecutionContext.QUERYDONEBOUND, lb);
					updateBoundary(context, timezone);
					UsageAggregationStatusUtil util = new UsageAggregationStatusUtil();
					util.updateUsageAggList(context,
							(String) context
									.getProperty(JobExecutionContext.JOB_NAME),
							dateTransformer.getFormattedDate(new Date(lb)),
							dateTransformer.getFormattedDate(new Date(nextLb)),
							getTzRegion(context, timezone));
					lb = nextLb;
					nextLb = dateTransformer.getNextBound(lb, plevel)
							.getTimeInMillis();
				} else {
					LOGGER.warn("Job interrupted. Hence exiting..");
					throw new WorkFlowExecutionException(
							"InterruptExpetion: Job was aborted by the user");
				}
			}
		}

		private void addHivePartition(WorkFlowContext context)
				throws Exception {
			schedulerJobRunner jobRunner = null;
			try {
				jobRunner = schedulerJobRunnerfactory.getRunner(
						schedulerConstants.HIVE_DATABASE,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				String query = "msck repair table %s";
				String targetTable = (String) context
						.getProperty(JobExecutionContext.TARGET);
				query = String.format(query, targetTable);
				jobRunner.runQuery(query);
			} finally {
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
			}
		}

		private void updateBoundary(WorkFlowContext context, String timezone)
				throws WorkFlowExecutionException {
			String region = getTzRegion(context, timezone);
			UpdateBoundary updateBoundary = new UpdateBoundary();
			updateBoundary.updateBoundaryTable(context, region);
		}

		private String getTzRegion(WorkFlowContext context, String timezone) {
			String region = null;
			if (TimeZoneUtil.isTimeZoneEnabled(context)) {
				region = timezone;
			}
			return region;
		}
	}
}

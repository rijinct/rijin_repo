
package com.project.rithomas.jobexecution.reaggregation;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.script.ScriptExecutor;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ReaggScriptExecutor extends ReaggregationHandler {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggScriptExecutor.class);

	private boolean isInterrupted = false;

	protected static final String ADDITIONAL_ARGS = "ADDITIONAL_ARGS";

	private static final String SCRIPT_NAME = "SCRIPT_NAME";

	protected boolean isArchivingEnabled;

	public ReaggScriptExecutor(WorkFlowContext context) {
		super(context);
	}

	@Override
	public void executeHandler(WorkFlowContext context)
			throws WorkFlowExecutionException {
		try {
			String aggJobName = (String) context
					.getProperty(JobExecutionContext.PERF_JOB_NAME);
			String additionalArgs = (String) context
					.getProperty(aggJobName + "_" + ADDITIONAL_ARGS);
			String scriptToExecute = (String) context
					.getProperty(aggJobName + "_" + SCRIPT_NAME);
			context.setProperty(ADDITIONAL_ARGS, additionalArgs);
			context.setProperty(SCRIPT_NAME, scriptToExecute);
			Long lb = (Long) context.getProperty(JobExecutionContext.REAGG_LB);
			Long nextLb = (Long) context
					.getProperty(JobExecutionContext.REAGG_NEXT_LB);
			boolean updateQsUpperBoundary = (boolean) context
					.getProperty(JobExecutionContext.REAGG_QS_UB_STATUS);
			this.isArchivingEnabled = ReaggCommonUtil
					.populateArchiveInfo(context);
			handleArchiving(context, lb);
			executeScript(lb, nextLb);
			if (updateQsUpperBoundary) {
				context.setProperty(JobExecutionContext.UB_QS_JOB, nextLb);
				context.setProperty(JobExecutionContext.REAGG_QS_UB_STATUS,
						false);
			}
		} catch (Exception e) {
			throw new WorkFlowExecutionException(
					"Script execution failed:" + e.getMessage(), e);
		} finally {
			LOGGER.info("Ending Script execution");
		}
	}

	private void handleArchiving(WorkFlowContext context, Long lb)
			throws Exception {
		if (isArchivingEnabled
				&& ReaggCommonUtil.isArchivedInDifferentTable(aggLevel)) {
			ReaggCommonUtil.dropPartitionifArchived(context, lb);
		}
	}

	private void executeScript(Long lb, Long nextLb) throws Exception {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		String timeZone = (String) context
				.getProperty(JobExecutionContext.REAGG_TZ_RGN);
		ScriptExecutor executor = new ScriptExecutor();
		if (!isInterrupted) {
			if (timeZone == null) {
				timeZone = "Default";
			}
			LOGGER.info("For timezone: {}", timeZone);
			LOGGER.info("Lower bound: {}",
					dateTransformer.getFormattedDate(lb));
			LOGGER.info("Next lower bound: {}",
					dateTransformer.getFormattedDate(nextLb));
			executor.execute(context, lb, nextLb, timeZone);
		} else {
			LOGGER.warn("Job interrupted. Hence exiting..");
			throw new WorkFlowExecutionException(
					"InterruptExpetion: Job was aborted by the user");
		}
	}
}

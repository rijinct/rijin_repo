
package com.project.rithomas.jobexecution.archiving;

import org.apache.commons.lang.math.NumberUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.jobexecution.common.util.ReaggregationListUtil;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ArchivePartitionExecutor extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ArchivePartitionExecutor.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		setRetentionDays(context);
		ReaggregationListUtil reaggUtil = new ReaggregationListUtil();
		try {
			boolean isArchiveEabled = PartitionUtil.isArchiveEnabled(context);
			if (isArchiveEabled
					&& !reaggUtil
					.isAnyReaggDependentJobsActive(context,
							getArchivingDays(context))) {
				LOGGER.info("Archiving the partitions.");
				getArchivePartitioner(context).archive(context);
			} else if (!isArchiveEabled) {
				LOGGER.info("Skipping Archiving as Archiveing is not enabled.");
			} else {
				LOGGER.info(
						"Skipping Archiving partitions as Historic reaggregation dependent jobs are still active.. ");
			}
		} catch (JobExecutionException e) {
			throw new WorkFlowExecutionException(e.getMessage());
		}
		return success;
	}

	private int getArchivingDays(WorkFlowContext context) {
		return NumberUtils.toInt((String) context.getProperty(
				JobExecutionContext.ARCHIVING_DAYS));
	}

	private void setRetentionDays(WorkFlowContext context) {
		String sourceJobName = (String) context
				.getProperty(JobExecutionContext.SOURCEJOBID);
		context.setProperty(JobExecutionContext.RETENTIONDAYS,
				(String) context.getProperty(sourceJobName + "_RETENTIONDAYS"));
	}

	public static BaseArchivePartitioner getArchivePartitioner(
			WorkFlowContext context) throws WorkFlowExecutionException {
		return shouldArchiveInArchiveTable(context)
				? new ArchivePartitionInArchiveTable(context)
				: new ArchivePartitionInSameTable(context);
	}

	private static boolean shouldArchiveInArchiveTable(
			WorkFlowContext context) {
		return JobTypeDictionary.USAGE_JOB_TYPE.equals(
				(String) context.getProperty(JobExecutionContext.SOURCEJOBTYPE))
				|| JobExecutionContext.ARCHIVING_DAY_JOB_INTERVALS
						.contains((String) context
								.getProperty(JobExecutionContext.PLEVEL));
	}
}
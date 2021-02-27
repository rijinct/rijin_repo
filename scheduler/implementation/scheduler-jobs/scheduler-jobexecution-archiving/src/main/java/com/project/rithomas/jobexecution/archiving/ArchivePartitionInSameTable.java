
package com.project.rithomas.jobexecution.archiving;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.PartitionUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class ArchivePartitionInSameTable extends BaseArchivePartitioner {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ArchivePartitionInSameTable.class);

	public ArchivePartitionInSameTable(WorkFlowContext context)
			throws WorkFlowExecutionException {
		super(context);
	}

	@Override
	public void archive(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		try {
			for (String region : tzRegions) {
				archivePartitions(context, targetTable, region);
			}
		} catch (Exception e) {
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					e.getMessage());
			LOGGER.error("Exception encountered while archiving {}", e);
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException("Exception: " + e.getMessage(),
					e);
		} finally {
			closeConnection();
		}
	}

	private void archivePartitions(WorkFlowContext context, String targetTable, String region)
			throws Exception {
		boolean shouldgetOnlyFirstPartition = false;
		Long archivingDayValue = getArchivingDayValue(context, region);
		List<String> partitions = PartitionUtil.showPartitionsResult(context,
				targetTable, shouldgetOnlyFirstPartition);
		String partitionColumn = PartitionUtil.getPartitionColumn(context);
		Map<String, String> archiveTableTemplate = getArchiveTableTemplate(
				targetTable, partitionColumn);
		Long maxDayValue = getMaxDayValue(context, region);
		try {
			Set<Long> distictPartitionValues = getDistinctPartitions(
					partitionColumn, partitions, region);
			for (Long partitionVal : distictPartitionValues) {
				LOGGER.debug("partition Value : {}", partitionVal);
				if (shouldArchivePartition(archivingDayValue, maxDayValue,
						partitionVal)) {
					archiveEachPartition(archiveTableTemplate, partitionVal,
							context, region);
				} else if (partitionVal > archivingDayValue) {
					break;
				}
			}
		} catch (Exception e) {
			throw new WorkFlowExecutionException(
					"Exception during archiving each partition ", e);
		}
	}

	private boolean shouldArchivePartition(Long archivingDayValue,
			Long maxDayValue, Long partitionVal) {
		return partitionVal < archivingDayValue
				&& (maxDayValue == null || partitionVal > maxDayValue);
	}
}

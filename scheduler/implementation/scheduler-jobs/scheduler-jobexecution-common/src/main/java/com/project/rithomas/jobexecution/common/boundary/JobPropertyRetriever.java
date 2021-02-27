
package com.project.rithomas.jobexecution.common.boundary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class JobPropertyRetriever {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobPropertyRetriever.class);

	private static final String PARTITION_CHECK_REQUIRED = "_PARTITION_CHECK_REQUIRED";

	private static final String PARTITION_UPDATE_FORMULA = "_PARTITION_UPDATE_FORMULA";

	private static final String RETENTION_DAYS = "_RETENTIONDAYS";

	private WorkFlowContext context;

	public JobPropertyRetriever(WorkFlowContext context) {
		this.context = context;
	}

	public int getAggRetentionDays() {
		int retentionDays = 0;
		if (context.getProperty(JobExecutionContext.MAXVALUE) == null) {
			retentionDays = getCalculatedRetentionDays();
			LOGGER.debug(
					"Aggregation is being done for the first time. Hence, retrieving LB based on retentiondays : {}",
					retentionDays);
		} else {
			retentionDays = getRetentionDays();
		}
		return retentionDays;
	}

	private int getCalculatedRetentionDays() {
		int srcRetentionDays = getSrcRetentionDays(context);
		int jobRetentionDays = getRetentionDays();
		int retentionDays = jobRetentionDays;
		if (jobRetentionDays > srcRetentionDays) {
			retentionDays = srcRetentionDays;
		}
		return retentionDays;
	}

	public int getRetentionDays() {
		return Integer.parseInt((String) context
				.getProperty(JobExecutionContext.RETENTIONDAYS));
	}

	@SuppressWarnings("unchecked")
	private int getSrcRetentionDays(WorkFlowContext context) {
		int retentionDays = Integer.parseInt((String) context
				.getProperty(JobExecutionContext.RETENTIONDAYS));
		List<String> sourceJobIdList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		for (String jobName : sourceJobIdList) {
			Object eachSrcRetentionDays = context
					.getProperty(new StringBuilder(jobName)
							.append(RETENTION_DAYS).toString());
			if (eachSrcRetentionDays != null) {
				int srcRetentionDays = Integer
						.parseInt((String) eachSrcRetentionDays);
				if (retentionDays > srcRetentionDays) {
					retentionDays = srcRetentionDays;
				}
			}
		}
		LOGGER.debug("Source Retention Days is : {}", retentionDays);
		return retentionDays;
	}

	public boolean isPlevelHourOr15Min() {
		String performanceLevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		return JobExecutionContext.MIN_15.equalsIgnoreCase(performanceLevel)
				|| JobExecutionContext.HOUR.equalsIgnoreCase(performanceLevel);
	}

	public String getAggregationLevel() {
		return (String) context.getProperty(JobExecutionContext.ALEVEL);
	}

	public String getPartitionLevel() {
		return (String) context.getProperty(JobExecutionContext.PLEVEL);
	}

	public Integer getAggregationRetentionDays() {
		return (Integer) context
				.getProperty(JobExecutionContext.AGG_RETENTIONDAYS);
	}

	@SuppressWarnings("unchecked")
	public List<String> getExcludedStageTypes() {
		return (String) context.getProperty(
				JobExecutionContext.EXCLUDE_STAGE_TYPE) != null ? (List) Arrays
						.asList(StringUtils.split((String) context.getProperty(
								JobExecutionContext.EXCLUDE_STAGE_TYPE)), ",")
						: Collections.EMPTY_LIST;
	}

	@SuppressWarnings("unchecked")
	public boolean arePartitionsLoadedInCouchbase() {
		boolean partitionsLoadedInCouchbase = false;
		List<String> sourceJobIdList = (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
		if (isSourcePartitionCheckRequired(sourceJobIdList)
				&& !doesAllPartitionSubFormulaContainHiphen(sourceJobIdList)) {
			partitionsLoadedInCouchbase = true;
		}
		return partitionsLoadedInCouchbase;
	}

	private boolean isSourcePartitionCheckRequired(
			List<String> sourceJobIDList) {
		return BooleanUtils.toBoolean(String.valueOf(
				context.getProperty(new StringBuilder(sourceJobIDList.get(0))
						.append(PARTITION_CHECK_REQUIRED).toString())));
	}

	private boolean doesAllPartitionSubFormulaContainHiphen(
			List<String> sourceJobIDList) {
		String partitionUpdateFormula = (String) context
				.getProperty(new StringBuilder(sourceJobIDList.get(0))
						.append(PARTITION_UPDATE_FORMULA).toString());
		boolean allPartitionSubFormulaContainsHiphen = true;
		if (StringUtils.isNotEmpty(partitionUpdateFormula)) {
			String[] subFormulaArray = partitionUpdateFormula.split(",");
			for (String eachSubFormula : subFormulaArray) {
				if (!eachSubFormula.contains("-")) {
					allPartitionSubFormulaContainsHiphen = false;
					break;
				}
			}
		}
		return allPartitionSubFormulaContainsHiphen;
	}
}

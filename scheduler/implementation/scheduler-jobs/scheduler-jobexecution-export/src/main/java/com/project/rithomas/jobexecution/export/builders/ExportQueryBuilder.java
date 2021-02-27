
package com.project.rithomas.jobexecution.export.builders;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ExportQueryBuilder {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ExportQueryBuilder.class);

	private String jobId;

	private boolean isNotEntityJob;

	public ExportQueryBuilder(String jobId, boolean isNotEntityJob) {
		this.jobId = jobId;
		this.isNotEntityJob = isNotEntityJob;
	}

	public String build(WorkFlowContext context, Long lb, Long nextLb) {
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		String sqlToExecute;
		LOGGER.debug("Export Query to be executed:: {} ", sql);
		if (isNotEntityJob) {
			sqlToExecute = sql
					.replace(JobExecutionContext.LOWER_BOUND, lb.toString())
					.replace(JobExecutionContext.UPPER_BOUND,
							nextLb.toString());
			if (jobId.contains("Exp_APNHLR_EXPORT_1_HOUR_ExportJob")) {
				Long maxImsiApnExport = (Long) context
						.getProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT);
				sqlToExecute = sqlToExecute
						.replace("'" + JobExecutionContext.MAX_IMSI_APN_EXPORT
								+ "'", String.valueOf(maxImsiApnExport))
						.replace(JobExecutionContext.LOWERBOUND_DATE,
								DateFunctionTransformation.getInstance()
										.getFormattedDate(lb));
			}
		} else {
			sqlToExecute = sql;
		}
		return sqlToExecute;
	}
}

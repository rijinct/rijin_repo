
package com.project.rithomas.jobexecution.reaggregation;

import java.sql.SQLException;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.aggregation.SubPartitionUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class HiveReaggregation extends ReaggregationHandler {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveReaggregation.class);

	private schedulerJobRunner jobRunner = null;

	private HiveQueryBuilder hiveQueryBuilder = null;

	private BoundaryQuery boundaryQuery = new BoundaryQuery();

	public HiveReaggregation(WorkFlowContext context) throws Exception {
		super(context);
		String executionEngine = JobExecutionUtil.getExecutionEngine(context);
		jobRunner = schedulerJobRunnerfactory.getRunner(executionEngine,
				(boolean) context
						.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
		hiveQueryBuilder = new HiveQueryBuilder(context, jobRunner);
	}

	@Override
	public void executeHandler(WorkFlowContext context) {
		try {
			String jobId = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			List<String> sourceJobs = boundaryQuery.getSourceJobIds(jobId);
			Long reaggLb = (Long) context
					.getProperty(JobExecutionContext.REAGG_LB);
			boolean updateQsUpperBoundary = (boolean) context
					.getProperty(JobExecutionContext.REAGG_QS_UB_STATUS);
			String regionId = (String) context
					.getProperty(JobExecutionContext.REAGG_TZ_RGN);
			String reaggLbTime = dateTransformer.getFormattedDate(reaggLb);
			jobRunner.setQueryHint(QueryConstants.DISABLE_COMPRESSION_COMM);
			String sqlToExecute = hiveQueryBuilder.constructQuery(reaggLbTime,
					updateQsUpperBoundary, sourceJobs);
			context.setProperty(JobExecutionContext.FLAG, "false");
			enableCompression();
			handleArchiving(context, reaggLb);
			LOGGER.info("Reaggregation for the time: {} starts now.",
					regionId == null ? reaggLbTime
							: reaggLbTime + ", region: " + regionId);
			Thread logThread = applicationLogger.startApplicationIdLog(
					Thread.currentThread(),
					jobRunner.getPreparedStatement(sqlToExecute), jobId,
					(String) context.getProperty(
							JobExecutionContext.APPLICATION_ID_LOGGER_SLEEP_TIME_IN_SEC));
			jobRunner.runQuery(sqlToExecute);
			ReaggCommonUtil.printRecordCount(regionId, reaggLb, context,
					applicationLogger);
			applicationLogger.stopApplicationIdLog(logThread);
			LOGGER.info("Reaggregation completed for the time: {}",
					regionId == null ? reaggLbTime
							: reaggLbTime + ", region: " + regionId);
			SubPartitionUtil partUtil = new SubPartitionUtil();
			partUtil.storeParitionInfo(context, jobRunner, regionId != null
					? regionId : JobExecutionContext.DEFAULT_TIMEZONE, reaggLb);
			LOGGER.debug("Getting number of records inserted");
		} catch (Exception ex) {
			LOGGER.error("Exception occured", ex);
		} finally {
			try {
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
			} catch (Exception e) {
				LOGGER.warn("Exception while clossing the connection {}", e);
			}
		}
	}

	private void handleArchiving(WorkFlowContext context, Long reaggLb)
			throws Exception {
		if (isArchivingEnabled
				&& !ReaggCommonUtil.isArchivedInDifferentTable(aggLevel)) {
			ReaggCommonUtil.dropPartitionifArchived(context, reaggLb);
		}
	}

	private void enableCompression() throws SQLException {
		for (String enableCompress : QueryConstants.ENABLE_COMPRESSION_COMM) {
			jobRunner.setQueryHint(enableCompress);
		}
	}
}

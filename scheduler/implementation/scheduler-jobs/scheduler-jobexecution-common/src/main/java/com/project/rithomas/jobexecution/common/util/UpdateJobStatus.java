
package com.project.rithomas.jobexecution.common.util;

import java.math.BigInteger;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class UpdateJobStatus {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UpdateJobStatus.class);

	public void updateFinalJobStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		try {
			String status = (String) context
					.getProperty(JobExecutionContext.STATUS);
			LOGGER.info("Updating the final Job Status to {}", status);
			BigInteger nextVal = (BigInteger) context
					.getProperty(JobExecutionContext.ETL_STATUS_SEQ);
			LOGGER.debug("next value: {}", nextVal);
			String description = (String) context
					.getProperty(JobExecutionContext.DESCRIPTION);
			String errorDescription = (String) context
					.getProperty(JobExecutionContext.ERROR_DESCRIPTION);
			String updateSql = null;
			if (errorDescription != null) {
				errorDescription = errorDescription.replace("'", "''");
			}
			if ("I".equalsIgnoreCase(status)) {
				updateSql = QueryConstants.ETL_STATUS_UPDATE_FOR_WAITING;
			} else {
				updateSql = QueryConstants.ETL_STATUS_UPDATE;
			}
			updateSql = updateSql
					.replace(QueryConstants.STATUS, "'" + status + "'")
					.replace(QueryConstants.DESCRIPTION,
							"'" + description + "'")
					.replace(QueryConstants.ERROR_DESCRIPTION,
							"'" + errorDescription + "'")
					.replace(QueryConstants.PROC_ID, nextVal.toString());
			
			QueryExecutor queryExecutor = new QueryExecutor();
			queryExecutor.executePostgresqlUpdate(updateSql, context);
		} catch (JobExecutionException e) {
			LOGGER.error("Exception while executing query {}" + e.getMessage());
			throw new WorkFlowExecutionException(
					"Exception while executing query", e);
		}
	}

	public void insertJobStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		try {
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String status = (String) context
				.getProperty(JobExecutionContext.STATUS);
		String description = (String) context
				.getProperty(JobExecutionContext.DESCRIPTION);
		
			LOGGER.info("Inserting ETL_STATUS  to {}", status);
			Object nextVal = 0;
			String sqlForSeq = "select nextval('rithomas.ETL_STAT_SEQ')";
			QueryExecutor queryExecutor = new QueryExecutor();
			
			LOGGER.debug(" insertJobStatus executeMetadatasqlQuery : sqlForSeq {}", sqlForSeq);
			Object[] resultSet = queryExecutor.executeMetadatasqlQuery(
					sqlForSeq, context);
			if (resultSet != null) {
				nextVal = resultSet[0];
			}
			context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, nextVal);
			String etlStatusInsert = null;
			if ("W".equalsIgnoreCase(status)) {
				etlStatusInsert = QueryConstants.ETL_STATUS_INSERT_FOR_WAITING
						.replace(QueryConstants.PROC_ID, nextVal.toString())
						.replace("$JOBNAME", "'" + jobName + "'")
						.replace("$JOBTYPE", "'" + jobType + "'")
						.replace(QueryConstants.STATUS, "'" + status + "'")
						.replace(QueryConstants.DESCRIPTION,
								"'" + description + "'");
			} else {
				etlStatusInsert = QueryConstants.ETL_STATUS_INSERT
						.replace(QueryConstants.PROC_ID, nextVal.toString())
						.replace("$JOBNAME", "'" + jobName + "'")
						.replace("$JOBTYPE", "'" + jobType + "'")
						.replace(QueryConstants.STATUS, "'" + status + "'");
			}
			queryExecutor.executePostgresqlUpdate(etlStatusInsert, context);
		} catch (Exception e) {
			LOGGER.error("Exception while executing query", e);
			throw new WorkFlowExecutionException(
					"Exception while executing query", e);
		}
	}

	public void updateETLErrorStatusInTable(Exception ex,
			WorkFlowContext context) throws WorkFlowExecutionException {
		if (ex != null && context != null) {
			LOGGER.error(" Exception {}", ex.getMessage(), ex);
			context.setProperty(JobExecutionContext.STATUS, "E");
			context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
					ex.getMessage() == null ? ex.getClass().toString()
							: ex.getMessage());
			this.updateFinalJobStatus(context);
		}
	}
}

package com.project.rithomas.jobexecution.export;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.List;

import com.rijin.analytics.exceptions.InternalServerErrorException;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.sdk.export.client.ExportServiceClient;
import com.rijin.analytics.sdk.export.model.ExportConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.jobexecution.export.builders.ExportConfigurationBuilder;
import com.project.rithomas.jobexecution.export.builders.ExportQueryBuilder;
import com.project.rithomas.jobexecution.export.executors.ExportToHbaseExecutor;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

public class ExportManager extends AbortableWorkflowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ExportManager.class);

	public static final String FILE_SEP = System.getProperty("file.separator",
			"/");

	private static final String UNDERSCORE = "_";

	private static final String HYPHEN = "-";

	private String sourceJobType;

	private ExportToHbaseExecutor exportToHbaseExecutor = new ExportToHbaseExecutor();

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		sourceJobType = (String) context
				.getProperty(JobExecutionContext.SOURCEJOBTYPE);
		try {
			performExportOperation(context);
			success = true;
		} catch (Exception e) {
			updateErrorStatus(context, e);
			throw new WorkFlowExecutionException("Excpetion: " + e.getMessage(),
					e);
		}
		return success;
	}

	private void performExportOperation(WorkFlowContext context)
			throws Exception {
		LOGGER.info("Executing Export Query::");
		Long lb = (Long) context.getProperty(JobExecutionContext.LB);
		Long ub = (Long) context.getProperty(JobExecutionContext.UB);
		Long nextLb = (Long) context.getProperty(JobExecutionContext.NEXT_LB);
		boolean isJobTypeHbaseLoader = Boolean.parseBoolean((String) context
				.getProperty(JobExecutionContext.HIVE_TO_HBASE_LOADER));
		while (isSourceJobEntity() || isNextLBValid(ub, nextLb)) {
			ReConnectUtil reConnectUtil = new ReConnectUtil();
			while (reConnectUtil.shouldRetry()) {
				try {
					if (!interrupted) {
						if (isJobTypeHbaseLoader) {
							exportToHbaseExecutor.execute(context, lb, nextLb);
						} else {
							executeExportQuery(context, lb, nextLb);
						}
						UpdateBoundary updateBoundary = new UpdateBoundary();
						updateBoundary.updateBoundaryTable(context, null);
						if (!isSourceJobEntity()) {
							lb = nextLb;
							nextLb = calculateNextLB(context, nextLb);
						}
					} else {
						performInterruptOperation(context);
					}
					break;
				} catch (SQLException | UndeclaredThrowableException e) {
					updateErrorStatus(context, e);
					if (reConnectUtil.isRetryRequired(e.getMessage())) {
						reConnectUtil.updateRetryAttemptsForHive(e.getMessage(),
								context);
					} else {
						throw new WorkFlowExecutionException(
								"SQLException: " + e.getMessage(), e);
					}
				}
			}
			if (isSourceJobEntity()) {
				break;
			}
		}
	}

	private boolean isSourceJobEntity() {
		return JobTypeDictionary.ENTITY_JOB_TYPE
				.equalsIgnoreCase(sourceJobType);
	}

	private void updateErrorStatus(WorkFlowContext context, Exception e)
			throws WorkFlowExecutionException {
		LOGGER.error("Exception {}", e.getMessage(), e);
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				e.getMessage());
		updateErrorStatus(context);
	}

	private boolean isNextLBValid(Long ub, Long nextLb) {
		return nextLb <= ub && nextLb <= DateFunctionTransformation
				.getInstance().getSystemTime();
	}

	private void executeExportQuery(WorkFlowContext context, Long lb,
			Long nextLb) throws SQLException, JobExecutionException,
			WorkFlowExecutionException {
		ExportConfiguration configuration = new ExportConfigurationBuilder()
				.build(context);
		ExportQueryBuilder queryBuilder = new ExportQueryBuilder(
				configuration.getJobName(), !isSourceJobEntity());
		String sqlToExecute = queryBuilder.build(context, lb, nextLb);
		LOGGER.debug("SQL to execute: {}", sqlToExecute);
		context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY,
				sqlToExecute);
		configuration.setHints(getHints(configuration.getJobName(),
				configuration.getExecutionEngine()));
		configuration.setQuery(sqlToExecute);
		configuration.setFileName(getFileName(context, lb, nextLb));
		LOGGER.info("Confguration built to perform export {}", configuration);
		executeExportRequest(configuration);
		if (lb != null) {
			context.setProperty(JobExecutionContext.QUERYDONEBOUND, lb);
		} else {
			context.setProperty(JobExecutionContext.QUERYDONEBOUND,
					DateFunctionTransformation.getInstance().getSystemTime());
		}
	}

	private void executeExportRequest(ExportConfiguration configuration) {
		try {
			ExportServiceClient exportService = new ExportServiceClient(
					configuration);
			exportService.execute();
		} catch (Exception exception) {
			LOGGER.error(
					"Exception occured when executing export on export service",
					exception);
			throw new InternalServerErrorException();
		}
	}

	private List<String> getHints(String jobId, String executionEngine)
			throws WorkFlowExecutionException {
		List<String> queryHints = HiveConfigurationProvider.getInstance()
				.getQueryHints(jobId, executionEngine);
		queryHints.add(QueryConstants.DISABLE_COMPRESSION_COMM);
		return queryHints;
	}

	private String getFileName(WorkFlowContext context, Long lowerBound,
			Long upperBound) {
		String fileName = (String) context
				.getProperty(GeneratorWorkFlowContext.FILENAME);
		if (!isSourceJobEntity()) {
			String jobId = (String) context
					.getProperty(JobExecutionContext.JOB_NAME);
			if (jobId.contains("SQM")) {
				fileName = fileName + HYPHEN + lowerBound + UNDERSCORE
						+ upperBound;
			} else {
				fileName = fileName + UNDERSCORE + lowerBound + UNDERSCORE
						+ upperBound;
			}
		}
		return fileName;
	}

	private Long calculateNextLB(WorkFlowContext context, Long nextLb) {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		LOGGER.info("New LowerBound value: {}",
				dateTransformer.getFormattedDate(nextLb));
		Long newNextLb = dateTransformer
				.getNextBound(nextLb,
						(String) context
								.getProperty(JobExecutionContext.INTERVAL))
				.getTimeInMillis();
		LOGGER.info("New Next LowerBound value: {}",
				dateTransformer.getFormattedDate(newNextLb));
		return newNextLb;
	}

	private void performInterruptOperation(WorkFlowContext context)
			throws WorkFlowExecutionException {
		LOGGER.debug("The process is interrupted.");
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				"Interruped by User.");
		context.setProperty(JobExecutionContext.DESCRIPTION,
				"Job was aborted by user.");
		updateErrorStatus(context);
		throw new WorkFlowExecutionException(
				"InterruptException: Job was aborted by the user");
	}

	private void updateErrorStatus(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		context.setProperty(JobExecutionContext.STATUS, "E");
		updateJobStatus.updateFinalJobStatus(context);
	}
}

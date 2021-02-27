
package com.project.rithomas.jobexecution.common.script;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.exception.HiveQueryException;
import com.project.rithomas.jobexecution.common.util.CommonCorrelationUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderExecuter;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class ScriptExecutor extends AbstractWorkFlowStep {

	protected static final String ADDITIONAL_ARGS = "ADDITIONAL_ARGS";

	private static final String SCRIPT_NAME = "SCRIPT_NAME";

	boolean isInterrupted = false;

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ScriptExecutor.class);

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		if (JobTypeDictionary.ENTITY_JOB_TYPE.equalsIgnoreCase(
				(String) context.getProperty(JobExecutionContext.JOBTYPE))) {
			CommonCorrelationUtil.constructMapForDependentQSJob(context);
		}
		return execute(context, null, null, null);
	}

	public boolean execute(WorkFlowContext context, Long lowerBound,
			Long upperBound, String timezone)
			throws WorkFlowExecutionException {
		String sourceTables = (String) context
				.getProperty(JobExecutionContext.SOURCE);
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String scriptToExecute = (String) context.getProperty(SCRIPT_NAME);
		List<String> commandWithArgs = new ArrayList<>();
		commandWithArgs.add(scriptToExecute);
		try {
			addSourceTargetAsArgs(context, sourceTables, targetTable,
					commandWithArgs);
			addBoundaryValuesToArgs(lowerBound, upperBound, timezone,
					commandWithArgs);
			handleAdditionalArgs(context, commandWithArgs);
			LOGGER.debug("arguments: {}", commandWithArgs);
			ProcessBuilderExecuter executor = new ProcessBuilderExecuter();
			executor.executeCommand(context, getConfig(), commandWithArgs,
					null);
			LOGGER.info("Script execution done.");
		} catch (HiveQueryException e) {
			LOGGER.error("Failed to get hdfs location: {}", e.getMessage(), e);
			throw new WorkFlowExecutionException("Failed to get hdfs location",
					e);
		} catch (Exception e) {
			LOGGER.error("Failed executing script: {}", e.getMessage(), e);
			throw new WorkFlowExecutionException("Failed executing script", e);
		}
		return true;
	}

	private void addBoundaryValuesToArgs(Long lowerBound, Long upperBound,
			String timezone, List<String> commandWithArgs) {
		if (lowerBound != null) {
			commandWithArgs.add("-l");
			commandWithArgs.add(lowerBound.toString());
		}
		if (upperBound != null) {
			commandWithArgs.add("-u");
			commandWithArgs.add(upperBound.toString());
		}
		if (timezone != null) {
			commandWithArgs.add("-tz");
			commandWithArgs.add(timezone);
		}
	}

	private void addSourceTargetAsArgs(WorkFlowContext context,
			String sourceTables, String targetTable,
			List<String> commandWithArgs) throws HiveQueryException {
		String sourceLocations = StringUtils.join(HiveTableQueryUtil
				.retrieveHDFSLocationGivenTableName(context, sourceTables),
				',');
		String targetLocation = HiveTableQueryUtil
				.retrieveHDFSLocationGivenTableName(context, targetTable)
				.get(0);
		commandWithArgs.add("-t");
		commandWithArgs.add(targetLocation);
		commandWithArgs.add("-s");
		commandWithArgs.add(sourceLocations);
	}

	void handleAdditionalArgs(WorkFlowContext context,
			List<String> commandWithArgs) throws Exception {
		String additionalArgs = getAdditionalArgs(context);
		if (additionalArgs != null) {
			String[] argArray = StringUtils.split(additionalArgs, ",");
			for (String arg : argArray) {
				commandWithArgs.add(arg);
			}
		}
	}

	String getAdditionalArgs(WorkFlowContext context) throws Exception {
		String additionalArgs = (String) context.getProperty(ADDITIONAL_ARGS);
		if (additionalArgs != null) {
			LOGGER.debug("Additional arguments configured: {}", additionalArgs);
			schedulerJobRunner jobRunner = null;
			try {
				jobRunner = schedulerJobRunnerfactory.getRunner(
						schedulerConstants.HIVE_DATABASE,
						(boolean) context.getProperty(
								JobExecutionContext.IS_CUSTOM_DB_URL));
				additionalArgs = RetrieveDimensionValues
						.replaceDynamicValuesInJobSQL(context, additionalArgs,
								jobRunner);
			} finally {
				if (jobRunner != null) {
					jobRunner.closeConnection();
				}
			}
			LOGGER.debug(
					"Additional arguments after replacing dynamic values: {}",
					additionalArgs);
		}
		return additionalArgs;
	}

	@Override
	public boolean abort(WorkFlowContext context) {
		isInterrupted = true;
		return true;
	}
}


package com.project.rithomas.jobexecution.project;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.rijin.scheduler.jobexecution.hive.query.HiveTableQueryUtil;
import com.project.rithomas.jobexecution.common.ApplicationLoggerFactory;
import com.project.rithomas.jobexecution.common.ApplicationLoggerUtilInterface;
import com.project.rithomas.jobexecution.common.UpdateBoundary;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.step.AbortableWorkflowStep;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DistinctSubscriberCountUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.ReConnectUtil;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.common.CharacteristicSpecification;
import com.project.rithomas.sdk.model.usage.UsageSpecification;
import com.project.rithomas.sdk.model.usage.UsageSpecificationCharacteristicUse;
import com.project.rithomas.sdk.model.usage.query.UsageSpecificationQuery;
import com.project.rithomas.sdk.model.utils.ModelUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.CodeGeneratorException;
import com.project.rithomas.sdk.workflow.generator.util.MultipleSourceGetParams;

// import org.antlr.stringtemplate.StringTemplateGroup;
public class GetSubscriberCountDetails extends AbortableWorkflowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(GetSubscriberCountDetails.class);

	private static final String HIVE_PARTITION_COLUMN = "dt";

	private static final String COLUMN_NAME = "imsi_id";

	protected Map<String, List<String>> sourceTableDimensionsMap = new HashMap<String, List<String>>();

	protected static final String SOURCE_PERF_SPEC_PARAMS = "SOURCE_PERF_SPEC_PARAMS";

	private boolean interrupted;

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String sqlTemplate = (String) context
				.getProperty(JobExecutionContext.SQL);
		ApplicationLoggerUtilInterface applicationLogger = ApplicationLoggerFactory
				.getApplicationLogger();
		context.setProperty(JobExecutionContext.APPLICATION_ID_LOGGER,
				applicationLogger);
		LOGGER.debug("template: {}", sqlTemplate);
		StringTemplate codeFromSt = new StringTemplate(sqlTemplate);
		schedulerJobRunner jobRunner = null;
		boolean success = false;
		Calendar calendarTemp = new GregorianCalendar();
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		UpdateBoundary updateBoundary = new UpdateBoundary();
		String executionEngine = JobExecutionUtil.getExecutionEngine(context);
		try {
			codeFromSt.setAttribute(SOURCE_PERF_SPEC_PARAMS,
					getMultiplePSSourceGetParams(context));
		} catch (CodeGeneratorException e) {
			throw new WorkFlowExecutionException(
					"CodeGeneratorException: " + e.getMessage(), e);
		} catch (JobExecutionException e) {
			throw new WorkFlowExecutionException(
					"JobExecutionException: " + e.getMessage(), e);
		}
		String sql = codeFromSt.toString();
		Long lb = (Long) context.getProperty(JobExecutionContext.LB);
		Long nextLb = (Long) context.getProperty(JobExecutionContext.NEXT_LB);
		Long ub = (Long) context.getProperty(JobExecutionContext.UB);
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		String sqlToExecute = null;
		Calendar calendarReportDate = new GregorianCalendar();
		LOGGER.debug("LB: {} UB: {} nextLB: {}",
				new Object[] { lb, ub, nextLb });
		try {
			ReConnectUtil reConnectUtil = new ReConnectUtil();
			while (reConnectUtil.shouldRetry()) {
				try {
					jobRunner = schedulerJobRunnerfactory.getRunner(
							executionEngine, (boolean) context.getProperty(
									JobExecutionContext.IS_CUSTOM_DB_URL));
					// Executing Hive UDF functions
					for (String hiveUDF : HiveConfigurationProvider
							.getInstance()
							.getQueryHints(jobId, executionEngine)) {
						jobRunner.setQueryHint(hiveUDF);
					}
					while (nextLb <= ub
							&& nextLb <= dateTransformer.getSystemTime()) {
						if (!interrupted) {
							// connTimer = new Timer();
							calendarReportDate.setTimeInMillis(lb);
							LOGGER.debug("reportdate: {}",
									dateTransformer.getFormattedDate(
											calendarReportDate.getTime()));
							sqlToExecute = sql
									.replace(JobExecutionContext.LOWER_BOUND,
											lb.toString())
									.replace(JobExecutionContext.UPPER_BOUND,
											nextLb.toString())

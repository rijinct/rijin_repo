
package com.project.rithomas.jobexecution.export;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.LBUBUtil;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunnerfactory;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class ExportLBUBCalculator extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ExportLBUBCalculator.class);

	private static final String NO_COMPLETE_DATA = "No Complete Data to Export further on";

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = false;
		LOGGER.info("Calculating LB and UB for Export Job");
		populateContextWithSrcBoundaryList(context);
		Calendar calendarLB = calculateLB(context);
		Long finalLb = populateContextWithLB(context, calendarLB);
		Calendar calendarUB = calculateUB(context);
		populateContextWithUB(context, calendarUB);
		Calendar calendarNB = populateContextWithNB(context, finalLb);
		LOGGER.info("LowerBound value is : {}",
				calendarLB == null ? null : DateFunctionTransformation
						.getInstance().getFormattedDate(calendarLB.getTime()));
		LOGGER.info("UpperBound value is : {}",
				calendarUB == null ? null : DateFunctionTransformation
						.getInstance().getFormattedDate(calendarUB.getTime()));
		LOGGER.info("Nextlowerbound value is : {}",
				calendarNB == null ? null : DateFunctionTransformation
						.getInstance().getFormattedDate(calendarNB.getTime()));
		populateContextWithReturnVal(context, calendarNB);
		success = true;
		return success;
	}

	private void populateContextWithSrcBoundaryList(WorkFlowContext context) {
		LBUBUtil lbubUtil = new LBUBUtil();
		List<Boundary> srcBoundaryList = lbubUtil
				.getSourceBoundaryList(context);
		context.setProperty(JobExecutionContext.SOURCE_BOUND_LIST,
				srcBoundaryList);
	}

	private Calendar calculateLB(WorkFlowContext context) {
		LOGGER.debug("Calculating LB::");
		Calendar calendarLB = null;
		Timestamp maxValue = (Timestamp) context
				.getProperty(JobExecutionContext.MAXVALUE);
		LOGGER.debug("Max Value:: {}", maxValue);
		if (maxValue == null) {
			calendarLB = calculateLbFromSrcTable(context);
		} else {
			calendarLB = calculateLbFromMaxValue(context);
		}
		LOGGER.debug("LB VALUE CALCULATED IS {}", calendarLB);
		return calendarLB;
	}

	private Calendar calculateLbFromSrcTable(WorkFlowContext context) {
		schedulerJobRunner jobRunner = null;
		Calendar calendarLB = null;
		try {
			jobRunner = schedulerJobRunnerfactory.getRunner(
					schedulerConstants.HIVE_DATABASE, (boolean) context
							.getProperty(JobExecutionContext.IS_CUSTOM_DB_URL));
			String tableName = retriveSourceTableName(context, jobRunner);
			LOGGER.info(
					"Last exported time is not available. Determining the LB value from : {}",
					tableName);
			calendarLB = retriveLbFromSrcTablePartition(context, jobRunner,
					tableName);
		} catch (JobExecutionException e) {
			LOGGER.error(e.getMessage());
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
		} catch (Exception e2) {
			LOGGER.error(e2.getMessage());
		} finally {
			if (jobRunner != null) {
				jobRunner.closeConnection();
			}
		}
		return calendarLB;
	}

	private String retriveSourceTableName(WorkFlowContext context,
			schedulerJobRunner jobRunner) throws JobExecutionException {
		String tableName = (String) context
				.getProperty(JobExecutionContext.SOURCE_TABLE_NAME);
		if (!doesTableExists(jobRunner, tableName)) {
			tableName = retriveTableNameFromJobProp(context);
		}
		return tableName;
	}

	private boolean doesTableExists(schedulerJobRunner jobRunner,
			String tableName) throws JobExecutionException {
		boolean tableExists = false;
		String query = QueryConstants.SHOW_TABLE_QUERY
				.replace(QueryConstants.TABLE_NAME, tableName.toUpperCase());
		LOGGER.debug("Query : {}", query);
		ResultSet rs = null;
		try {
			rs = (ResultSet) jobRunner.runQuery(query, null, null);
			LOGGER.debug("Result set" + rs.getFetchSize());
			tableExists = rs.next();
		} catch (SQLException e) {
			throw new JobExecutionException(e.getMessage(), e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				LOGGER.warn("Exception while closing the connection {}",
						e.getMessage());
			}
		}
		LOGGER.debug("does table : {}, exists : {}", tableName, tableExists);
		return tableExists;
	}

	private String retriveTableNameFromJobProp(WorkFlowContext context) {
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		JobDictionary jobDictionary = jobDictionaryQuery.retrieve(
				(String) context.getProperty(JobExecutionContext.SOURCEJOBID));
		JobPropertyQuery jobPropertyQuery = new JobPropertyQuery();
		JobProperty jobProperty = jobPropertyQuery
				.retrieve(jobDictionary.getId(), JobExecutionContext.TARGET);
		return jobProperty.getParamvalue();
	}

	private Calendar retriveLbFromSrcTablePartition(WorkFlowContext context,
			schedulerJobRunner jobRunner, String tableName)
			throws JobExecutionException {
		Calendar calendarLB = null;
		Long dateLB = getMinPartitionFromSrcTable(context, jobRunner,
				tableName);
		if (dateLB != null) {
			calendarLB = new GregorianCalendar();
			String exportInterval = String
					.valueOf(context.getProperty(JobExecutionContext.INTERVAL));
			dateLB = DateFunctionTransformation.getInstance().getTrunc(dateLB,
					exportInterval,
					(String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY),
					TimeZoneUtil.getZoneId(context, null));
			calendarLB.setTimeInMillis(dateLB);
		}
		return calendarLB;
	}

	private Long getMinPartitionFromSrcTable(WorkFlowContext context,
			schedulerJobRunner jobRunner, String tableName)
			throws JobExecutionException {
		Long dateLB = null;
		ResultSet rs = null;
		try {
			String hivePartitionColumn = (String) context
					.getProperty(JobExecutionContext.HIVE_PARTITION_COLUMN);
			String query = new StringBuilder("select min(")
					.append(hivePartitionColumn).append(") from ")
					.append(tableName).toString();
			LOGGER.debug("Query : {}", query);
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, query);
			rs = (ResultSet) jobRunner.runQuery(query, null, context);
			while (rs.next()) {
				if (rs.getLong(1) != 0) {
					dateLB = rs.getLong(1);
				}
				LOGGER.debug("dateLBString: {}", dateLB);
			}
		} catch (SQLException e) {
			LOGGER.error("SQL Exception : {}", e.getMessage());
			throw new JobExecutionException(e.getMessage(), e);
		} finally {
			if (rs != null) {
				ConnectionManager.closeResultSet(rs);
			}
		}
		return dateLB;
	}

	private Calendar calculateLbFromMaxValue(WorkFlowContext context) {
		String exportInterval = String
				.valueOf(context.getProperty(JobExecutionContext.INTERVAL));
		Timestamp maxValue = (Timestamp) context
				.getProperty(JobExecutionContext.MAXVALUE);
		Long tempLB = DateFunctionTransformation.getInstance().getTrunc(
				maxValue.getTime(), exportInterval,
				(String) context
						.getProperty(JobExecutionContext.WEEK_START_DAY),
				TimeZoneUtil.getZoneId(context, null));
		return DateFunctionTransformation.getInstance().getNextBound(tempLB,
				exportInterval);
	}

	private Long populateContextWithLB(WorkFlowContext context,
			Calendar calendarLB) {
		Long finalLb = null;
		if (calendarLB != null) {
			finalLb = calendarLB.getTimeInMillis();
		}
		LOGGER.debug("finalLb: {}", finalLb);
		context.setProperty(JobExecutionContext.LB, finalLb);
		context.setProperty(JobExecutionContext.LB_INIT, finalLb);
		return finalLb;
	}

	private Calendar calculateUB(WorkFlowContext context) {
		LOGGER.debug("Calculate UB");
		Calendar calendarUB = calculateUbFromSourceJobs(context);
		if (calendarUB != null) {
			String sourceJobType = (String) context
					.getProperty(JobExecutionContext.SOURCEJOBTYPE);
			LOGGER.debug("sourceJobType value : {}", sourceJobType);
			if (JobTypeDictionary.USAGE_JOB_TYPE
					.equalsIgnoreCase(sourceJobType)) {
				String delayTime = String.valueOf(
						context.getProperty(JobExecutionContext.DELAYTIME));
				calendarUB.add(Calendar.MINUTE, -Integer.parseInt(delayTime));
			}
			calendarUB = DateFunctionTransformation.getInstance()
					.getNextBound(calendarUB.getTimeInMillis(), String.valueOf(
							context.getProperty(JobExecutionContext.INTERVAL)));
		}
		LOGGER.debug("UpperBound value is : {}", calendarUB);
		return calendarUB;
	}

	@SuppressWarnings("unchecked")
	private Calendar calculateUbFromSourceJobs(WorkFlowContext context) {
		Calendar calendarUB = new GregorianCalendar();
		List<Boundary> srcBoundaryList = (List<Boundary>) context
				.getProperty(JobExecutionContext.SOURCE_BOUND_LIST);
		for (Boundary srcBoundary : srcBoundaryList) {
			Timestamp maxValue = srcBoundary.getMaxValue();
			if (maxValue != null
					&& maxValue.getTime() <= calendarUB.getTimeInMillis()) {
				calendarUB.setTime(maxValue);
				LOGGER.debug("calendarUB : {}", calendarUB);
			} else {
				calendarUB = null;
				break;
			}
		}
		return calendarUB;
	}

	private void populateContextWithUB(WorkFlowContext context,
			Calendar calendarUB) {
		Long finalUb = null;
		if (calendarUB != null) {
			finalUb = calendarUB.getTimeInMillis();
		}
		context.setProperty(JobExecutionContext.UB, finalUb);
	}

	private Calendar populateContextWithNB(WorkFlowContext context,
			Long finalLb) {
		Calendar calendarNB = null;
		if (finalLb != null) {
			calendarNB = DateFunctionTransformation.getInstance()
					.getNextBound(finalLb, String.valueOf(
							context.getProperty(JobExecutionContext.INTERVAL)));
			context.setProperty(JobExecutionContext.NEXT_LB,
					calendarNB.getTimeInMillis());
		}
		return calendarNB;
	}

	private void populateContextWithReturnVal(WorkFlowContext context,
			Calendar calendarNB) throws WorkFlowExecutionException {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		context.setProperty(JobExecutionContext.RETURN, 0);
		LBUBUtil.lbubChecks(context, null);
		if (calendarNB != null && calendarNB.getTimeInMillis() > dateTransformer
				.getSystemTime()) {
			LOGGER.info(NO_COMPLETE_DATA);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					NO_COMPLETE_DATA);
			context.setProperty(JobExecutionContext.RETURN, 1);
		}
	}
}

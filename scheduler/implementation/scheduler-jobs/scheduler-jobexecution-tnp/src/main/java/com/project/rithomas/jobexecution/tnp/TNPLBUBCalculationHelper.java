
package com.project.rithomas.jobexecution.tnp;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

public class TNPLBUBCalculationHelper {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(TNPLBUBCalculationHelper.class);

	private static final String NO_COMPLETE_DATA = "No Complete Data to calculate KPI/Threshold/Profile further";

	private static final String EITHER_OF_BOUNDARY_VALUE_NULL = "Either of the boundary value is null.Hence not calculating KPI/Threshold/Profile further";

	static void calculateLB(WorkFlowContext context,
			List<Boundary> srcBoundaryList, String jobType,
			Calendar calLBValue) {
		if (!(JobTypeDictionary.TNP_IMPORT_JOB_TYPE.equals(jobType))) {
			List<Long> minSrcDateList = new ArrayList<Long>();
			String partitionColumn = "dt";
			LOGGER.debug("Source boundary list: {}", srcBoundaryList);
			if (srcBoundaryList != null && !srcBoundaryList.isEmpty()) {
				partitionColumn = (String) context
						.getProperty(srcBoundaryList.get(0).getJobId() + "_"
								+ JobExecutionContext.HIVE_PARTITION_COLUMN);
			}
			LOGGER.debug("Partition column: {}", partitionColumn);
			if (partitionColumn != null && !partitionColumn.isEmpty()) {
				partitionNotEmptySteps(context, partitionColumn,
						minSrcDateList);
			}
			if (minSrcDateList != null && !minSrcDateList.isEmpty()) {
				Collections.sort(minSrcDateList);
				calLBValue.setTimeInMillis(
						minSrcDateList.get(minSrcDateList.size() - 1));
			}
		}
	}

	private static void partitionNotEmptySteps(WorkFlowContext context,
			String partitionColumn, List<Long> minSrcDateList) {
		String sourceTableName = (String) context
				.getProperty(JobExecutionContext.SOURCE_TABLE_NAME);
		LOGGER.debug("Source table name: {}", sourceTableName);
		List<String> srcTableNameList = null;
		if (StringUtils.isNotEmpty(sourceTableName)) {
			srcTableNameList = getSourceTableNames(sourceTableName);
		}
		if (srcTableNameList != null && !srcTableNameList.isEmpty()) {
			executeQueryForAllSourceTable(partitionColumn, minSrcDateList,
					srcTableNameList);
		}
	}

	private static List<String> getSourceTableNames(String sourceTableName) {
		List<String> srcTableNameList;
		if (sourceTableName.contains(",")) {
			String[] srcTabNameVals = sourceTableName.split(",");
			srcTableNameList = Arrays.asList(srcTabNameVals);
		} else {
			srcTableNameList = new ArrayList<String>();
			srcTableNameList.add(sourceTableName);
		}
		return srcTableNameList;
	}

	private static void executeQueryForAllSourceTable(String partitionColumn,
			List<Long> minSrcDateList, List<String> srcTableNameList) {
		ResultSet resultSet = null;
		try (Connection connection = ConnectionManager
				.getConnection(schedulerConstants.HIVE_DATABASE);
				Statement statement = connection.createStatement();) {
			for (String srcTableNameVal : srcTableNameList) {
				List<String> minSrcDateValList = new ArrayList<String>();
				resultSet = statement.executeQuery("select min("
						+ partitionColumn + ") from " + srcTableNameVal);
				while (resultSet.next()) {
					minSrcDateValList.add(resultSet.getString(1));
				}
				resultSet.close();
				if (minSrcDateValList != null && !minSrcDateValList.isEmpty()
						&& minSrcDateValList.size() == 1) {
					minSrcDateList
							.add(Long.parseLong(minSrcDateValList.get(0)));
				}
			}
		} catch (SQLException e) {
			LOGGER.error(
					"Exception occurred while executing query for getting minimum partition column value for source table,  Exception detail: {}",
					new Object[] { e });
		} catch (Exception e) {
			LOGGER.error(
					" Exception occurred while executing query for getting minimum partition column value for source table {}",
					e.getMessage());
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					resultSet = null;
					LOGGER.warn("Error while closing resultset {}", e);
				} finally {
					resultSet = null;
				}
			}
		}
	}

	static Long calculateFinalUBValue(Calendar calendarUB, String sourcePlevel,
			WorkFlowContext context) {
		Long finalUb;
		Calendar calUB;
		String sourceJobType = (String) context
				.getProperty(JobExecutionContext.SOURCEJOBTYPE);
		LOGGER.debug("sourceJobType value : {}", sourceJobType);
		if (JobTypeDictionary.USAGE_JOB_TYPE.equals(sourceJobType)) {
			finalUb = DateFunctionTransformation.getInstance()
					.getTrunc(calendarUB.getTimeInMillis(), sourcePlevel);
		} else {
			calUB = DateFunctionTransformation.getInstance().getNextBound(
					DateFunctionTransformation.getInstance().getTrunc(
							calendarUB.getTimeInMillis(), sourcePlevel,
							(String) context.getProperty(
									JobExecutionContext.WEEK_START_DAY)),
					sourcePlevel);
			finalUb = DateFunctionTransformation.getInstance().getTrunc(
					calUB.getTimeInMillis(), sourcePlevel, (String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY));
		}
		return finalUb;
	}

	static boolean setUBValue(Calendar calendarUB, boolean firstTime,
			Boundary srcBoundary) {
		boolean isFirstTime = true;
		if (firstTime) {
			calendarUB.setTime(srcBoundary.getMaxValue());
			isFirstTime = false;
		} else {
			if (srcBoundary.getMaxValue().getTime() <= calendarUB
					.getTimeInMillis()) {
				calendarUB.setTime(srcBoundary.getMaxValue());
			}
		}
		LOGGER.debug("UB value is: {}", calendarUB.getTime());
		return isFirstTime;
	}

	static int finalLBUBRegionCheck(WorkFlowContext context,
			int rgnCntToAggregate, String regionId, Long finalLb, Long finalUb,
			Long nextLb) {
		int regionCount = rgnCntToAggregate;
		if (finalUb == null || finalLb == null) {
			LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL + " for region: {}",
					regionId);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					EITHER_OF_BOUNDARY_VALUE_NULL + " for region " + regionId);
			return -1;
		} else if (finalLb.equals(finalUb)) {
			LOGGER.info(NO_COMPLETE_DATA + " for region: {}", regionId);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					NO_COMPLETE_DATA + " for region " + regionId);
		} else if (nextLb > finalUb) {
			LOGGER.info(NO_COMPLETE_DATA + " for region: {}", regionId);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					NO_COMPLETE_DATA + " for region: " + regionId);
		} else {
			// if data is there to aggregate
			regionCount++;
		}
		return regionCount;
	}

	static void finalLBUBCheck(WorkFlowContext context) {
		Long finalLb = (Long) context.getProperty(JobExecutionContext.LB);
		Long finalUb = (Long) context.getProperty(JobExecutionContext.UB);
		Long nextLb = (Long) context.getProperty(JobExecutionContext.NEXT_LB);
		if (finalUb == null || finalLb == null) {
			LOGGER.info(EITHER_OF_BOUNDARY_VALUE_NULL);
			context.setProperty(JobExecutionContext.RETURN, 1);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					EITHER_OF_BOUNDARY_VALUE_NULL);
		} else if (finalLb.equals(finalUb)) {
			LOGGER.info("No Data to Calculate KPI");
			context.setProperty(JobExecutionContext.RETURN, 1);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					"No Data to Calculate KPI");
		} else if (nextLb > finalUb) {
			LOGGER.info(NO_COMPLETE_DATA);
			context.setProperty(JobExecutionContext.RETURN, 1);
			context.setProperty(JobExecutionContext.DESCRIPTION,
					NO_COMPLETE_DATA);
		}
	}

	static Calendar getCalendar(WorkFlowContext context) {
		Calendar calendar = new GregorianCalendar();
		Date date = new Date();
		long dateInUnixFormat = date.getTime();
		calendar.setTimeInMillis(dateInUnixFormat);
		int retDays = (Integer) context
				.getProperty(JobExecutionContext.AGG_RETENTIONDAYS);
		calendar.add(Calendar.DAY_OF_MONTH, -retDays);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return calendar;
	}

	static String getDateFromMilliSeconds(Long milliseconds) {
		if (milliseconds != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(milliseconds);
			return DateFunctionTransformation.getInstance()
					.getFormattedDate(calendar.getTime());
		} else {
			return null;
		}
	}
}

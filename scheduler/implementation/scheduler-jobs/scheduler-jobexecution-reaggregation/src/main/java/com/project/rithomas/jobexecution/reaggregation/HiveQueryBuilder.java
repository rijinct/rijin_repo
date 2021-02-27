
package com.project.rithomas.jobexecution.reaggregation;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.rithomas.jobexecution.aggregation.AggregationUtil;
import com.project.rithomas.jobexecution.common.util.AddDBConstants;
import com.project.rithomas.jobexecution.common.util.CommonAggregationUtil;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.JobExecutionUtil;
import com.project.rithomas.jobexecution.common.util.MultipleQueryGeneratorBasedOnSubPartition;
import com.project.rithomas.jobexecution.common.util.RetrieveDimensionValues;
import com.project.rithomas.jobexecution.common.util.schedulerJobRunner;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class HiveQueryBuilder {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveQueryBuilder.class);

	ReaggregationQueryBuilder queryBuilder = new ReaggregationQueryBuilder();

	AggregationUtil aggregationUtil = new AggregationUtil();

	private WorkFlowContext context;

	private schedulerJobRunner jobRunner;

	public HiveQueryBuilder(WorkFlowContext context,
			schedulerJobRunner jobRunner) {
		this.context = context;
		this.jobRunner = jobRunner;
	}

	public String constructQuery(String reportTime,
			boolean updateQsUpperBoundary, List<String> sourceJobs)
			throws Exception {
		Long nextLb = (Long) context.getProperty(JobExecutionContext.REAGG_LB);
		Long lowerBoundInMillis = (Long) context
				.getProperty(JobExecutionContext.REAGG_LB);
		String regionId = (String) context
				.getProperty(JobExecutionContext.REAGG_TZ_RGN);
		String dataAvailable = RetrieveDimensionValues
				.getAvailableDataCheck(context);
		String dataAvailable2g = RetrieveDimensionValues
				.getAvailableType2G(context);
		String dataAvailable4g = RetrieveDimensionValues
				.getAvailableType4G(context);
		RetrieveDimensionValues.checkSource(context);
		String targetTable = (String) context
				.getProperty(JobExecutionContext.TARGET);
		String currentTimeHint = targetTable + System.currentTimeMillis();
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String sqlToExecute = aggregationUtil.isKubernetesEnvironment()
				? aggregationUtil.replaceKPIFormula(context)
				: CommonAggregationUtil.replaceKPIFormula(jobName, sql,
						context);
		sqlToExecute = RetrieveDimensionValues
				.replaceDynamicValuesInJobSQL(context, sqlToExecute, jobRunner);
		Long maxImsiApnExport = (Long) context
				.getProperty(JobExecutionContext.MAX_IMSI_APN_EXPORT);
		if (updateQsUpperBoundary) {
			context.setProperty(JobExecutionContext.UB_QS_JOB, nextLb);
			context
			.setProperty(JobExecutionContext.REAGG_QS_UB_STATUS,false);
		}
		Long newUb = 0L;
		if (sqlToExecute.contains(JobExecutionContext.PERF_MGW)) {
			newUb = nextLb + 120000;
		}
		ReaggregationSourceConfiguration configuration = new ReaggregationSourceConfiguration();
		configuration.setContext(context).setLowerBound(lowerBoundInMillis)
				.setNextLb(nextLb).setSourceJobs(sourceJobs);
		LOGGER.debug(
				"Updating the query with the ReaggregationSourceConfiguration {}",
				configuration);
		ReaggregationQueryBuilder queryBuilder = new ReaggregationQueryBuilder();
		queryBuilder.setConfiguration(configuration);
		sqlToExecute = queryBuilder.setSql(sqlToExecute).update();
		LOGGER.debug("sql after updating the archiving placeholders {}",
				sqlToExecute);
		sqlToExecute = sqlToExecute
				.replace(JobExecutionContext.LOWER_BOUND,
						lowerBoundInMillis.toString())
				.replace(JobExecutionContext.UPPER_BOUND, nextLb.toString())
				.replace(JobExecutionContext.LOWERBOUND_DATE, reportTime)
				.replace(JobExecutionContext.AVAILABLE_DATA_CHECK,
						dataAvailable)
				.replace(JobExecutionContext.AVAILABLE_2G_3G, dataAvailable2g)
				.replace(JobExecutionContext.AVAILABLE_4G, dataAvailable4g)
				.replace(JobExecutionContext.MAX_NO_OF_IMSI_EXPORT,
						maxImsiApnExport.toString())
				.replace(JobExecutionContext.NEXT_UPPERBOUND, newUb.toString())
				.replace(JobExecutionContext.TIMESTAMP_HINT, currentTimeHint)
				.replace(JobExecutionContext.CUTOFF_VOLUME_2G,
						(String) context.getProperty(
								JobExecutionContext.CUTOFF_VOLUME_2G))
				.replace(JobExecutionContext.CUTOFF_VOLUME_3G,
						(String) context.getProperty(
								JobExecutionContext.CUTOFF_VOLUME_3G))
				.replace(JobExecutionContext.CUTOFF_VOLUME_4G,
						(String) context.getProperty(
								JobExecutionContext.CUTOFF_VOLUME_4G))
				.replace(JobExecutionContext.TYPE_AVAILABLE_HO,
						RetrieveDimensionValues.getTypeAvailableHO(context))
				.replace(JobExecutionContext.TYPE_AVAILABLE_SGSN,
						RetrieveDimensionValues.getTypeAvailableSGSN(context))
				.replace(JobExecutionContext.LONG_CALLS_THRESHOLD_VALUE,
						getPropertyFromContext(
								JobExecutionContext.LONG_CALLS_THRESHOLD_VALUE))
				.replace(JobExecutionContext.AVERAGE_MOS_THRESHOLD,
						getPropertyFromContext(
								JobExecutionContext.AVERAGE_MOS_THRESHOLD));
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			sqlToExecute = updateSqlForTimeZone(regionId, currentTimeHint,
					sqlToExecute);
		} else {
			sqlToExecute = updateSqlForNonTimeZone(currentTimeHint,
					sqlToExecute);
		}
		executeQueryHints(sqlToExecute);
		LOGGER.debug("SQL to execute: {}", sqlToExecute);
		return MultipleQueryGeneratorBasedOnSubPartition.generateQuery(
				sqlToExecute, context, jobRunner, regionId, lowerBoundInMillis,
				nextLb);
	}

	private void executeQueryHints(String sqlToExecute)
			throws WorkFlowExecutionException, SQLException {
		List<String> hiveSettings = aggregationUtil
				.setQueryHintForParallelInsert(sqlToExecute);
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String executionEngine = JobExecutionUtil.getExecutionEngine(context);
		hiveSettings.addAll(HiveConfigurationProvider.getInstance()
				.getQueryHints(jobId, executionEngine));
		JobExecutionUtil.runQueryHints(jobRunner, hiveSettings);
	}

	private String updateSqlForNonTimeZone(String currentTimeHint,
			String sqlToExecute) {
		sqlToExecute = sqlToExecute.replace(JobExecutionContext.TIMEZONE_CHECK,
				"");
		context.setProperty(JobExecutionContext.TIMESTAMP_HINT,
				currentTimeHint);
		sqlToExecute = sqlToExecute.replace(
				JobExecutionContext.TIMEZONE_PARTITION,
				" , " + AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='"
						+ JobExecutionContext.DEFAULT_TIMEZONE + "' ");
		return sqlToExecute;
	}

	private String updateSqlForTimeZone(String regionId, String currentTimeHint,
			String sqlToExecute) throws WorkFlowExecutionException {
		String timezoneCheck = " and "
				+ AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='" + regionId
				+ "' ";
		String timezonePartitionColumn = regionId;
		if (TimeZoneUtil.isTimeZoneAgnostic(context)) {
			timezoneCheck = "";
			timezonePartitionColumn = JobExecutionContext.DEFAULT_TIMEZONE;
		}
		sqlToExecute = updateQueryWithTimezonePartition(currentTimeHint,
				sqlToExecute, timezoneCheck, timezonePartitionColumn);
		return sqlToExecute;
	}

	private String updateQueryWithTimezonePartition(String currentTimeHint,
			String finalSqlToExecute, String timezoneCheck,
			String timezonePartitionColumn) {
		finalSqlToExecute = finalSqlToExecute
				.replace(JobExecutionContext.TIMEZONE_CHECK, timezoneCheck);
		finalSqlToExecute = finalSqlToExecute.replace(
				JobExecutionContext.TIMEZONE_PARTITION,
				" , " + AddDBConstants.TIMEZONE_PARTITION_COLUMN + "='"
						+ timezonePartitionColumn + "' ");
		context.setProperty(
				timezonePartitionColumn + JobExecutionContext.TIMESTAMP_HINT,
				currentTimeHint);
		ThreadContext.put(JobExecutionContext.TZ_REGION_VAL,
				timezonePartitionColumn);
		return finalSqlToExecute;
	}

	private String getPropertyFromContext(String property) {
		if (StringUtils.isNotEmpty((String) context.getProperty(property))) {
			return (String) context.getProperty(property);
		} else {
			return StringUtils.EMPTY;
		}
	}
}

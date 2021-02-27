
package com.project.rithomas.jobexecution.reaggregation;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;

public class ReaggregationQueryBuilder {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggregationQueryBuilder.class);

	private String sql;

	private ReaggregationSourceConfiguration configuration;

	public void setConfiguration(
			ReaggregationSourceConfiguration configuration) {
		this.configuration = configuration;
	}

	public ReaggregationSourceConfiguration getConfiguration() {
		return configuration;
	}

	public ReaggregationQueryBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public String update() throws JobExecutionException {
		for (String sourceJob : configuration.getSourceJobs()) {
			configuration.process(sourceJob);
			String sourceTable = configuration.getSourceTable();
			LOGGER.debug("Updated query for the sourceTable {}", sourceTable);
			String[] toReplace = new String[] {
			String[] replacement = new String[] {
					configuration.getReportTimeWhereClause(),
					configuration.getTargetTable(),
					configuration.getSourceLowerBound(),
					configuration.getSourceUpperBound() };
			LOGGER.debug("The query will be updated with {}", configuration);
			sql = StringUtils.replaceEach(sql, toReplace, replacement);
		}
		LOGGER.debug(
				"finalSqlToExecute after replacing archiving placeholders : {}",
				sql);
		return sql;
	}
}

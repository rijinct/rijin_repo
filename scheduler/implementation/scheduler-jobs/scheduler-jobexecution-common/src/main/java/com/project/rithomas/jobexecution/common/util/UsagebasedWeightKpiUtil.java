
package com.project.rithomas.jobexecution.common.util;

import java.util.List;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class UsagebasedWeightKpiUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UsagebasedWeightKpiUtil.class);

	@SuppressWarnings("unchecked")
	public static String replaceKpiPlaceholders(String sql,
			WorkFlowContext context, List<String> usageBasedKpiPlaceHolders)
			throws JobExecutionException {
		List<Object[]> resultSet = null;
		if (!usageBasedKpiPlaceHolders.isEmpty()) {
			String usageBasedQuery = QueryConstants.US_BASE_QUERY.replace(
					JobExecutionContext.INTERVAL_PLACEHOLDER,
					(String) context.getProperty(JobExecutionContext.ALEVEL));
			LOGGER.debug("us_based_query: {}", usageBasedQuery);
			QueryExecutor executor = new QueryExecutor();
			resultSet = executor
					.executeMetadatasqlQueryMultiple(usageBasedQuery, context);
			if (resultSet != null && !resultSet.isEmpty()) {
				for (Object[] resultVal : resultSet) {
					String placeHolderName = JobExecutionContext.US_BASE_WEIGHT_PREFIX
						String avgKPIVal = (resultVal[2] == null
								|| resultVal[2].toString().isEmpty()) ? "null"
										: resultVal[2].toString();
						LOGGER.debug(
								"placeholder_name : {}, Kpi_definition : {}, avg_placeholder_name : {}, avg_value : {}",
								placeHolderName, resultVal[1].toString(),
								averagePlaceHolderName, avgKPIVal);
						sql = sql
								.replaceAll(placeHolderName,
										resultVal[1].toString())
								.replaceAll(averagePlaceHolderName, avgKPIVal);
					}
				}
			}
		}
		return sql;
	}
}

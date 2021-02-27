
package com.project.rithomas.jobexecution.common.util;

import java.util.Calendar;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class DayLightSavingUtil {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(DayLightSavingUtil.class);

	public int getCntIfUBInTimeZoneDST(Calendar ub, String region,
			WorkFlowContext context) throws JobExecutionException {
		int ubDSTCnt = 0;
		String upperBoundDate = DateFunctionTransformation.getInstance()
				.getFormattedDate(ub.getTime());
		Object[] resultSet = null;
		String sql = "select count(*) from saidata.es_rgn_tz_info_1 where ((date_trunc('day',start_time)= date_trunc('day', timestamp '"
				+ upperBoundDate
				+ "')) OR (date_trunc('day',end_time)= date_trunc('day', timestamp '"
				+ upperBoundDate
				+ "'))) and date_trunc('hour',start_time) <= timestamp '"
				+ upperBoundDate + "' and end_time >= timestamp '" + upperBoundDate + "'";
		if (region != null && !region.isEmpty()) {
			sql = sql + " and tmz_region='" + region + "'";
		}
		LOGGER.debug("query to get ub dst count: {}", sql);
		QueryExecutor queryExecutor = new QueryExecutor();
		resultSet = queryExecutor.executeMetadatasqlQuery(sql, context);
		if (resultSet != null) {
			ubDSTCnt = Integer.parseInt(resultSet[0].toString());
		}
		LOGGER.debug("ubDSTCnt: {}", ubDSTCnt);
		return ubDSTCnt;
	}

	public int getCntIfLBInTimeZoneDST(Calendar lb, String region,
			WorkFlowContext context) throws JobExecutionException {
		int lbDSTCnt = 0;
		String lowerBoundDate = DateFunctionTransformation.getInstance()
				.getFormattedDate(lb.getTime());
		Object[] resultSet = null;
		String sql = "select count(*) from saidata.es_rgn_tz_info_1 where  date_trunc('hour',start_time) = timestamp '"
				+ lowerBoundDate + "'";
		if (region != null && !region.isEmpty()) {
			sql = sql + " and tmz_region='" + region + "'";
		}
		LOGGER.debug("query to get lb dst count: {}", sql);
		QueryExecutor queryExecutor = new QueryExecutor();
		resultSet = queryExecutor.executeMetadatasqlQuery(sql, context);
		if (resultSet != null) {
			lbDSTCnt = Integer.parseInt(resultSet[0].toString());
		}
		LOGGER.debug("lbDSTCnt: {}", lbDSTCnt);
		return lbDSTCnt;
	}
}

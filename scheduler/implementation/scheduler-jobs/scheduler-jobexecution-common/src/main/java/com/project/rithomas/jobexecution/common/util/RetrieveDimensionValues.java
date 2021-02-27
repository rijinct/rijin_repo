
package com.project.rithomas.jobexecution.common.util;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class RetrieveDimensionValues {

	public final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(RetrieveDimensionValues.class);

	@SuppressWarnings("unchecked")
	public static String getAvailableDataCheck(WorkFlowContext context)
			throws JobExecutionException {
		int count = 0;
		String dataAvilable = "0";
		List<Object[]> resultSet = null;
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		if (sql.contains(JobExecutionContext.AVAILABLE_DATA_CHECK)) {
			String availableDataCheckSql = QueryConstants.AVAILABLE_DATA_SGSN_QUERY;
			QueryExecutor queryExecutor = new QueryExecutor();
			resultSet = queryExecutor.executeMetadatasqlQueryMultiple(
					availableDataCheckSql, context);
			if (resultSet != null && !resultSet.isEmpty()) {
				resultSet.size();
				for (Object[] itObj : resultSet) {
					count++;
				}
			}
			if (count >= 1) {
				dataAvilable = "1";
			}
		}
		return dataAvilable;
	}

	@SuppressWarnings("unchecked")
	public static void checkSource(WorkFlowContext context)
			throws JobExecutionException {
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String sqlToExec;
		List<Object> resultSet = null;
		int allCount = 0;
		int hoCount = 0;
		int lteCount = 0;
		int count2G3G = 0;
		String sourceAll = "0";
		String sourceHO = "0";
		String sourceLTE = "0";
		String source2G3G = "0";
		String availableDataCheckSql = QueryConstants.CHECK_SOURCE_QUERY;
		if (jobName.contains("DATA_CP_SEGG") || jobName.contains("SGSN_SQM")
				|| jobName.contains("LTE_SQM")) {
			QueryExecutor queryExecutor = new QueryExecutor();
			resultSet = queryExecutor.executeMetadatasqlQueryMultiple(
					availableDataCheckSql, context);
			if (resultSet != null && !resultSet.isEmpty()) {
				for (Object rs : resultSet) {
					if (JobExecutionContext.SOURCE_CHECK_ALL
							.contains(rs.toString())) {
						allCount++;
					} else if (JobExecutionContext.SOURCE_CHECK_LTE
							.contains(rs.toString())) {
						lteCount++;
					} else if (JobExecutionContext.SOURCE_CHECK_2G_3G
							.contains(rs.toString())) {
						count2G3G++;
					}
					if (JobExecutionContext.SOURCE_CHECK_HO
							.contains(rs.toString())) {
						hoCount++;
					}
				}
			}
			if (allCount >= 1) {
				sourceAll = "1";
			}
			if (hoCount >= 1) {
				sourceHO = "1";
			}
			if (lteCount >= 1) {
				sourceLTE = "1";
			}
			if (count2G3G >= 1) {
				source2G3G = "1";
			}
			sqlToExec = sql
					.replace(JobExecutionContext.SOURCE_CHECK_ALL_RES,
							sourceAll)
					.replace(JobExecutionContext.SOURCE_CHECK_HO_RES, sourceHO)
					.replace(JobExecutionContext.SOURCE_CHECK_LTE_RES,
							sourceLTE)
					.replace(JobExecutionContext.SOURCE_CHECK_2G_3G_GN_RES,
							source2G3G);
			context.setProperty(JobExecutionContext.SQL, sqlToExec);
		}
	}

	@SuppressWarnings("unchecked")
	public static String getAvailableType2G(WorkFlowContext context)
			throws JobExecutionException {
		String availableData2G = "Yes";
		String sqlToExec;
		String availableData2gSql = null;
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		String jobid = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		Object[] resultSet = null;
		// Retrieve the home country code and home imsi code
		if (sql.contains(JobExecutionContext.AVAILABLE_2G_3G)) {
			if (jobid.contains("HTTP") || jobid.contains("QOE")
					|| jobid.contains("DATA_UP")) {
				availableData2gSql = QueryConstants.AVAILABLE_DATA_HTTP_QUERY;
			} else if (jobid.contains("GN")) {
				availableData2gSql = QueryConstants.AVAILABLE_DATA_GN_QUERY;
			}
		}
		if (availableData2gSql != null) {
			QueryExecutor queryExecutor = new QueryExecutor();
			if (jobid.contains("HTTP") || jobid.contains("QOE")
					|| jobid.contains("DATA_UP")) {
				resultSet = queryExecutor
						.executeMetadatasqlQuery(availableData2gSql, context);
				if (resultSet != null) {
					availableData2G = resultSet[0].toString();
				}
			} else if (jobid.contains("GN")) {
				List<Object> resultSetList = queryExecutor
						.executeMetadatasqlQueryMultiple(availableData2gSql,
								context);
				if (resultSetList != null && !resultSetList.isEmpty()) {
					availableData2G = resultSetList.iterator().next()
							.toString();
				}
				sqlToExec = sql.replace(JobExecutionContext.AVAILABLE_2G_3G,
						availableData2G);
				context.setProperty(JobExecutionContext.SQL, sqlToExec);
			}
		}
		return availableData2G;
	}

	public static String getAvailableType4G(WorkFlowContext context)
			throws JobExecutionException {
		String availableData2G = "Yes";
		String availableData2gSql = null;
		Object[] resultSet = null;
		String sql = (String) context.getProperty(JobExecutionContext.SQL);
		String jobid = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		// Retrieve the home country code and home imsi code
		if (sql.contains(JobExecutionContext.AVAILABLE_4G)) {
			if (jobid.contains("HTTP") || jobid.contains("QOE")
					|| jobid.contains("DATA_UP")) {
				availableData2gSql = QueryConstants.AVAILABLE_DATA_HTTP_4G_QUERY;
			} else if (jobid.contains("GN")) {
				availableData2gSql = QueryConstants.AVAILABLE_DATA_GN_4G_QUERY;
			}
		}
		if (availableData2gSql != null) {
			QueryExecutor queryExecutor = new QueryExecutor();
			resultSet = queryExecutor
					.executeMetadatasqlQuery(availableData2gSql, context);
			if (resultSet != null) {
				availableData2G = resultSet[0].toString();
			}
		}
		if (sql.contains(JobExecutionContext.AVAILABLE_4G)
				&& (availableData2G.isEmpty() || availableData2G == null)) {
			throw new JobExecutionException(
					"4G availability is not configured");
		} else {
			return availableData2G;
		}
	}

	public static String getCommaSeparatedStringValues(String sql,
			WorkFlowContext context) throws JobExecutionException {
		List<Object> resultSet = null;
		String ret = "";
		String value = null;
		QueryExecutor queryExecutor = new QueryExecutor();
		resultSet = queryExecutor.executeMetadatasqlQueryMultiple(sql, context);
		if (resultSet != null && !resultSet.isEmpty()) {
			for (Object result : resultSet) {
				value = result.toString();
				LOGGER.debug("value : {}", value);
				ret = ret.concat("'").concat(value).concat("'").concat(",");
				LOGGER.debug("ret : {}", ret);
			}
		}
		if (!ret.equals("")) {
			ret = ret.substring(0, ret.length() - 1);
		} else {
			ret = "''";
		}
		return ret;
	}

	public static String getTypeAvailableHO(WorkFlowContext context)
			throws JobExecutionException {
		String ret = "";
		String aggQuery = (String) context.getProperty(JobExecutionContext.SQL);
		String sql = QueryConstants.TYPE_AVAILABLE_HO_QUERY;
		if (aggQuery.contains(JobExecutionContext.TYPE_AVAILABLE_HO)) {
			LOGGER.debug("inside TYPE_AVAILABLE_HO");
			try {
				ret = getCommaSeparatedStringValues(sql, context);
			} catch (JobExecutionException e) {
				LOGGER.error(" JobExecutionException {}", e);
			}
		}
		if (aggQuery.contains(JobExecutionContext.TYPE_AVAILABLE_HO)
				&& (ret.isEmpty() || ret == null)) {
			throw new JobExecutionException(
					"type value is not available in sgsn_available_data table");
		} else {
			return ret;
		}
	}

	public static String getTypeAvailableSGSN(WorkFlowContext context)
			throws JobExecutionException {
		String ret = "";
		String sql = QueryConstants.TYPE_AVAILABLE_SGSN_QUERY;
		String aggQuery = (String) context.getProperty(JobExecutionContext.SQL);
		if (aggQuery.contains(JobExecutionContext.TYPE_AVAILABLE_SGSN)) {
			LOGGER.debug("inside TYPE_AVAILABLE_SGSN");
			try {
				ret = getCommaSeparatedStringValues(sql, context);
			} catch (JobExecutionException e) {
				LOGGER.error(" JobExecutionException {}", e);
			}
		}
		if (aggQuery.contains(JobExecutionContext.TYPE_AVAILABLE_SGSN)
				&& (ret.isEmpty() || ret == null)) {
			throw new JobExecutionException(
					"data is not available in sgsn_available_data table");
		} else {
			return ret;
		}
	}

	@SuppressWarnings("unchecked")
	public static String replaceDynamicValuesInJobSQL(WorkFlowContext context,
			String sql, schedulerJobRunner jobRunner) throws Exception {
		List<Object[]> resultSet = null;
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		if (JobTypeDictionary.PERF_REAGG_JOB_TYPE.equals(
				(String) context.getProperty(JobExecutionContext.JOBTYPE))) {
			jobId = (String) context
					.getProperty(JobExecutionContext.PERF_JOB_NAME);
		}
		String sqlDetails = QueryConstants.DYNAMIC_PARAM_QUERY
				.replace(JobExecutionContext.ENTITY_JOB_NAME, jobId);
		LOGGER.debug("Postgres query to fetch dynamic param details:{} ",
				sqlDetails);
		QueryExecutor executor = new QueryExecutor();
		resultSet = executor.executeMetadatasqlQueryMultiple(sqlDetails,
				context);
		if (resultSet != null && !resultSet.isEmpty()) {
			for (Object[] resultVal : resultSet) {
				if (sql.contains(resultVal[0].toString())) {
					sql = sql.replaceAll(resultVal[0].toString(),
							getDynamicValue(resultVal, context, jobRunner));
				}
			}
		}
		LOGGER.debug("Query after replacing the dynamic values:{} ", sql);
		return sql;
	}

	private static String getDynamicValue(Object[] resultVal,
			WorkFlowContext context, schedulerJobRunner jobRunner)
			throws Exception {
		String query = resultVal[1].toString();
		LOGGER.debug("Provided query for dynamic values:{} ", query);
		Object value = resultVal[2];
		if (query != null && !query.isEmpty()) {
			value = getDynamicValueFromDB(resultVal, context, query, value,
					jobRunner);
		}
		LOGGER.debug("Dynamic param: {} Value replaced: {}",
				resultVal[0].toString(), value.toString());
		return value.toString();
	}

	private static Object getDynamicValueFromDB(Object[] resultVal,
			WorkFlowContext context, String query, Object value,
			schedulerJobRunner jobRunner) throws Exception {
		String database = resultVal[4].toString();
		if (JobExecutionContext.HIVE_DB.equalsIgnoreCase(database)) {
			context.setProperty(JobExecutionContext.HIVE_SESSION_QUERY, query);
			value = getDynamicValueFromHive(resultVal, query, value, context,
					jobRunner);
		} else if (JobExecutionContext.POSTGRES_DB.equalsIgnoreCase(database)) {
			value = getDynamicValueFromPostgres(resultVal, context, query,
					value);
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private static Object getDynamicValueFromPostgres(Object[] resultVal,
			WorkFlowContext context, String query, Object value)
			throws JobExecutionException {
		List<Object> resultSet = null;
		StringBuilder dynamicVal = null;
		QueryExecutor executor = new QueryExecutor();
		resultSet = executor.executeMetadatasqlQueryMultiple(query, context);
		if (resultSet != null && !resultSet.isEmpty()) {
			dynamicVal = new StringBuilder();
			for (Object result : resultSet) {
				if (!dynamicVal.toString().isEmpty()) {
					dynamicVal = dynamicVal.append(",")
							.append(result.toString());
				} else {
					dynamicVal.append(result.toString());
				}
			}
			value = dynamicVal.toString();
		} else if (value.toString().isEmpty()) {
			throw new JobExecutionException(resultVal[0].toString()
							+ " dynamic dimension value is not available in POSTGRES DB");
		}
		return value;
	}

	private static Object getDynamicValueFromHive(Object[] resultVal,
			String query, Object value, WorkFlowContext context,
			schedulerJobRunner jobRunner) throws Exception {
		String outputDirName = ((String) context
				.getProperty(JobExecutionContext.JOB_NAME));
		String type = resultVal[3].toString();
		Object result = jobRunner.runQuery(query, outputDirName, context);
		if (result instanceof ResultSet) {
			ResultSet rs = (ResultSet) result;
		try {
			if (rs.next()) {
				if (JobExecutionContext.INTEGER.equalsIgnoreCase(type)) {
					value = rs.getInt(1);
					} else if (JobExecutionContext.STRING
							.equalsIgnoreCase(type)) {
					value = rs.getString(1);
				}
				} else if (value == null || value.toString().isEmpty()) {
					throw new JobExecutionException(resultVal[0].toString()
								+ " dynamic dimension value is not available in HIVE DB");
			}
		} finally {
			if (rs != null)
				rs.close();
			}
		} else {
			List<String> fileContent = (List<String>) result;
			if (!fileContent.isEmpty()) {
				if (JobExecutionContext.INTEGER.equalsIgnoreCase(type)) {
					value = Integer.parseInt(fileContent.get(0).split(",")[0]);
				} else if (JobExecutionContext.STRING.equalsIgnoreCase(type)) {
					value = fileContent.get(0).split(",")[0];
				}
			} else {
				throw new JobExecutionException(resultVal[0].toString()
						+ " dynamic dimension value is not available in HIVE DB");
			}
		}
		LOGGER.debug("dimension value to be replaced :{}", value);
		return value;
	}

	public static Map<String, String> getRuntimePropFromDB(
			WorkFlowContext context) throws JobExecutionException {
		Map<String, String> runtimeProp = new HashMap<String, String>();
		List<Object[]> resultSet = null;
		QueryExecutor executor = new QueryExecutor();
		resultSet = executor.executeMetadatasqlQueryMultiple(
				QueryConstants.RUNTIME_PROP_QUERY_ALL, context);
		if (CollectionUtils.isNotEmpty(resultSet)) {
			for (Object[] resultVal : resultSet) {
				runtimeProp.put(String.valueOf(resultVal[0]),
						String.valueOf(resultVal[1]));
			}
		}
		return runtimeProp;
	}
}


package com.project.rithomas.jobexecution.common.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Sets;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

public class MultipleQueryGeneratorBasedOnSubPartition {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MultipleQueryGeneratorBasedOnSubPartition.class);
	
	private static final String QUERY_SUB_PART_VALUES = "select distinct $PARTITION_COLUMNS:{part| json_extract_path_text(partition_values,'$part$') $part$ , }$ 1  "
			+ "from sub_partition_values where table_name in ($TABLE_NAMES:{tableName| '$tableName$',}$ 'CEM') and dt>='$LOWER_BOUND$' and dt<'$UPPER_BOUND$' and tz='$TIME_ZONE$'"
			+ " $PARTITION_COLUMNS:{part| and json_extract_path_text(partition_values,'$part$') is not null }$";

	public static String generateQuery(String query, WorkFlowContext context,
			schedulerJobRunner jobRunner, String tz, Long lb, Long ub) throws Exception {
		StringTemplate template = new StringTemplate(query);
		if (query.contains(JobExecutionContext.SUBPARTITION_VALUES)) {
			List<SubPartitionValues> subPartitionValues = subPartitionValuesGenerator(
					context, jobRunner, tz, lb, ub);
			template.setAttribute("SubPartitionValues", subPartitionValues);
		}
		return template.toString();
	}

	private static List<SubPartitionValues> getSubPartitionValuesFromSource(
			WorkFlowContext context, String tz,
			String subPartitions, Long lb, Long ub) throws JobExecutionException {
		String source = (String) context.getProperty(JobExecutionContext.SOURCES_FOR_PART_INFO);
		String[] partitionColumns = StringUtils.split(subPartitions, ',');
		String query = getQueryToFetchSubPartValues(tz, lb, ub, source,
				partitionColumns);
		QueryExecutor queryExecutor = new QueryExecutor();
		List<Object[]> result = queryExecutor.executeMetadatasqlQueryMultiple(
				query, context);
		return getValuesList(partitionColumns, result);
	}

	private static String getQueryToFetchSubPartValues(String tz, Long lb,
			Long ub, String source, String[] partitionColumns) {
		String[] sourceTables = StringUtils.split(source, ',');
		StringTemplate queryTemplate = new StringTemplate(
				QUERY_SUB_PART_VALUES);
		queryTemplate.setAttribute("TABLE_NAMES", sourceTables);
		queryTemplate.setAttribute("LOWER_BOUND", lb);
		queryTemplate.setAttribute("UPPER_BOUND", ub);
		queryTemplate.setAttribute("TIME_ZONE",
				tz != null ? tz : JobExecutionContext.DEFAULT_TIMEZONE);
		queryTemplate.setAttribute("PARTITION_COLUMNS", partitionColumns);
		return queryTemplate.toString();
	}

	private static List<SubPartitionValues> getValuesList(
			String[] partitionColumns, List<Object[]> result) {
		List<SubPartitionValues> valuesList = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(result)) {
			for(Object[] objectArray: result) {
				List<String> values = new ArrayList<>();
				for (int i=0; i<partitionColumns.length; i++) {
					values.add((String)objectArray[i]);
				}
				SubPartitionValues subPartValues = new SubPartitionValues(values);
				valuesList.add(subPartValues);
			}
		}
		return valuesList;
	}
	
	private static List<SubPartitionValues> subPartitionValuesGenerator(
			WorkFlowContext context, schedulerJobRunner jobRunner, String tz, Long lb, Long ub)
			throws Exception {
		String subPartition = (String) context
				.getProperty(GeneratorWorkFlowContext.SUBPARTITION_COLUMNS);
		List<SubPartitionValues> subPartitionValues = getSubPartitionValuesFromSource(
				context, tz, subPartition, lb, ub);
		if (CollectionUtils.isEmpty(subPartitionValues)) {
			List<Set<String>> values = retrieveSubPartitionValues(subPartition,
					context, jobRunner);
			if (CollectionUtils.isEmpty(values)) {
				throw new JobExecutionException(
						"No values avaialble in sub partition dimension tables");
			}
			Set<List<String>> cartesianList = Sets.cartesianProduct(values);
			LOGGER.debug("cartesianList:{}:size{}", cartesianList,
					cartesianList.size());
			for (List<String> valueList : cartesianList) {
				SubPartitionValues partitionValues = new SubPartitionValues(
						valueList);
				subPartitionValues.add(partitionValues);
			}
		}
		return subPartitionValues;
	}

	private static List<Set<String>> retrieveSubPartitionValues(
			String subPartition, WorkFlowContext context,
			schedulerJobRunner jobRunner) throws Exception {
		String[] subPartitionColumns = subPartition.split(",");
		List<Set<String>> subPartitionValueList = new ArrayList<>();
		for (String column : subPartitionColumns) {
			String subPartColumnQuery = QueryConstants.SUB_PART_COLUMN_QUERY
					.replace(JobExecutionContext.SUB_PARTITION_COLUMN,
							(String) column);
			LOGGER.debug("executing query:{}", subPartColumnQuery);
			QueryExecutor executor = new QueryExecutor();
			List<Object[]> resultSet = executor.executeMetadatasqlQueryMultiple(
					subPartColumnQuery, context);
			if (CollectionUtils.isNotEmpty(resultSet)) {
				LOGGER.debug("resultSet:{}", resultSet.toArray());
				subPartitionValueList
						.add(getValueSet(resultSet, context, jobRunner));
			} else {
				throw new JobExecutionException(
						"No entry found in rithomas.sub_part_column_info for sub partition column:"
								.concat(column));
			}
			LOGGER.debug("subPartitionValueList:{}", subPartitionValueList);
		}
		return subPartitionValueList;
	}

	private static Set<String> getValueSet(List<Object[]> resultSet,
			WorkFlowContext context, schedulerJobRunner jobRunner)
			throws JobExecutionException, SQLException {
		Set<String> valueSet = new HashSet<>();
		String outputDirName = ((String) context
				.getProperty(JobExecutionContext.JOB_NAME));
		Object result = jobRunner.runQuery(
				QueryConstants.SUB_PART_VALUE_QUERY
						.replace(JobExecutionContext.SUB_PARTITION_COLUMN,
								resultSet.get(0)[0].toString())
						.replace(JobExecutionContext.SUB_PARTITION_COLUMN_TABLE,
								resultSet.get(0)[1].toString()),
				outputDirName, context);
		if (result instanceof ResultSet) {
			ResultSet rs = (ResultSet) result;
			try {
				while (rs.next()) {
					String val = rs.getString(1);
					if (val != null) {
						valueSet.add(val);
					} else {
						LOGGER.warn(
								"Ignoring the null value of the result set.");
					}
				}
			} finally {
				if (rs != null)
					rs.close();
			}
		} else {
			List<String> resultValues = (List<String>) result;
			for (String value : resultValues) {
				valueSet.add(value.split(",")[0]);
			}
		}
		LOGGER.debug("valueSet:{}", valueSet);
		if (valueSet.isEmpty()) {
			throw new JobExecutionException(
					"Only null entries found in the table : "
							+ resultSet.get(0)[1].toString()
							+ ", for sub partition column: "
							+ resultSet.get(0)[0].toString());
		}
		return valueSet;
	}
}

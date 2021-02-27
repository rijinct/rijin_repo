
package com.project.rithomas.jobexecution.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class HistoricalReaggregationExecutor extends PopulateReaggregationList {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HistoricalReaggregationExecutor.class);

	private static final String DELIMITER_CHARACTER = ",";
	
	private StringJoiner enableAggJobIdList;

	private StringJoiner enableReaggJobIdList;

	private StringJoiner disableReaggJobIdList;
	
	private StringJoiner missingReaggJobList;

	private Map<String,String> jobIdTriggerStatus = new HashMap<String,String>();

	private JobExecutionContext context = new JobExecutionContext();

	public void initialize() {
		enableAggJobIdList = new StringJoiner(DELIMITER_CHARACTER);
		enableReaggJobIdList = new StringJoiner(DELIMITER_CHARACTER);
		disableReaggJobIdList = new StringJoiner(DELIMITER_CHARACTER);
		missingReaggJobList = new StringJoiner(DELIMITER_CHARACTER);
	}

	public void populateReaggListFromGroup(
			PopulateReaggParamConfig populateReaggParamConfig,
			File jobIdGroupMappingCsv) {
		initialize();
		try (FileInputStream fileInputStream = new FileInputStream(
				jobIdGroupMappingCsv);
				InputStreamReader inputStreamReader = new InputStreamReader(
						fileInputStream, StandardCharsets.UTF_8.name());
				BufferedReader br = new BufferedReader(inputStreamReader)) {
			String currentline;
			while ((currentline = br.readLine()) != null) {
				String[] recordFromFile = currentline.split(DELIMITER_CHARACTER);
				if (recordFromFile.length != 2) {
					throw new Exception();
				}
				String jobId = recordFromFile[0];
				String groupName = recordFromFile[1];
				String reaggregationJobId = null;
				LOGGER.debug("JobId's for the group :{} are : {}", groupName,
						jobId);
				String[] groupNameListArr = StringUtils.split(groupName, ":");
				List<String> groupNameList = Arrays.asList(groupNameListArr);
				if (CollectionUtils.containsAny(
						populateReaggParamConfig.enableGroupList,
						groupNameList)) {
					reaggregationJobId = getReaggregationJobId(jobId);
					if (StringUtils.isNotEmpty(reaggregationJobId)) {
						enableReaggJobIdList
								.add("'" + reaggregationJobId + "'");
						enableAggJobIdList.add("'" + jobId + "'");
						jobIdTriggerStatus.put(jobId, "Not Triggered");
					} else {
						missingReaggJobList.add("'" + jobId + "'");
					}
				} else if (CollectionUtils.containsAny(
						populateReaggParamConfig.disableGroupList,
						groupNameList)) {
					reaggregationJobId = getReaggregationJobId(jobId);
					if (StringUtils.isNotEmpty(reaggregationJobId)) {
					disableReaggJobIdList.add("'" + reaggregationJobId + "'");
					} else {
						missingReaggJobList.add("'" + jobId + "'");
					}
				}
			}
			if (StringUtils.isNotEmpty(enableReaggJobIdList.toString())) {
				updateReaggregationProperty(enableReaggJobIdList.toString(),
						"YES");
				populateReaggregationForJobId(populateReaggParamConfig,
						enableAggJobIdList.toString());
			}
			if (StringUtils.isNotEmpty(disableReaggJobIdList.toString())) {
				LOGGER.info("Disabling Reaggregation for the JobId's:{}", disableReaggJobIdList);
				updateReaggregationProperty(disableReaggJobIdList.toString(),
						"NO");
			}
		} catch (IOException e) {
			LOGGER.error("Unable to read the file as its unavailable", e);
		} catch (Exception e) {
			LOGGER.error("Invalid entries present in the file", e);
		} finally {
			if (MetadataHibernateUtil.getSessionFactory() != null) {
				MetadataHibernateUtil.getSessionFactory().close();
			}
			if (StringUtils.isNotEmpty(enableReaggJobIdList.toString())) {
				LOGGER.info(
						"Reaggregation Jobtrigger status for the given enable Jobgroup:{} are: {}",
						populateReaggParamConfig.enableGroupList,
						jobIdTriggerStatus);
			}
			if (StringUtils.isNotEmpty(missingReaggJobList.toString())) {
				LOGGER.warn(
						"Reaggregation job not found for the given aggregation jobs: {}",
						missingReaggJobList);
			}
		}
	}

	private void populateReaggregationForJobId(
			PopulateReaggParamConfig populateReaggParamConfig,
			String enableAggJobIdList) {
		LOGGER.info("Enabling Reaggregation for the JobId's:{}",
				enableAggJobIdList);
		String[] enableJobIdListArr = StringUtils.split(enableAggJobIdList,
				",");
		for (String jobId : enableJobIdListArr) {
			String jobIdWithoutQuote = jobId.replace("'", "");
			populateReaggListAndTriggerDependentJob(jobIdWithoutQuote,
					populateReaggParamConfig.startTime,
					populateReaggParamConfig.endTime,
					populateReaggParamConfig.regionId);
			jobIdTriggerStatus.replace(jobIdWithoutQuote, "Triggered");
		}
	}

	private void updateReaggregationProperty(String jobList, String paramVal) {
		String query = QueryConstants.ENABLE_DISABLE_REAGG_JOB_QUERY
				.replace(QueryConstants.JOB_ID, jobList)
				.replace(QueryConstants.PARAM_VAL, paramVal);
		LOGGER.debug("query:{}", query);
		QueryExecutor executor = new QueryExecutor();
		try {
			GetDBResource.getInstance()
					.retrievePostgresObjectProperties(context);
			executor.executePostgresqlUpdate(query, context);
		} catch (Exception e) {
			LOGGER.info("Exception while updating DB", e);
		}
	}

	public static void main(String[] args) {
		PopulateReaggParamConfig populateReaggParamConfig = new PopulateReaggParamConfig();
		HistoricalReaggregationExecutor historicalReaggregationExecutor = new HistoricalReaggregationExecutor();
		PopulateReaggregationList populateReaggregationList = new PopulateReaggregationList();
		JCommander.newBuilder().addObject(populateReaggParamConfig).build()
				.parse(args);
		String startTime = populateReaggParamConfig.startTime;
		String endTime = populateReaggParamConfig.endTime;
		File jobIdGroupMappingCsv = StringModificationUtil
				.getFileWithCanonicalPath(populateReaggParamConfig.file);
		if (populateReaggregationList.validateInput(startTime, endTime)) {
			historicalReaggregationExecutor.populateReaggListFromGroup(
					populateReaggParamConfig, jobIdGroupMappingCsv);
		}
	}
}

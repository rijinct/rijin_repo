
package com.project.rithomas.jobexecution.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import javax.persistence.EntityManager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.stringtemplate.v4.ST;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.beans.JobSchedulingData;
import com.rijin.analytics.scheduler.beans.Schedule;
import com.rijin.analytics.scheduler.beans.schedulerManagementOperationResult;
import com.rijin.analytics.scheduler.beans.Simple;
import com.rijin.analytics.scheduler.beans.Trigger;
import com.rijin.analytics.scheduler.beans.loader.ProcessorContextLoader;
import com.rijin.analytics.scheduler.processor.BaseClientProcessor;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.model.meta.query.RuntimePropertyQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class PopulateReaggregationList {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(PopulateReaggregationList.class);

	String providerJob;

	BaseClientProcessor baseClientProcessor;

	private JobDictionary reaggJobDictionary = null;

	
	public static void main(String[] args) {
		PopulateReaggregationList reaggregationExecutor = new PopulateReaggregationList();
		if (args == null || args.length < 3) {
			LOGGER.error(
					"Insufficient arguments to populate the reaggregation list");
			reaggregationExecutor.systemExit();
		}
		try {
			String aggJobName = args[0];
			String startTime = args[1];
			String endTime = args[2];
			String regionId = null;
			if (args.length > 3) {
				regionId = args[3];
			}
			reaggregationExecutor.populateReaggListAndTriggerDependentJob(
					aggJobName, startTime, endTime, regionId);
		} finally {
			if (MetadataHibernateUtil.getSessionFactory() != null) {
				MetadataHibernateUtil.getSessionFactory().close();
			}
		}
	}

	public void populateReaggListAndTriggerDependentJob(String aggJobName,
			String startTime, String endTime, String regionId) {
		if (validateInput(startTime, endTime)) {
			JobExecutionContext context = new JobExecutionContext();
			try {
				GetDBResource.getInstance()
						.retrievePostgresObjectProperties(context);
			} catch (JobExecutionException e) {
				LOGGER.error("Exception while retrieving postgres properties{}",
						e);
			}
			generateReaggQueryIfSouceIsUsage(context, aggJobName);
			boolean isUpdated = populateReaggList(aggJobName, startTime,
					endTime, regionId, context);

			if (!isUpdated) {
				systemExit();
			}
			triggerDependentJob(providerJob);
		}
	}

	private void generateReaggQueryIfSouceIsUsage(JobExecutionContext context,
			String aggJobName) {
		if (isJobSourceUsage(aggJobName)) {
			try {
				ReaggQueryGenerator queryGenerator = new ReaggQueryGenerator();
				String reaggQuery = queryGenerator.generateReaggQuery(
						fetchAggregationQuery(context, aggJobName));
				LOGGER.debug("Re-aggregation Query generated: {}", reaggQuery);
				insertUpdateToExecuteQuery(context, aggJobName, reaggQuery);
			} catch (IOException | SQLException | JobExecutionException e) {
				LOGGER.error(
						"Exception while generating and updating to_execute:{}",
						e);
			}
		}
	}

	private boolean isJobSourceUsage(String aggJobName) {
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		String sourceJobType = boundaryQuery.getSourceJobType(aggJobName);
		if (StringUtils.isNotEmpty(sourceJobType)
				&& sourceJobType.equals(JobTypeDictionary.USAGE_JOB_TYPE)) {
			return true;
		}
		return false;
	}

	private String fetchAggregationQuery(JobExecutionContext context,
			String aggJobName)
			throws JobExecutionException, SQLException, IOException {
		String sql = null;
		Object[] resultSet = fetchQueryFromToExecute(aggJobName, context);
		if (resultSet != null) {
			LOGGER.debug("Result set : {}", resultSet[0]);
			if (resultSet[0] instanceof String) {
				sql = (String) resultSet[0];
			} else {
				Clob clob = (Clob) resultSet[0];
				InputStream inputStream = clob.getAsciiStream();
				StringWriter stringWriter = new StringWriter();
				IOUtils.copy(inputStream, stringWriter);
				sql = stringWriter.toString();
			}
		}
		return sql;
	}

	protected Object[] fetchQueryFromToExecute(String jobID,
			WorkFlowContext context) throws JobExecutionException {
		ST sqlTemplate = new ST(
				"select TO_EXECUTE from TO_EXECUTE where JOB_ID = \'<jobID>\'");
		sqlTemplate.add("jobID", jobID);
		LOGGER.debug("sqlToExecute: {}", sqlTemplate);
		QueryExecutor queryExecutor = new QueryExecutor();
		return queryExecutor.executeMetadatasqlQuery(sqlTemplate.render(),
				context);
	}

	private void insertUpdateToExecuteQuery(JobExecutionContext context,
			String aggJobName, String query) throws JobExecutionException {
		QueryExecutor executor = new QueryExecutor();
		String sqlToInserOrUpdateToExecute = QueryConstants.GET_TO_EXECUTE_INSERT_UPDATE
				.replace(QueryConstants.JOB_ID,
						aggJobName.replace("AggregateJob", "ReaggregateJob"))
				.replace(QueryConstants.AGG_QUERY, query.replaceAll("'", "''"));
		executor.executePostgresqlUpdate(sqlToInserOrUpdateToExecute, context);
	}

	public boolean validateInput(String startTime, String endTime) {
		boolean isValid = true;
		String expectedFormat = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat dateFormat = new SimpleDateFormat(expectedFormat);
		Date startDate = null;
		Date endDate = null;
		try {
			startDate = dateFormat.parse(startTime);
		} catch (ParseException e) {
			isValid = false;
			LOGGER.error(
					"Given start time value {} is not in expected format {}",
					startTime, expectedFormat);
		}
		try {
			endDate = dateFormat.parse(endTime);
		} catch (ParseException e) {
			isValid = false;
			LOGGER.error("Given end time value {} is not in expected format {}",
					endTime, expectedFormat);
		}
		if (isValid && endDate.getTime() < startDate.getTime()) {
			LOGGER.error(
					"Given end time {} ({}) is less than start time {} ({})",
					endTime, endDate.getTime(), startTime, startDate.getTime());
			isValid = false;
		}
		if (!isValid) {
			systemExit();
		}
		return isValid;
	}

	private boolean populateReaggList(String aggJobName, String startTime,
			String endTime, String regionId, JobExecutionContext context) {
		boolean isUpdated = true;
		String aggLevel = null;
		String target = null;
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		BoundaryQuery boundaryQuery = new BoundaryQuery();
		JobDictionary jobDictionary = jobDictionaryQuery.retrieve(aggJobName);
		if (jobDictionary == null) {
			LOGGER.error("No aggregation job with the name {} is found.",
					aggJobName);
			return false;
		}
		List<JobProperty> jobProperties = jobDictionary.getJobProperties()
				.getJobProperty();
		if (jobProperties != null && !jobProperties.isEmpty()) {
			for (JobProperty jobProperty : jobProperties) {
				if (jobProperty.getJobPropPK().getParamname()
						.equals(JobExecutionContext.ALEVEL)) {
					aggLevel = jobProperty.getParamvalue();
				} else if (jobProperty.getJobPropPK().getParamname()
						.equals(JobExecutionContext.TARGET)) {
					target = jobProperty.getParamvalue();
				}
			}
		}
		LOGGER.debug("ALEVEL: {}, TARGET: {}", aggLevel, target);
		DateFunctionTransformation dateFuncTransformer = DateFunctionTransformation
				.getInstance();
		Date startDate = dateFuncTransformer.getDate(startTime);
		Date endDate = dateFuncTransformer.getDate(endTime);
		RuntimePropertyQuery runtimePropertyQuery = new RuntimePropertyQuery();
		String weekStartDay = runtimePropertyQuery
				.retrieve(JobExecutionContext.WEEK_START_DAY, "MONDAY");
		context.setProperty(JobExecutionContext.WEEK_START_DAY, weekStartDay);
		Long truncStartTime = dateFuncTransformer.getTrunc(startDate.getTime(),
				aggLevel, weekStartDay);
		Long truncEndTime = dateFuncTransformer.getTrunc(endDate.getTime(),
				aggLevel, weekStartDay);
		EntityManager entityManager = null;
		try {
			providerJob = getReaggregationJobId(aggJobName);
			if (providerJob == null) {
				LOGGER.error(
						"Reaggregation job not found for the given aggregation job: {}",
						aggJobName);
				isUpdated = false;
			} else {
				LOGGER.debug("Corresponding reaggregation job id: {}",
						providerJob);
				List<String> sourceJobs = boundaryQuery
						.getSourceJobIds(reaggJobDictionary.getJobid());
				LOGGER.debug("Source job ids: {}", sourceJobs);
				String requestorJob = null;
				if (sourceJobs != null && !sourceJobs.isEmpty()) {
					requestorJob = sourceJobs.get(0);
				}
				List<Boundary> boundaryList = null;
				if (regionId != null) {
					boundaryList = boundaryQuery
							.retrieveByJobIdAndRegionId(aggJobName, regionId);
				} else {
					boundaryList = boundaryQuery.retrieveByJobId(aggJobName);
				}
				Timestamp maxValue = null;
				if (boundaryList != null && !boundaryList.isEmpty()) {
					Boundary aggJobBoundary = boundaryList.get(0);
					maxValue = aggJobBoundary.getMaxValue();
				}
				LOGGER.debug("Max value of aggregation job: {}", maxValue);
				if (maxValue != null) {
					Long maxValueInMillis = maxValue.getTime();
					JobDictionary aggJob = getJobDictionary(aggJobName);
					int retention = Integer.parseInt(
							getProperty(aggJob, JobExecutionContext.RETENTIONDAYS));
					JobRetention jobRetention = new JobRetention(providerJob,
							retention, maxValueInMillis);
					TreeNode<JobRetention> parent = new TreeNode<>(jobRetention);
					Map<String, TreeNode<JobRetention>> jobMap = new HashMap<>();
					jobMap.put(providerJob, parent);
					buildHierarchy(context, providerJob, regionId, parent, jobMap);
					updateHierarchyRetention(parent);
					int updatedRetention = parent.getData().getRetention();
					Long retentionDateValue = dateFuncTransformer
							.getTruncatedDateAfterSubtractingDays(updatedRetention);
					retentionDateValue = dateFuncTransformer.getTrunc(retentionDateValue,
							aggLevel, (String) context
									.getProperty(JobExecutionContext.WEEK_START_DAY));
					if (truncEndTime > maxValueInMillis) {
						truncEndTime = maxValueInMillis;
					}
					if (truncStartTime < retentionDateValue) {
						truncStartTime = retentionDateValue;
					}
					Long nextStartTime = truncStartTime;
					ReaggregationListUtil reaggListUtil = new ReaggregationListUtil();
					Set<Long> startTimeValues = new TreeSet<>();
					while (nextStartTime <= truncEndTime) {
						startTimeValues.add(nextStartTime);
						String reportTime = dateFuncTransformer
								.getFormattedDate(new Date(nextStartTime));
						String loadTime = dateFuncTransformer
								.getFormattedDate(new Date());
						if (!reaggListUtil.recordExists(context, requestorJob,
								providerJob, reportTime, regionId)) {
							reaggListUtil.insertIntoReaggList(context, loadTime,
									reportTime, aggLevel, providerJob,
									requestorJob, regionId, "Initialized");
						}
						nextStartTime = dateFuncTransformer
								.getNextBound(nextStartTime, aggLevel)
								.getTimeInMillis();
					}
					LOGGER.info("Start time list size: {}", startTimeValues.size());
					Map<String, Set<Long>> aggLevelReportTimeMap = new HashMap<>();
					aggLevelReportTimeMap.put(aggLevel, startTimeValues);
					List<String> nextAggLevels = getNextAggLevels(aggLevel, context);
					for (String nextAggLevel : nextAggLevels) {
						aggLevelReportTimeMap.put(nextAggLevel,
								getReportTimesForAggLevel(nextAggLevel,
										startTimeValues, weekStartDay));
					}
					LOGGER.info("Agg levels: {}",
							aggLevelReportTimeMap.keySet());
					handleDependentJobs(context, parent, aggLevelReportTimeMap, regionId);
					LOGGER.info("Tree: {}", parent);
				}
			}
		} catch (JobExecutionException e) {
			LOGGER.error("Error while populating reagg list: {}",
					e.getMessage());
			isUpdated = false;
		} finally {
			if (entityManager != null) {
				entityManager.close();
			}
		}
		return isUpdated;
	}

	protected String getReaggregationJobId(String aggJobId) {
		EntityManager entityManager = null;
		String providerJob = null;
		try {
			entityManager = ModelResources.getEntityManager();
			List<JobProperty> jobPropertyList = entityManager
					.createNamedQuery(JobProperty.FIND_BY_PARAMNAME_VALUE,
							JobProperty.class)
					.setParameter(JobProperty.PARAM_NAME,
							JobExecutionContext.PERF_JOB_NAME)
					.setParameter(JobProperty.PARAM_VALUE, aggJobId)
					.getResultList();
			if (jobPropertyList == null || jobPropertyList.isEmpty()) {
				LOGGER.error(
						"Reaggregation job not found for the given aggregation job: {}",
						aggJobId);
			} else {
				JobProperty jobProperty = jobPropertyList.get(0);
				reaggJobDictionary = jobProperty.getJobDictionary();
				providerJob = reaggJobDictionary.getJobid();
				LOGGER.info("Corresponding reaggregation job id: {}",
						providerJob);
			}
		} catch (Exception e) {
			LOGGER.error("Error while populating reagg list: {}",
					e.getMessage());
		} finally {
			if (entityManager != null) {
				entityManager.close();
			}
		}
		return providerJob;
	}

	private void handleDependentJobs(JobExecutionContext context,
			TreeNode<JobRetention> parent, Map<String, Set<Long>> aggLevelReportTimeMap, String regionId)
			throws JobExecutionException {
		List<TreeNode<JobRetention>> children = parent.getChildren();
		for (TreeNode<JobRetention> child : children) {
			String reaggJob = child.getData().getJobName();
			String aggJobName = getAggJobName(reaggJob);
			JobDictionary jd = getJobDictionary(aggJobName);
			String plevel = getProperty(jd, JobExecutionContext.PLEVEL);
			Integer retentionDays = child.getData().getRetention();
			
			LOGGER.info("Job name: {}, Plevel: {}, Retention: {}, Max value: {}",
					reaggJob, plevel, retentionDays, child.getData().getMaxValue());
			DateFunctionTransformation dateTransformer = DateFunctionTransformation
					.getInstance();
			Long retentionDateValue = dateTransformer
					.getTruncatedDateAfterSubtractingDays(retentionDays);
			retentionDateValue = dateTransformer.getTrunc(retentionDateValue,
					plevel, (String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY));
			LOGGER.info("Job name: {}, retention date: {}", reaggJob, retentionDateValue);
			Set<Long> reportTimes = aggLevelReportTimeMap.get(plevel);
			for (Long reportTime : reportTimes) {
				// TODO retention handling
				if (reportTime <= child.getData().getMaxValue()) {
					if (reportTime >= retentionDateValue) {
						addEntry(context, parent.getData().getJobName(), reaggJob,
								reportTime, plevel, regionId);
					}
				} else {
					break;
				}
			}
			handleDependentJobs(context, child,
					aggLevelReportTimeMap, regionId);
		}
	}
	
		 void buildHierarchy(JobExecutionContext context, String jobName,
			String regionId, TreeNode<JobRetention> parent,
			Map<String, TreeNode<JobRetention>> jobMap)
			throws JobExecutionException {
		Map<String, Long> depJobMap = getDependentJobs(jobName, context,
				regionId);
		for (Entry<String, Long> depJobEntry : depJobMap.entrySet()) {
			String reaggJob = depJobEntry.getKey();
			String aggJobName = getAggJobName(reaggJob);
			JobDictionary jd = getJobDictionary(aggJobName);
			Integer retentionDays = Integer.parseInt(
					getProperty(jd, JobExecutionContext.RETENTIONDAYS));
			String jobEnableStatus = getProperty(jd,
					JobExecutionContext.JOB_ENABLED);
			if (StringUtils.isNotEmpty(jobEnableStatus)
					&& jobEnableStatus.equals("NO")) {
				continue;
			}
			LOGGER.info("Job name: {}, Retention: {}", reaggJob, retentionDays);
			TreeNode<JobRetention> child = null;
			if (jobMap.containsKey(reaggJob)) {
				child = jobMap.get(reaggJob);
			} else {
				child = new TreeNode<JobRetention>(new JobRetention(reaggJob,
						retentionDays, depJobEntry.getValue()));
				jobMap.put(reaggJob, child);
			}
			parent.addChild(child);
			buildHierarchy(context, reaggJob, regionId, child, jobMap);
		}
	}
	
	 void updateHierarchyRetention(TreeNode<JobRetention> parent) {
		List<TreeNode<JobRetention>> leafNodes = parent.getLeafNodes();
		for (TreeNode<JobRetention> leafNode : leafNodes) {
			int maxRetention = leafNode.getData().getRetention();
			updateHierarchy(leafNode, maxRetention);
		}
	}
	
	private void updateHierarchy(TreeNode<JobRetention> leafNode,
			int maxRetention) {
		List<TreeNode<JobRetention>> parentNodes = leafNode.getParents();
		for (TreeNode<JobRetention> parent : parentNodes) {
			if (maxRetention < parent.getData().getRetention()) {
				maxRetention = parent.getData().getRetention();
			} else {
				parent.getData().setRetention(maxRetention);
			}
			LOGGER.info(
					"Job name: {}, retention: {} after updating the hierarchy",
					parent.getData().getJobName(),
					parent.getData().getRetention());
			updateHierarchy(parent, maxRetention);
		}
	}
	
	private void addEntry(WorkFlowContext context, String jobName,
			String targetJob, Long reportTime, String plevel, String regionId)
			throws JobExecutionException {
		ReaggregationListUtil reaggListUtil = new ReaggregationListUtil();
		String reportTimeString = DateFunctionTransformation.getInstance()
				.getFormattedDate(reportTime);
		String loadTime = DateFunctionTransformation.getInstance()
				.getFormattedDate(new Date());
		if (reaggListUtil.recordExists(context, jobName, targetJob,
				reportTimeString, regionId)) {
			//TODO To update based on both time and old status
			reaggListUtil.updateReaggregationList(context, targetJob,
					"'" + jobName + "'", reportTimeString, "Pending",
					"Initialized", regionId);
		} else {
			reaggListUtil.insertIntoReaggList(context, loadTime,
					reportTimeString, plevel, targetJob, jobName, regionId,
					"Pending");
		}
	}

	private String getProperty(JobDictionary jd, String property) {
		JobPropertyQuery jpQuery = new JobPropertyQuery();
		JobProperty jp = null;
		String paramValue = null;
		jp = jpQuery.retrieve(jd.getId(), property);
		if (jp != null) {
			paramValue = jp.getParamvalue();
		}
		return paramValue;
	}

	private String getAggJobName(String jobName) {
		JobDictionary jd = getJobDictionary(jobName);
		return getProperty(jd, "PERF_JOB_NAME");
	}

	private JobDictionary getJobDictionary(String jobName) {
		JobDictionaryQuery jdQuery = new JobDictionaryQuery();
		return jdQuery.retrieve(jobName);
	}
	
	private Map<String, Long> getDependentJobs(String providerJob,
			WorkFlowContext context, String regionId) throws JobExecutionException {
		Map<String, Long> depJobMap = new HashMap<>();
		QueryExecutor executor = new QueryExecutor();
		String depJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY
				.replace(QueryConstants.JOB_ID, providerJob);
		if (regionId != null) {
			depJobQuery = QueryConstants.DEPENDENT_JOBS_QUERY_TZ
					.replace(QueryConstants.JOB_ID, providerJob)
					.replace(QueryConstants.REGION_ID, regionId)
					.replace(QueryConstants.DEFAULT_PART,
							JobExecutionContext.DEFAULT_TIMEZONE);
		}
		LOGGER.debug("Dependent job query: {}", depJobQuery);
		List dependentJobs = executor
				.executeMetadatasqlQueryMultiple(depJobQuery, context);
		if (dependentJobs != null && !dependentJobs.isEmpty()) {
			for (Object depJob : dependentJobs) {
				Object[] depJobInfo = (Object[]) depJob;
				String depJobName = depJobInfo[0].toString();
				Long maxValue = -1L;
				if (depJobInfo[2] != null) {
					String maxValueString = depJobInfo[2].toString();
					maxValue = DateFunctionTransformation.getInstance()
							.getDate(maxValueString).getTime();
				}
				depJobMap.put(depJobName, maxValue);
			}
		}
		LOGGER.info("Dependent job map for {} is : {}", providerJob, depJobMap);
		return depJobMap;
	}

	private Set<Long> getReportTimesForAggLevel(String aggLevel,
			Set<Long> startTimeValues, String weekStartDay) {
		Set<Long> reportTimes = new TreeSet<>();
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		for (Long startTime : startTimeValues) {
			reportTimes.add(dateTransformer.getTrunc(startTime, aggLevel,
					weekStartDay));
		}
		LOGGER.info("For agg level {}, number of report time values: {}",
				aggLevel, reportTimes.size());
		return reportTimes;
	}

	private List<String> getNextAggLevels(String aggLevel,
			JobExecutionContext context) throws JobExecutionException {
		List<String> aggLevels = new ArrayList<>();
		String query = "select distinct targetintervalid from rithomas.generator_conv_factor where sourceintervalid='"
				+ aggLevel + "'";
		QueryExecutor queryExecutor = new QueryExecutor();
		List<Object> resultList = queryExecutor.executeMetadatasqlQueryMultiple(
				query, context);
		for (Object result : resultList) {
			if (!aggLevel.equals(result.toString())) {
				aggLevels.add(result.toString());
			}
		}
		LOGGER.info("Next agg levels: {}", aggLevels);
		return aggLevels;
	}

	private void triggerDependentJob(String aggJobName) {
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		JobDictionary jobDictionary = jobDictionaryQuery
				.retrieve(aggJobName);
		String jobGroup = jobDictionary.getAdaptationId() + "_"
				+ jobDictionary.getAdaptationVersion();
		LOGGER.debug("Autotriggering job with name {} and group {}",
				aggJobName, jobGroup);
		JobSchedulingData jobSchedulingData = getJobSchedulingData(
				aggJobName, jobGroup);
		schedulerManagementOperationResult result = null;
		baseClientProcessor = ProcessorContextLoader.getInstance()
				.getClientProcessor();
		baseClientProcessor.setAutoTriggerMode(true);
		LOGGER.debug("Processing request : {}",
				jobSchedulingData);
		result = baseClientProcessor
				.processRequest(jobSchedulingData);
		if (result != null) {
			if (result.getErrorCount() > 0) {
				LOGGER.error("Error while triggering the job: {}",
						aggJobName);
				LOGGER.error("Error Details: {}",
						result.getErrorMsgs());
			} else {
				LOGGER.info(
						"Successfully triggered the job: {} under the group: {}",
						aggJobName, jobGroup);
			}
		}
	}

	private JobSchedulingData getJobSchedulingData(String jobName,
			String jobGroup) {
		JobSchedulingData jobSchedulingData = new JobSchedulingData();
		Schedule schedule = new Schedule();
		List<Trigger> triggers = new ArrayList<Trigger>();
		Trigger trigger = new Trigger();
		Simple simpleTrigger = new Simple();
		Date startDate = new Date();
		simpleTrigger.setJobGroup(jobGroup);
		simpleTrigger.setJobName(jobName);
		simpleTrigger
				.setName(jobName.concat(String.valueOf(startDate.getTime())));
		simpleTrigger.setGroup(jobGroup);
		trigger.setSimple(simpleTrigger);
		triggers.add(trigger);
		schedule.setTrigger(triggers);
		jobSchedulingData.setOperation("SCHEDULE");
		jobSchedulingData.setSchedule(schedule);
		return jobSchedulingData;
	}

	private void systemExit() {
		System.exit(1);
	}
	
	static class JobRetention {

		private String jobName;

		private int retention;
		
		private long maxValue;

		public JobRetention(String jobName) {
			this.jobName = jobName;
		}

		public JobRetention(String jobName, int retention, long maxValue) {
			this.jobName = jobName;
			this.retention = retention;
			this.maxValue = maxValue;
		}
		
		public String getJobName() {
			return jobName;
		}

		public void setJobName(String jobName) {
			this.jobName = jobName;
		}

		public int getRetention() {
			return retention;
		}

		public void setRetention(int retention) {
			this.retention = retention;
		}

		public long getMaxValue() {
			return maxValue;
		}

		public void setMaxValue(long maxValue) {
			this.maxValue = maxValue;
		}
		
		@Override
		public String toString() {
			return "Name: "+jobName+", Retention: "+retention;
		}
	}
}

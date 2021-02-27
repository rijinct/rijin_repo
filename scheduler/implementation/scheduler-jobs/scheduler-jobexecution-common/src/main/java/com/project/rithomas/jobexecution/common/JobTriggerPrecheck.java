
package com.project.rithomas.jobexecution.common;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.DateParameterType;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.ModifySourceJobList;
import com.project.rithomas.jobexecution.common.util.QueryConstants;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.TimeUnitConstants;
import com.project.rithomas.jobexecution.common.util.TimeZoneUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowStep;
import com.project.rithomas.sdk.workflow.deployment.util.DeployerConstants;

public class JobTriggerPrecheck extends AbstractWorkFlowStep {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobTriggerPrecheck.class);

	private static final String LB_INIT = "LOWER_BOUND";

	private static final String UPPER_BOUND = "UPPER_BOUND";

	private final QueryExecutor queryExecutor = new QueryExecutor();

	private final UpdateJobStatus updateJobStatus = new UpdateJobStatus();

	private final DateFunctionTransformation dateTransformer = DateFunctionTransformation
			.getInstance();
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String aggLevel = (String) context
				.getProperty(JobExecutionContext.ALEVEL);
		String sourceJobName = (String) context
				.getProperty(JobExecutionContext.SOURCEJOBID);
		String sourceJobType = (String) context
				.getProperty(JobExecutionContext.SOURCEJOBTYPE);
		QueryExecutor queryExecutor = new QueryExecutor();
		Long upperBound = (Long) context.getProperty(JobExecutionContext.UB);
		Long lowerBoundInitial = (Long) context
				.getProperty(JobExecutionContext.LB);
		LOGGER.debug("Job type: {}, Job name: {}, Aggregation level: {}",
				jobType, jobName, aggLevel);
		LOGGER.debug(
				"Source Job type: {}, Source Job name: {}, Lowerbound: {}, Upperbound: {}",
				new Object[] { sourceJobType, sourceJobName, lowerBoundInitial,
						upperBound });
		if (context
				.getProperty(JobExecutionContext.AUTO_TRIGGER_ENABLED) != null
				&& "NO".equalsIgnoreCase((String) context.getProperty(
						JobExecutionContext.AUTO_TRIGGER_ENABLED))) {
			LOGGER.info(
					"Auto triggering is disabled. So not triggering target jobs.");
		} else {
			LOGGER.info(
					"Checking for the jobs to be triggered for re-aggregation..");
			// Source job type will be null for usage jobs
			// for re-aggregation jobs source job type will be
			// Usage/Reaggregation
			if (JobTypeDictionary.ENTITY_JOB_TYPE.equalsIgnoreCase(jobType)) {
				addDependentExportJobs(context);
			}
			if (JobTypeDictionary.USAGE_JOB_TYPE.equalsIgnoreCase(jobType)
					|| JobTypeDictionary.PERF_REAGG_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
				try {
					boolean check = true;
					List<String> jobsToBeTriggered = new ArrayList<String>();
					if (JobTypeDictionary.USAGE_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
						Map<String, String> reaggJobBoundaryMap = (Map<String, String>) context
								.getProperty(
										JobExecutionContext.REAGG_JOB_BOUNDARY_MAP);
						if (reaggJobBoundaryMap == null
								|| reaggJobBoundaryMap.isEmpty()) {
							check = false;
						}
					}
					if (check) {
						LOGGER.debug(
								"Querying the dependent jobs to be triggered..");
						String dependentJobQuery = QueryConstants.REAGG_DEPENDENT_JOBS_QUERY
								.replace(QueryConstants.JOB_ID, jobName);
						LOGGER.debug("Query to get dependent jobs: {}",
								dependentJobQuery);
						List depJobs = queryExecutor
								.executeMetadatasqlQueryMultiple(
										dependentJobQuery, context);
						if (depJobs != null && !depJobs.isEmpty()) {
							for (Object itObj : depJobs) {
								addDependentJobs(context, queryExecutor,
										jobsToBeTriggered, itObj);
							}
						}
					}
					context.setProperty(
							JobExecutionContext.JOBS_TO_BE_TRIGGERED,
							jobsToBeTriggered);
					LOGGER.info("Jobs to be triggered: {}", jobsToBeTriggered);
				} catch (JobExecutionException e) {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(
							"Exception while executing the query: "
									+ e.getMessage(),
							e);
				}
			}
			// Retrieve jobs to be triggered for Aggregation job type
			else if (JobTypeDictionary.PERF_JOB_TYPE
					.equalsIgnoreCase(jobType)) {
				LOGGER.debug("Querying the dependent jobs to be triggered..");
				String dependentJobQuery = getDependentQuery(context, jobName);
				LOGGER.debug("Query to get dependent jobs: {}",
						dependentJobQuery);
				List<String> jobsToBeTriggered = new ArrayList<String>();
				try {
					List depJobs = queryExecutor
							.executeMetadatasqlQueryMultiple(dependentJobQuery,
									context);
					if (depJobs != null && !depJobs.isEmpty()) {
						for (Object itObj : depJobs) {
							if (itObj.getClass().isArray()) {
								// [jobid, maxvalue, alevel, regions]
								Object[] data = (Object[]) itObj;
								LOGGER.info("Dependent Job Query Result : {}",
										Arrays.toString(data));
								String depJobName = data[0].toString();
								String depJobmaxValue = null;
								if (data[1] != null) {
									depJobmaxValue = data[1].toString();
								}
								String depJobAgglevel = data[2].toString();
								String region = null;
								Map<String, Long> lowerUpperBoundMap = null;
								if (TimeZoneUtil.isTimeZoneEnabled(context)
										&& !(TimeZoneUtil
												.isTimeZoneAgnostic(context))) {
									if (data[3] != null) {
										region = data[3].toString();
									}
									if (StringUtils.isNotEmpty(region)) {
										if (!JobExecutionContext.DEFAULT_TIMEZONE
												.equalsIgnoreCase(
														(String) data[3])) {
											LOGGER.debug(
													"TIME_ZONE_SUPPORT region !=null "
															+ Arrays.toString(
																	data));
											lowerUpperBoundMap = getLowerUpperBoundsFromContext(
													context, region);
											boolean aggregation = shouldAggregate(
													lowerUpperBoundMap);
											if (aggregation) {
												addJobsToBeTriggered(context,
														jobsToBeTriggered,
														depJobName,
														depJobmaxValue,
														depJobAgglevel,
														lowerUpperBoundMap,
														region);
											}
										} else {
											addJobsToBeTriggered(context,
													jobsToBeTriggered,
													depJobName, depJobmaxValue,
													depJobAgglevel,
													getLowerUpperBoundForDefaultRgn(
															context), region);
										}
									} else {
										LOGGER.debug(
												"TIME_ZONE_SUPPORT region==null/Default "
														+ Arrays.toString(
																data));
										Long max = getLBUBFromContextForRegionNull(
												context);
										LOGGER.debug("MAX-VALUE  " + max);
										LOGGER.debug("depJobmaxValue  "
												+ depJobmaxValue);
										LOGGER.debug(
												" depJobName " + depJobName);
										if (depJobmaxValue == null
												&& max != null) {
											addJobsIfValid(jobsToBeTriggered,
													depJobName);
										} else if (depJobmaxValue != null) {
											DateFormat df1 = new SimpleDateFormat(
													"yyyy-MM-dd hh:mm:ss.S");
											Date dateMaxValue = df1
													.parse(depJobmaxValue);
											long depJobmaxValueInLong = dateMaxValue
													.getTime();
											if (max > depJobmaxValueInLong) {
												// If the job doesn't contain
												// then
												// add
												addJobsIfValid(
														jobsToBeTriggered,
														depJobName);
											}
										}
									}
								} else {
									LOGGER.debug(" ((ELSE)) "
											+ Arrays.toString(data));
									lowerUpperBoundMap = getLBUBWithOutTimeZoneEnabled(
											context);
									LOGGER.debug("Method Return LB "
											+ lowerUpperBoundMap.get(
													JobTriggerPrecheck.LB_INIT));
									LOGGER.debug("Method Return UB "
											+ lowerUpperBoundMap.get(
													JobTriggerPrecheck.UPPER_BOUND));
									addJobsToBeTriggered(context,
											jobsToBeTriggered, depJobName,
											depJobmaxValue, depJobAgglevel,
											lowerUpperBoundMap, region);
								}
							} else {
								throw new WorkFlowExecutionException(
										"Exception while querying the reaggregation list.");
							}
						}
					}
					context.setProperty(
							JobExecutionContext.JOBS_TO_BE_TRIGGERED,
							jobsToBeTriggered);
					LOGGER.info("Jobs to be triggered: {}", jobsToBeTriggered);
				} catch (JobExecutionException e) {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(
							"Exception while executing the query: "
									+ e.getMessage(),
							e);
				} catch (ParseException e) {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(
							"Exception while executing the query: "
									+ e.getMessage(),
							e);
				} catch (Exception e) {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(
							"Exception while executing the query: "
									+ e.getMessage(),
							e);
				}
			} else {
				if (!JobTypeDictionary.ENTITY_JOB_TYPE
						.equalsIgnoreCase(jobType)) {
					fetchDependentJobs(context, queryExecutor, updateJobStatus);
				}
			}
		}
		addDependentQSJob(context);
		addDependentArchivingJob(context);
		return true;
	}

	private void addDependentExportJobs(WorkFlowContext context)
			throws WorkFlowExecutionException {
		List<String> jobsToBeTriggered = getJobsToBeTriggered(context);
		try {
			String dependentExportJob = retrieveDependentExportJobs(context);
			if (isNotEmpty(dependentExportJob)) {
				addDependentJobs(context, queryExecutor, jobsToBeTriggered,
						dependentExportJob);
			}
		} catch (JobExecutionException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Exception while executing the query: " + e.getMessage(),
					e);
		}
		context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
				jobsToBeTriggered);
		LOGGER.info("Final list of export jobs to be triggered : {}",
				jobsToBeTriggered);
	}

	private String retrieveDependentExportJobs(WorkFlowContext context)
			throws JobExecutionException {
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String exportJob = null;
		Object[] dependentExportJobs = queryExecutor.executeMetadatasqlQuery(
				QueryConstants.DEPENDENT_EXPORT_JOBS_QUERY
						.replace(QueryConstants.JOB_ID, jobName),
				context);
		LOGGER.debug("Dependent export jobs query: {} ",
				QueryConstants.DEPENDENT_EXPORT_JOBS_QUERY);
		if (dependentExportJobs != null && dependentExportJobs.length > 0
				&& dependentExportJobs.getClass().isArray()) {
			exportJob = dependentExportJobs[0].toString();
		}
		return exportJob;
	}

	private List<String> getJobsToBeTriggered(WorkFlowContext context) {
		return context
				.getProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED) != null
						? (List<String>) context.getProperty(
								JobExecutionContext.JOBS_TO_BE_TRIGGERED)
						: new ArrayList<>();
	}

	@SuppressWarnings("unchecked")
	private void addDependentArchivingJob(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		List<String> jobsToBeTriggered = getJobsToBeTriggered(context);
		try {
			if (JobTypeDictionary.PERF_JOB_TYPE.equalsIgnoreCase(jobType)
					|| JobTypeDictionary.USAGE_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
				String archivingDependentJob = retrieveArchivingDependentJobs(
						context, jobName, queryExecutor);
				if (StringUtils.isNotEmpty(archivingDependentJob)) {
					addDependentJobs(context, queryExecutor, jobsToBeTriggered,
							archivingDependentJob);
				}
			}
		} catch (JobExecutionException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Exception while executing the query: " + e.getMessage(),
					e);
		} catch (ParseException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Exception while parsing the max value of the job : "
							+ e.getMessage(),
					e);
		}
		context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
				jobsToBeTriggered);
		LOGGER.info("Final list of jobs to be triggered : {}",
				jobsToBeTriggered);
	}

	private String retrieveArchivingDependentJobs(WorkFlowContext context,
			String jobName, QueryExecutor queryExecutor)
			throws JobExecutionException, ParseException {
		String archivingDependentJob = null;
		Calendar archivingJobMaxValue = null;
		Object[] archivingDepJobs = queryExecutor.executeMetadatasqlQuery(
				QueryConstants.ARCHIVING_DEPENDENT_JOBS_QUERY
						.replace(QueryConstants.JOB_ID, jobName),
				context);
		String maxValueFromBoundary = null;
		if (ArrayUtils.isNotEmpty(archivingDepJobs)
				&& archivingDepJobs.getClass().isArray()) {
			if (null != archivingDepJobs[1]) {
				maxValueFromBoundary = archivingDepJobs[1].toString();
			}
			if (isNotEmpty(maxValueFromBoundary)) {
				Date maxValue = dateTransformer.getDate(maxValueFromBoundary);
				archivingJobMaxValue = dateTransformer.getNextBound(
						maxValue.getTime(), getArchivingPlevel(context));
			}
			int archivingDays = NumberUtils.toInt(archivingDepJobs[2] != null
					? archivingDepJobs[2].toString() : null);
			if (archivingDays > 0) {
				Long archiveDayValue = DateFunctionTransformation
						.getDateBySubtractingDays(archivingDays, context)
						.getTimeInMillis();
				if (archivingJobMaxValue != null) {
					if (archiveDayValue > archivingJobMaxValue
							.getTimeInMillis()) {
						archivingDependentJob = archivingDepJobs[0].toString();
					}
				} else {
					archivingDependentJob = archivingDepJobs[0].toString();
				}
			}
		}
		LOGGER.debug("The archiving dependent job to be triggered : {}",
				archivingDependentJob);
		return archivingDependentJob;
	}

	private String getArchivingPlevel(WorkFlowContext context) {
		String archivingPlevel = (String) context
				.getProperty(JobExecutionContext.PLEVEL);
		if (JobExecutionContext.ARCHIVING_DAY_JOB_INTERVALS
				.contains(archivingPlevel)) {
			archivingPlevel = DeployerConstants.DAY_INTERVAL;
		}
		LOGGER.debug("Archiving plevel : {}", archivingPlevel);
		return archivingPlevel;
	}

	private void addDependentJobs(WorkFlowContext context,
			QueryExecutor queryExecutor, List<String> jobsToBeTriggered,
			Object itObj) throws JobExecutionException {
		String depJobName = itObj.toString();
		Object[] isJobDisabled = getDisabledJobStatus(context, queryExecutor,
				depJobName);
		if (null == isJobDisabled) {
			LOGGER.debug(
					"Dependent job from reaggregation list: {}. Checking if the job is in running state..",
					depJobName);
			Object[] result = getETLJobRunStatus(context, queryExecutor,
					depJobName);
			if (result != null) {
				String count = result[0].toString();
				if ("0".equals(count)) {
					addJobsIfValid(jobsToBeTriggered, depJobName);
				}
			}
		} else {
			LOGGER.debug("Disabled status: {}", Arrays.toString(isJobDisabled));
		}
	}

	private Object[] getETLJobRunStatus(WorkFlowContext context,
			QueryExecutor queryExecutor, String depJobName)
			throws JobExecutionException {
		return queryExecutor.executeMetadatasqlQuery(
				"select count(*) from rithomas.etl_status where job_name='"
						+ depJobName + "' and status='R'",
				context);
	}

	private Object[] getDisabledJobStatus(WorkFlowContext context,
			QueryExecutor queryExecutor, String depJobName)
			throws JobExecutionException {
		String dependentDisabledJobQuery = QueryConstants.REAGG_DEPENDENT_DISABLED_JOBS_QUERY
				.replace(QueryConstants.JOB_ID, depJobName);
		LOGGER.debug("Query to get disabled dependent jobs: {}",
				dependentDisabledJobQuery);
		return queryExecutor
				.executeMetadatasqlQuery(dependentDisabledJobQuery, context);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Long> getMinOfLowerAndUpperBounds(
			WorkFlowContext context) throws WorkFlowExecutionException {
		Map<String, Long> minLowerUpperBoundValues = new HashMap<String, Long>();
		HashMap<String, Long> leastReportTimeRegionMap = (HashMap<String, Long>) context
				.getProperty(JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION);
		LOGGER.debug("leastReportTimeRegionMap" + leastReportTimeRegionMap);
		HashMap<String, Long> maxReportTimeRegionMap = (HashMap<String, Long>) context
				.getProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION);
		LOGGER.debug("maxReportTimeRegionMap" + maxReportTimeRegionMap);
		List<String> listOfRegions = getFinalListOfRegions(context);
		List<String> skippedRegions = getRegionsToBeSkipped(context);
		for (String skippedRegion : skippedRegions) {
			leastReportTimeRegionMap.remove(skippedRegion);
			maxReportTimeRegionMap.remove(skippedRegion);
		}
		Long minValue = null;
		Long maxValue = null;
		if (leastReportTimeRegionMap.size() == listOfRegions.size()
				&& maxReportTimeRegionMap.size() == listOfRegions.size()) {
			maxValue = getMinReportTime(maxReportTimeRegionMap);
			minValue = getMinReportTime(leastReportTimeRegionMap);
		}
		minLowerUpperBoundValues.put(JobTriggerPrecheck.UPPER_BOUND, maxValue);
		minLowerUpperBoundValues.put(JobTriggerPrecheck.LB_INIT, minValue);
		LOGGER.debug("*** RETURN VALUE FROM getMinOfLowerAndUpperBounds *** "
				+ minLowerUpperBoundValues);
		return minLowerUpperBoundValues;
	}

	@SuppressWarnings("unchecked")
	private List<String> getFinalListOfRegions(WorkFlowContext context)
			throws WorkFlowExecutionException {
		List<String> skipRegionList = getRegionsToBeSkipped(context);
		List<String> regionIds = (List<String>) context
				.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
		if (skipRegionList != null && regionIds != null) {
			regionIds.removeAll(skipRegionList);
		}
		LOGGER.debug("Final list of region IDs : {}", regionIds);
		return regionIds;
	}

	private List<String> getRegionsToBeSkipped(WorkFlowContext context)
			throws WorkFlowExecutionException {
		ModifySourceJobList modifySourceJobList = new ModifySourceJobList(
				context);
		List<String> skipRegionList = modifySourceJobList
				.getListOfSkipTimeZoneForAgnosticJobs();
		LOGGER.debug("Regions list that needs to be skipped : {}",
				skipRegionList);
		return skipRegionList;
	}

	private Long getMinReportTime(HashMap<String, Long> reportTimeRegionMap) {
		Long minReportTime = null;
		LOGGER.debug(" reportTimeRegionMap.values() :: {}",
				reportTimeRegionMap.values());
		List<Long> reportTimeList = getNonNullReportTimeList(
				reportTimeRegionMap.values());
		if (reportTimeList != null && !reportTimeList.isEmpty()) {
			minReportTime = Collections.min(reportTimeList);
		}
		return minReportTime;
	}

	private List<Long> getNonNullReportTimeList(
			Collection<Long> reportTimeRegionLongList) {
		List<Long> reportTimeRegionList = new ArrayList<Long>();
		for (Long testLong : reportTimeRegionLongList) {
			if (testLong != null) {
				reportTimeRegionList.add(testLong);
			}
		}
		return reportTimeRegionList;
	}

	private Boolean addQSJobsToBeTriggered(WorkFlowContext context,
			Map<String, Long> lowerUpperBoundMap) throws ParseException {
		String aggLevel = (String) context
				.getProperty(JobExecutionContext.ALEVEL);
		Boolean isJobsToBeTriggered = false;
		Long dateLb = lowerUpperBoundMap.get(JobTriggerPrecheck.LB_INIT);
		Long dateUb = lowerUpperBoundMap.get(JobTriggerPrecheck.UPPER_BOUND);
		if (dateLb != null && dateUb != null) {
			LOGGER.debug("LB:{}", dateLb);
			LOGGER.debug("UB:{}", dateUb);
			Date dateTruncUb = new Date(
					dateTransformer.getTrunc(dateUb, aggLevel, (String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY)));
			Date dateTruncLb = new Date(
					dateTransformer.getTrunc(dateLb, aggLevel, (String) context
							.getProperty(JobExecutionContext.WEEK_START_DAY)));
			LOGGER.debug("dateTruncUb : {}, dateTruncLb : {}",
					dateTruncUb.toString(), dateTruncLb.toString());
			isJobsToBeTriggered = ((dateTruncLb.before(dateTruncUb))
					&& (dateTruncUb.compareTo(dateTruncLb) == 1));
		}
		return isJobsToBeTriggered;
	}

	private void addDependentQSJob(WorkFlowContext context)
			throws WorkFlowExecutionException {
		String jobType = (String) context
				.getProperty(JobExecutionContext.JOBTYPE);
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		List<String> jobsToBeTriggered = getJobsToBeTriggered(context);
		if (context.getProperty(
				JobExecutionContext.QS_AUTO_TRIGGER_ENABLED) != null
				&& "NO".equalsIgnoreCase((String) context.getProperty(
						JobExecutionContext.QS_AUTO_TRIGGER_ENABLED))) {
			LOGGER.info(
					"Auto triggering is disabled for Query scheduler. So not triggering Query scheduler jobs.");
		} else {
			if (JobTypeDictionary.PERF_JOB_TYPE.equalsIgnoreCase(jobType)
					|| JobTypeDictionary.PERF_REAGG_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
				try {
					if (JobTypeDictionary.PERF_REAGG_JOB_TYPE
							.equalsIgnoreCase(jobType)) {
						jobName = (String) context
								.getProperty(JobExecutionContext.PERF_JOB_NAME);
					}
					String qsDependentJob = retrieveQSDependentJobs(context,
							jobName, queryExecutor);
					if (qsDependentJob != null && !qsDependentJob.isEmpty()) {
						if (!(JobTypeDictionary.PERF_REAGG_JOB_TYPE
								.equalsIgnoreCase(jobType))
								&& TimeZoneUtil.isTimeZoneEnabled(context)
								&& !(TimeZoneUtil
										.isTimeZoneAgnostic(context))) {
							if (addQSJobsToBeTriggered(context,
									getMinOfLowerAndUpperBounds(context))) {
								jobsToBeTriggered.add(qsDependentJob);
							}
						} else {
							jobsToBeTriggered.add(qsDependentJob);
						}
					}
				} catch (JobExecutionException|ParseException e) {
					updateJobStatus.updateETLErrorStatusInTable(e, context);
					throw new WorkFlowExecutionException(
							"Exception while executing the query: "
									+ e.getMessage(),
							e);
				}
			} else if (JobTypeDictionary.ENTITY_JOB_TYPE
					.equalsIgnoreCase(jobType)) {
				jobsToBeTriggered.add(retrieveQSJobForEntityJob(jobName));
			}
		}
		context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
				jobsToBeTriggered);
		LOGGER.info("Final list of jobs to be triggered : {}",
				jobsToBeTriggered);
	}

	private String retrieveQSJobForEntityJob(String entityJobName) {
		return entityJobName.replace("CorrelationJob", "QSJob");
	}

	private String retrieveQSDependentJobs(WorkFlowContext context,
			String jobName, QueryExecutor queryExecutor)
			throws JobExecutionException {
		String qsDependentJob = null;
		if (JobExecutionContext.QS_JOB_INTERVALS
				.contains(context.getProperty(JobExecutionContext.PLEVEL))) {
			Object[] qsDepJobs = queryExecutor.executeMetadatasqlQuery(
					QueryConstants.QS_DEPENDENT_JOBS_QUERY
							.replace(QueryConstants.JOB_ID, jobName),
					context);
			if (qsDepJobs != null && qsDepJobs.length > 0
					&& qsDepJobs.getClass().isArray()) {
				// [jobid]
				qsDependentJob = qsDepJobs[0].toString();
			}
		}
		LOGGER.debug("The query scheduler dependent job to be triggered : {}",
				qsDependentJob);
		return qsDependentJob;
	}

	private Map<String, Long> getLowerUpperBoundForDefaultRgn(
			WorkFlowContext context) throws WorkFlowExecutionException {
		Map<String, Long> lowerUpperBoundValues = new HashMap<String, Long>();
		List<String> regionsForAgg = getFinalListOfRegions(context);
		List<Long> lbValues = new ArrayList<Long>();
		List<Long> ubValues = new ArrayList<Long>();
		for (String region : regionsForAgg) {
			lbValues.add((Long) context.getProperty("LB_INIT_" + region));
			ubValues.add((Long) context
					.getProperty(JobExecutionContext.UB + "_" + region));
		}
		lowerUpperBoundValues.put(JobTriggerPrecheck.LB_INIT,
				Collections.min(getNonNullReportTimeList(lbValues)));
		lowerUpperBoundValues.put(JobTriggerPrecheck.UPPER_BOUND,
				Collections.min(getNonNullReportTimeList(ubValues)));
		LOGGER.debug(
				"The lower and upper bound values map for the default region : {}",
				lowerUpperBoundValues);
		return lowerUpperBoundValues;
	}

	@SuppressWarnings("unchecked")
	private void fetchDependentJobs(WorkFlowContext context,
			QueryExecutor queryExecutor, UpdateJobStatus updateJobStatus)
			throws WorkFlowExecutionException {
		List<String> jobsToBeTriggered = new ArrayList<String>();
		String jobName = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		List<Long> upperBounds = new ArrayList<Long>();
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			List<String> regions = (List<String>) context
					.getProperty(JobExecutionContext.AGG_TIME_ZONE_REGIONS);
			Map<String, Long> offsetMap = (Map<String, Long>) context
					.getProperty(
							JobExecutionContext.REGION_TIMEZONE_OFFSET_MAP);
			for (String region : regions) {
				Long offset = offsetMap.get(region);
				Long ub = (Long) context
						.getProperty(JobExecutionContext.TIME_ZONE_UB + region);
				if (ub != null) {
					ub = ub + offset;
					upperBounds.add(ub);
				}
			}
		} else {
			Long upperBound = (Long) context
					.getProperty(JobExecutionContext.UB);
			upperBounds.add(upperBound);
		}
		LOGGER.debug(
				"Fetching dependent jobs for job: {}, which has upper bound: {}",
				new Object[] { jobName, upperBounds });
		String dependentJobQuery = QueryConstants.FETCH_DEPENDENT_JOBS_QUERY
				.replace(QueryConstants.JOB_ID, jobName);
		LOGGER.debug("Query to get dependent jobs: {}", dependentJobQuery);
		try {
			List<Object[]> dependentJobs = queryExecutor
					.executeMetadatasqlQueryMultiple(dependentJobQuery,
							context);
			if (dependentJobs != null && !dependentJobs.isEmpty()) {
				for (Object dependentJob : dependentJobs) {
					if (dependentJob.getClass().isArray()) {
						Object[] data = (Object[]) dependentJob;
						LOGGER.debug("Data of dependent job: "
								+ Arrays.toString(data));
						addDependentJobs(jobsToBeTriggered, data, upperBounds);
					}
				}
			}
			context.setProperty(JobExecutionContext.JOBS_TO_BE_TRIGGERED,
					jobsToBeTriggered);
			LOGGER.info("Jobs to be triggered: {}", jobsToBeTriggered);
		} catch (JobExecutionException e) {
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Exception while executing the dependent KPI query: "
							+ e.getMessage(),
					e);
		}
	}

	private void addDependentJobs(List<String> jobsToBeTriggered, Object[] data,
			List<Long> upperBounds) throws WorkFlowExecutionException {
		String dependentJobName = data[0].toString();
		LOGGER.debug(
				"Checking if the job has MAXVALUE as null for dependent job: {}",
				dependentJobName);
		if (data[1] == null) {
			LOGGER.debug("MAXVALUE for dependent job: {} is null.",
					dependentJobName);
			addJobsIfValid(jobsToBeTriggered, dependentJobName);
		} else {
			Long maxvalue = null, newMaxValue = null;
			String maxval = data[1].toString();
			maxvalue = getMaxValueOfDependentJob(maxval);
			LOGGER.debug("MAXVALUE for dependent job: {} is NOT null.",
					dependentJobName);
			LOGGER.debug("MAXVALUE for dependent job: {} .upperbound {}",
					maxvalue, upperBounds);
			for (Long upperBound : upperBounds) {
				if (maxvalue != null && upperBound > maxvalue) {
					LOGGER.debug(
							"Dependent job: {} has upper bound: {}, which is more than maxvalue {}.",
							new Object[] { dependentJobName, upperBounds,
									maxvalue });
					String pLvl = data[2].toString();
					long pLevel = getPLevelOfDependentJob(pLvl);
					LOGGER.debug("Dependent job: {} has PLEVEL {}.",
							new Object[] { dependentJobName, pLevel });
					newMaxValue = maxvalue + pLevel;
					if (newMaxValue <= upperBound) {
						LOGGER.debug(
								"Dependent job: {} has upper bound {}, which is more than (maxvalue + plevel) {}.",
								new Object[] { dependentJobName, upperBounds,
										newMaxValue });
						addJobsIfValid(jobsToBeTriggered, dependentJobName);
					}
				}
			}
		}
	}

	private Long getMaxValueOfDependentJob(String maxval)
			throws WorkFlowExecutionException {
		Long maxvalue = null;
		DateFormat sdf = new SimpleDateFormat(JobExecutionContext.DATEFORMAT);
		Date myDate;
		if (!("".equalsIgnoreCase(maxval)) || !maxval.isEmpty()) {
			try {
				myDate = sdf.parse(maxval);
				maxvalue = (long) myDate.getTime();
			} catch (ParseException e) {
				throw new WorkFlowExecutionException(
						"Exception while parsing the date: " + e.getMessage(),
						e);
			}
		}
		return maxvalue;
	}

	private long getPLevelOfDependentJob(String pLvl) {
		DateParameterType dateType = null;
		String aggLevel = pLvl;
		long plevel = 0;
		int interval = 0;
		if (pLvl.contains(TimeUnitConstants.MIN)) {
			interval = Integer
					.parseInt(pLvl.replace(TimeUnitConstants.MIN, ""));
			aggLevel = TimeUnitConstants.MIN;
		}
		dateType = DateParameterType.valueOf(aggLevel);
		switch (dateType) {
		case MONTH:
			plevel = TimeUnitConstants.DAYS_IN_MONTH
					* TimeUnitConstants.HOURS_IN_DAY
					* TimeUnitConstants.MINUTES_IN_HOUR
					* TimeUnitConstants.SECONDS_IN_MINUTE
					* TimeUnitConstants.MILLIS_IN_SECOND;
			break;
		case WEEK:
			plevel = TimeUnitConstants.DAYS_IN_WEEK
					* TimeUnitConstants.HOURS_IN_DAY
					* TimeUnitConstants.MINUTES_IN_HOUR
					* TimeUnitConstants.SECONDS_IN_MINUTE
					* TimeUnitConstants.MILLIS_IN_SECOND;
			break;
		case DAY:
			plevel = TimeUnitConstants.HOURS_IN_DAY
					* TimeUnitConstants.MINUTES_IN_HOUR
					* TimeUnitConstants.SECONDS_IN_MINUTE
					* TimeUnitConstants.MILLIS_IN_SECOND;
			break;
		case HOUR:
			plevel = TimeUnitConstants.MINUTES_IN_HOUR
					* TimeUnitConstants.SECONDS_IN_MINUTE
					* TimeUnitConstants.MILLIS_IN_SECOND;
			break;
		case MIN:
			plevel = TimeUnitConstants.SECONDS_IN_MINUTE
					* TimeUnitConstants.MILLIS_IN_SECOND;
			plevel = interval * plevel;
			break;
		default:
			break;
		}
		return plevel;
	}

	private void addJobsIfValid(List<String> jobsToBeTriggered,
			String depJobName) {
		if (!jobsToBeTriggered.contains(depJobName)) {
			jobsToBeTriggered.add(depJobName);
		}
	}

	protected void addJobsToBeTriggered(WorkFlowContext context,
			List<String> jobsToBeTriggered, String depJobName,
			String depJobmaxValue, String depJobAgglevel,
			Map<String, Long> lowerUpperBoundMap, String region) throws ParseException {
		Date dateLb;
		Date dateUb;
		LOGGER.debug("Dependent job alevel:" + depJobAgglevel);
		LOGGER.debug("Method addJobsToBeTriggered LB "
				+ lowerUpperBoundMap.get(JobTriggerPrecheck.LB_INIT));
		LOGGER.debug("Method addJobsToBeTriggered UB "
				+ lowerUpperBoundMap.get(JobTriggerPrecheck.UPPER_BOUND));
		dateLb = new Date(lowerUpperBoundMap.get(JobTriggerPrecheck.LB_INIT));
		dateUb = new Date(
				lowerUpperBoundMap.get(JobTriggerPrecheck.UPPER_BOUND));
		LOGGER.debug("LB:{}", dateLb.toString());
		LOGGER.debug("UB:{}", dateUb.toString());
		String zoneId = TimeZoneUtil.getZoneId(context, region);
		if (depJobmaxValue == null) {
			LOGGER.debug("Dependent job maxValue is null.");
			LOGGER.debug("dateLb.getTime() {}", dateLb.getTime());
			LOGGER.debug(
					"dateTransformer.getTrunc(dateLb.getTime(), depJobAgglevel)",
					dateTransformer.getTrunc(dateLb.getTime(), depJobAgglevel,
							(String) context.getProperty(
									JobExecutionContext.WEEK_START_DAY), zoneId));
			LOGGER.debug("dateUb.getTime()", dateUb.getTime());
			LOGGER.debug(
					"dateTransformer.getTrunc(dateUb.getTime(), depJobAgglevel)",
					dateTransformer.getTrunc(dateUb.getTime(), depJobAgglevel,
							(String) context.getProperty(
									JobExecutionContext.WEEK_START_DAY), zoneId));
			if (depJobAgglevel.equalsIgnoreCase(JobExecutionContext.MIN_15)) {
				LOGGER.debug("Adding {} to job list.", depJobName);
				addJobsIfValid(jobsToBeTriggered, depJobName);
			} else if (dateTransformer.getTrunc(dateLb.getTime(),
					depJobAgglevel,
					(String) context.getProperty(
							JobExecutionContext.WEEK_START_DAY), zoneId) < DateFunctionTransformation
									.getInstance().getTrunc(dateUb.getTime(),
											depJobAgglevel,
											(String) context.getProperty(
													JobExecutionContext.WEEK_START_DAY), zoneId)) {
				LOGGER.debug("Adding {} to job list.", depJobName);
				addJobsIfValid(jobsToBeTriggered, depJobName);
			}
		} else {
			LOGGER.debug("Dependent job maxValue: {}", depJobmaxValue);
			DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
			Date dateMaxValue = df1.parse(depJobmaxValue);
			String aggLevel = (String) context
					.getProperty(JobExecutionContext.ALEVEL);
			if (aggLevel.equalsIgnoreCase(depJobAgglevel)) {
				int dateCompare = dateUb.compareTo(dateMaxValue);
				if (dateCompare == 0 || dateCompare == 1) {
					LOGGER.debug("Adding {} to job list.", depJobName);
					addJobsIfValid(jobsToBeTriggered, depJobName);
				}
			} else {
				Date dateTruncUb = new Date(DateFunctionTransformation
						.getInstance().getTrunc(dateUb.getTime(),
								depJobAgglevel, (String) context.getProperty(
										JobExecutionContext.WEEK_START_DAY), zoneId));
				LOGGER.debug("Date UB truncated to dep job agg level {} : {}",
						depJobAgglevel, dateTruncUb);
				LOGGER.debug("dateMaxValue:{}", dateMaxValue);
				int dateCompare = dateTruncUb.compareTo(dateMaxValue);
				if (dateCompare == 1) {
					LOGGER.debug("Adding {} to job list.", depJobName);
					addJobsIfValid(jobsToBeTriggered, depJobName);
				}
			}
		}
	}

	private boolean shouldAggregate(Map<String, Long> lowerUpperBoundMap) {
		if (lowerUpperBoundMap.containsKey(JobTriggerPrecheck.UPPER_BOUND)
				&& lowerUpperBoundMap.containsKey(JobTriggerPrecheck.LB_INIT)) {
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Long> getLowerUpperBoundsFromContext(
			WorkFlowContext context, String regionId) {
		Map<String, Long> lowerUpperBoundValues = new HashMap<String, Long>();
		HashMap<String, Long> leastReportTimeRegionMap = (HashMap<String, Long>) context
				.getProperty(JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION);
		LOGGER.debug("leastReportTimeRegionMap" + leastReportTimeRegionMap);
		HashMap<String, Long> maxReportTimeRegionMap = (HashMap<String, Long>) context
				.getProperty(JobExecutionContext.MAX_REPORT_TIME_FOR_REGION);
		LOGGER.debug("maxReportTimeRegionMap" + maxReportTimeRegionMap);
		if (leastReportTimeRegionMap.get(regionId) != null
				&& maxReportTimeRegionMap.get(regionId) != null) {
			lowerUpperBoundValues.put(JobTriggerPrecheck.UPPER_BOUND,
					maxReportTimeRegionMap.get(regionId));
			lowerUpperBoundValues.put(JobTriggerPrecheck.LB_INIT,
					leastReportTimeRegionMap.get(regionId));
		}
		LOGGER.debug("*** RETURN VALUE FROM getLowerUpperBoundsFromContext *** "
				+ lowerUpperBoundValues);
		return lowerUpperBoundValues;
	}

	private Map<String, Long> getLBUBWithOutTimeZoneEnabled(
			WorkFlowContext context) {
		Map<String, Long> lowerUpperBoundValues = new HashMap<String, Long>();
		lowerUpperBoundValues.put(JobTriggerPrecheck.LB_INIT,
				(Long) context.getProperty(JobExecutionContext.LB_INIT));
		lowerUpperBoundValues.put(JobTriggerPrecheck.UPPER_BOUND,
				(Long) context.getProperty(JobExecutionContext.UB));
		LOGGER.debug("JobExecutionContext.LB_INIT - LB"
				+ lowerUpperBoundValues.get(JobTriggerPrecheck.LB_INIT));
		LOGGER.debug("JobExecutionContext.LB_UB - UB"
				+ lowerUpperBoundValues.get(JobTriggerPrecheck.UPPER_BOUND));
		return lowerUpperBoundValues;
	}

	@SuppressWarnings("unchecked")
	private Long getLBUBFromContextForRegionNull(WorkFlowContext context)
			throws WorkFlowExecutionException {
		Map<String, Long> lowerUpperBoundValues = new HashMap<String, Long>();
		Entry<String, Long> maxEntry = null;
		Long maxEntryValue = null;
		if (TimeZoneUtil.isTimeZoneEnabled(context)
				&& !(TimeZoneUtil.isTimeZoneAgnostic(context))) {
			HashMap<String, Long> leastReportTimeRegionMap = (HashMap<String, Long>) context
					.getProperty(
							JobExecutionContext.MAX_REPORT_TIME_FOR_REGION);
			LOGGER.debug(" JobExecutionContext.LEAST_REPORT_TIME_FOR_REGION  "
					+ leastReportTimeRegionMap);
			for (Entry<String, Long> entry : leastReportTimeRegionMap
					.entrySet()) {
				if (maxEntry == null
						|| entry.getValue() > maxEntry.getValue()) {
					maxEntry = entry;
				}
			}
		} else {
			lowerUpperBoundValues.put(JobTriggerPrecheck.UPPER_BOUND,
					(Long) context.getProperty(JobExecutionContext.UB));
			lowerUpperBoundValues.put(JobTriggerPrecheck.LB_INIT,
					(Long) context.getProperty(JobExecutionContext.LB_INIT));
		}
		if (maxEntry != null) {
			maxEntryValue = maxEntry.getValue();
		}
		return maxEntryValue;
	}

	private String getDependentQuery(WorkFlowContext context, String jobName) {
		String dependentJobQuery = null;
		if (TimeZoneUtil.isTimeZoneEnabled(context)) {
			dependentJobQuery = QueryConstants.AGG_DEPENDENT_JOBS_QUERY_TIME_ZONE
					.replace(QueryConstants.JOB_ID, jobName);
		} else {
			dependentJobQuery = QueryConstants.AGG_DEPENDENT_JOBS_QUERY
					.replace(QueryConstants.JOB_ID, jobName);
		}
		return dependentJobQuery;
	}
}

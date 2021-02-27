
package com.project.rithomas.jobexecution.reaggregation;

import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ArchivedPartitionHelper {

	private static final String PARTITION_TYPE_5MIN = "5MIN";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ArchivedPartitionHelper.class);

	private static final String ARCHIVING_JOB = "ArchivingJob";


	private WorkFlowContext context;

	private static final int ARCHIVED_TABLE_IDENTIFIER_POSITION = 3;

	private static final String ARCHIVED_TABLE_IDENTIFIER = "ARC_";

	public ArchivedPartitionHelper(WorkFlowContext context) {
		this.context = context;
	}

	public boolean isIntervalArchivedInSeparateTable(String interval) {
		boolean isArchivedInSeparateTable = JobExecutionContext.ARCHIVING_DAY_JOB_INTERVALS
				.contains(interval);
		LOGGER.debug("is interval archived in separate table: {}",
				isArchivedInSeparateTable);
		return isArchivedInSeparateTable;
	}

	public boolean isPartitionArchived(Long lowerBoundInMillis,
			Long archivingDate) {
		boolean isArchived = lowerBoundInMillis < archivingDate;
		LOGGER.debug("is partition archived : {}", isArchived);
		return isArchived;
	}

	public Long getArchivingDate(Integer archivingDays)
			throws JobExecutionException {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		return dateTransformer
				.getTruncatedDateAfterSubtractingDays(archivingDays);
	}

	public Integer getArchivingDaysForJob(String job)
			throws JobExecutionException {
		return getArchivingDays(getArchivingJob(job));
	}

	public Integer getArchivingDays(String sourceJob)
			throws JobExecutionException {
		Integer sourceJobArchivingDays = null;
		if (sourceJob.startsWith("Usage_") && getPartitionRTType().toUpperCase()
				.contains(PARTITION_TYPE_5MIN)) {
			sourceJobArchivingDays = getArchivingDaysFromRuntimeProp();
		} else {
			sourceJobArchivingDays = getArchivingDaysFromJobProp(sourceJob);
		}
		return sourceJobArchivingDays;
	}

	public Integer getArchivingDaysFromRuntimeProp()
			throws JobExecutionException {
		LOGGER.debug("fetching archiving days from runtime prop");
		Integer archivingDays = null;
		QueryExecutor queryExecutor = new QueryExecutor();
		String archivingDaysQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_DAYS';";
		LOGGER.debug("query : {}", archivingDaysQuery);
		Object[] days = queryExecutor
				.executeMetadatasqlQuery(archivingDaysQuery, context);
		LOGGER.info("value : {}", days);
		if (ArrayUtils.isNotEmpty(days)) {
			archivingDays = Integer.parseInt((String) days[0]);
		}
		LOGGER.debug("returning archivingDays : {}", archivingDays);
		return archivingDays;
	}

	public String getArchivingJob(String job) {
		return StringUtils.replaceEach(job,
				new String[] { "AggregateJob", "LoadJob", "ReaggregateJob" },
				new String[] { ARCHIVING_JOB, ARCHIVING_JOB, ARCHIVING_JOB });
	}

	public String getArchivingJob(String sourceTable, List<String> sourceJobs) {
		String sourceJob = getSourceJob(sourceTable, sourceJobs);
		return getArchivingJob(sourceJob);
	}

	public String getSourceJob(String sourceTable, List<String> sourceJobs) {
		String temp = StringUtils.replaceEach(sourceTable,
				new String[] { "PS_", "US_" },
				new String[] { "Perf_", "Usage_" });
		Iterable<String> resultJob = Iterables.filter(sourceJobs,
				Predicates.containsPattern(temp));
		return Iterables.get(resultJob, 0);
	}

	public Integer getArchivingDaysFromJobProp(String sourceJob)
			throws JobExecutionException {
		ST archivingDaysQuery = new ST(
				"select paramvalue from job_prop where jobid in (select id from job_dictionary where jobid = '<sourceJob>') and paramname = 'ARCHIVING_DAYS';");
		archivingDaysQuery.remove("sourceJob");
		QueryExecutor queryExecutor = new QueryExecutor();
		archivingDaysQuery.add("sourceJob", sourceJob);
		LOGGER.debug("archivingDaysQuery : {}", archivingDaysQuery.render());
		Object[] archivingValue = queryExecutor
				.executeMetadatasqlQuery(archivingDaysQuery.render(), context);
		LOGGER.debug("archivingValue : {}", archivingValue);
		return Integer.parseInt((String) archivingValue[0]);
	}

	public String getSourceTableToQuery(boolean useArchivingTable,
			String sourceTable) {
		return useArchivingTable ? getCorrespondingArchivingTable(sourceTable)
				: sourceTable;
	}

	public String getCorrespondingArchivingTable(String source) {
		StringBuilder sb = new StringBuilder(source);
		sb.insert(ARCHIVED_TABLE_IDENTIFIER_POSITION,
				ARCHIVED_TABLE_IDENTIFIER);
		return sb.toString();
	}

	public String getPartitionRTType() throws JobExecutionException {
		String partitionClause = REPORT_TIME_PARTITION_RT_CLAUSE;
		QueryExecutor queryExecutor = new QueryExecutor();
		String reportTimeClauseQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_PARTITION_TYPE';";
		Object[] type = queryExecutor
				.executeMetadatasqlQuery(reportTimeClauseQuery, context);
		if (ArrayUtils.isNotEmpty(type)
				&& PARTITION_TYPE_5MIN.equalsIgnoreCase(type[0].toString())) {
			partitionClause = FIVE_MINUTE_PARTITION_RT_CLAUSE;
		}
		return partitionClause;
	}
}

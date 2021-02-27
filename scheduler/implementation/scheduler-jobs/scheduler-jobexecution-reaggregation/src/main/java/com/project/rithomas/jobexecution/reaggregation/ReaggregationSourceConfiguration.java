
package com.project.rithomas.jobexecution.reaggregation;

import static org.apache.commons.lang3.StringUtils.substringBeforeLast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class ReaggregationSourceConfiguration {

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggregationSourceConfiguration.class);

	private String sourceTable;

	private String sourceLowerBound;

	private String sourceUpperBound;

	private String reportTimeWhereClause = "";

	private boolean useArchivingTable = false;

	private Long lowerBoundInMillis;

	private Long nextLb;

	private WorkFlowContext context;

	private List<String> sourceJobs = Collections.emptyList();;

	public ReaggregationSourceConfiguration setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
		return this;
	}

	public String getTargetTable() {
		ArchivedPartitionHelper helper = new ArchivedPartitionHelper(context);
		return helper.getSourceTableToQuery(useArchivingTable, sourceTable);
	}

	public ReaggregationSourceConfiguration process(String sourceJob)
			throws JobExecutionException {
		DateFunctionTransformation dateTransformer = DateFunctionTransformation
				.getInstance();
		this.sourceTable = getSourceTable(sourceJob);
		ArchivedPartitionHelper helper = new ArchivedPartitionHelper(context);
		LOGGER.debug("sourceJob : {}", sourceJob);
		String archivingJob = helper.getArchivingJob(sourceJob);
		LOGGER.debug("archivingJob : {}", archivingJob);
		Integer archivingDays = helper.getArchivingDays(archivingJob);
		LOGGER.debug("archivingDays : {}", archivingDays);
		Long archivingDate = helper.getArchivingDate(archivingDays);
		LOGGER.debug("archivingDate : {}", archivingDate);
		sourceLowerBound = lowerBoundInMillis.toString();
		sourceUpperBound = nextLb.toString();
		String interval = (String) context
				.getProperty(sourceJob + "_" + JobExecutionContext.PLEVEL);
		LOGGER.debug("interval plevel : {}", interval);
		LOGGER.debug("archivingDays != 0 : {}", archivingDays != 0);
		if (helper.isPartitionArchived(lowerBoundInMillis, archivingDate)
				&& helper.isIntervalArchivedInSeparateTable(interval)
				&& archivingDays != 0) {
			LOGGER.debug("updating query to use archiving table");
			reportTimeWhereClause = helper.getPartitionRTType();
			useArchivingTable = true;
			Long lowerBoundInMillisDay = dateTransformer
					.getTrunc(lowerBoundInMillis, "DAY");
			LOGGER.debug("lowerBoundInMillisDay : {}", lowerBoundInMillisDay);
			sourceLowerBound = lowerBoundInMillisDay.toString();
			sourceUpperBound = Long.toString(
					dateTransformer.getNextBound(lowerBoundInMillisDay, "DAY")
							.getTimeInMillis());
		}
		return this;
	}

	public String getSourceTable(String sourceJob) {
		String temp = StringUtils.replaceEach(sourceJob,
				new String[] { "Perf_", "Usage_" },
				new String[] { "PS_", "US_" });
		return substringBeforeLast(temp, "_");
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public ReaggregationSourceConfiguration setLowerBound(
			Long lowerBoundInMillis) {
		this.lowerBoundInMillis = lowerBoundInMillis;
		return this;
	}

	public ReaggregationSourceConfiguration setNextLb(Long nextLb) {
		this.nextLb = nextLb;
		return this;
	}

	public ReaggregationSourceConfiguration setContext(
			WorkFlowContext context) {
		this.context = context;
		return this;
	}

	public ReaggregationSourceConfiguration setSourceJobs(
			List<String> sourceJobs) {
		sourceJobs = new ArrayList<>(sourceJobs);
		this.sourceJobs = Collections.unmodifiableList(sourceJobs);
		return this;
	}

	public List<String> getSourceJobs() {
		return this.sourceJobs;
	}

	public String getReportTimeWhereClause() {
		return reportTimeWhereClause;
	}

	public String getSourceLowerBound() {
		return sourceLowerBound;
	}

	public String getSourceUpperBound() {
		return sourceUpperBound;
	}

	@Override
	public String toString() {
		return "ReaggregationSourceConfiguration [sourceTable=" + sourceTable
				+ ", sourceLowerBound=" + sourceLowerBound
				+ ", sourceUpperBound=" + sourceUpperBound
				+ ", reportTimeWhereClause=" + reportTimeWhereClause
				+ ", useArchivingTable=" + useArchivingTable
				+ ", lowerBoundInMillis=" + lowerBoundInMillis + ", nextLb="
				+ nextLb + ", context=" + context + ", sourceJobs=" + sourceJobs
				+ "]";
	}
}


package com.project.rithomas.jobexecution.common.boundary;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class JobDetails {

	private static final String VERSION_SUFFIX = "_1";

	private WorkFlowContext context;

	public JobDetails(WorkFlowContext context) {
		this.context = context;
	}

	protected Entry<String, Long> getLeastMaxValueOfSrcs(
			Map<String, Long> reportTimeMapForAllStages) {
		Entry<String, Long> maxEntry = null;
		for (Entry<String, Long> entry : reportTimeMapForAllStages.entrySet()) {
			if (entry.getValue() != null && (maxEntry == null
					|| entry.getValue() > maxEntry.getValue())) {
				maxEntry = entry;
			}
		}
		return maxEntry;
	}

	public String getSourceJobType() {
		return (String) context.getProperty(JobExecutionContext.SOURCEJOBTYPE);
	}

	public String getSourceJobName() {
		return (String) context
				.getProperty(JobExecutionContext.SOURCE_JOB_NAME);
	}

	public void setUpperBound(String region, Long finalUb) {
		if (region != null) {
			context.setProperty(JobExecutionContext.UB + "_" + region, finalUb);
		} else {
			context.setProperty(JobExecutionContext.UB, finalUb);
		}
	}

	public Long getUpperBound(String region) {
		Long finalUb;
		if (region != null) {
			finalUb = (Long) context
					.getProperty(JobExecutionContext.UB + "_" + region);
		} else {
			finalUb = (Long) context.getProperty(JobExecutionContext.UB);
		}
		return finalUb;
	}

	public void setLBINIT(String region, Long finalLb) {
		if (region != null) {
			context.setProperty("LB_INIT_" + region, finalLb);
		} else {
			context.setProperty(JobExecutionContext.LB_INIT, finalLb);
		}
	}

	public Long getLBINIT(String region) {
		Long lbInit;
		if (region != null) {
			lbInit = (Long) context.getProperty("LB_INIT_" + region);
		} else {
			lbInit = (Long) context.getProperty(JobExecutionContext.LB_INIT);
		}
		return lbInit;
	}

	public void setMaxValue(String region, Timestamp maxValue) {
		if (region != null) {
			context.setProperty("MAXVALUE_" + region, maxValue);
		} else {
			context.setProperty("MAXVALUE", maxValue);
		}
	}

	public Timestamp getMaxValue(String region) {
		return StringUtils.isNotEmpty(region) ? getMaxValueForTimezone(region)
				: getMaxValueForNonTimezone();
	}

	private Timestamp getMaxValueForTimezone(String region) {
		return (Timestamp) context.getProperty("MAXVALUE_" + region);
	}

	private Timestamp getMaxValueForNonTimezone() {
		return (Timestamp) context.getProperty("MAXVALUE");
	}

	public void setUbDstCount(String region, int ubDSTCnt) {
		if (StringUtils.isNotEmpty(region)) {
			context.setProperty(JobExecutionContext.UB_DST_COUNT + "_" + region,
					ubDSTCnt);
		} else {
			context.setProperty(JobExecutionContext.UB_DST_COUNT, ubDSTCnt);
		}
	}

	public long getUbDstCount(String region) {
		return StringUtils.isNotEmpty(region)
				? getUBDstCountForNonTimezone(region)
				: getUBDstCountForTimezone();
	}

	private long getUBDstCountForNonTimezone(String region) {
		return (int) context
				.getProperty(JobExecutionContext.UB_DST_COUNT + "_" + region);
	}

	private long getUBDstCountForTimezone() {
		return (int) context.getProperty(JobExecutionContext.UB_DST_COUNT);
	}

	public void setNextLB(String region, Long nextLb) {
		if (StringUtils.isNotEmpty(region)) {
			context.setProperty("NEXT_LB_" + region, nextLb);
		} else {
			context.setProperty(JobExecutionContext.NEXT_LB, nextLb);
		}
	}

	public Long getNextLB(String region) {
		return StringUtils.isNotEmpty(region) ? getNextLBForRegion(region)
				: getNextLBWithoutRegion();
	}

	private Long getNextLBWithoutRegion() {
		return (Long) context.getProperty(JobExecutionContext.NEXT_LB);
	}

	private Long getNextLBForRegion(String region) {
		return (Long) context.getProperty("NEXT_LB_" + region);
	}

	public void setSourceJobList(String region,
			List<String> modifiedSrcJobList) {
		if (StringUtils.isNotEmpty(region)) {
			context.setProperty(JobExecutionContext.SOURCEJOBLIST + region,
					modifiedSrcJobList);
		} else {
			context.setProperty(JobExecutionContext.SOURCEJOBLIST,
					modifiedSrcJobList);
		}
	}

	public List<String> getSourceJobList(String regionId) {
		return (StringUtils.isNotEmpty(regionId)
				? getSourceJobListForRegion(regionId) : getSourceJobList());
	}

	@SuppressWarnings("unchecked")
	private List<String> getSourceJobListForRegion(String regionId) {
		return (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST + regionId);
	}

	@SuppressWarnings("unchecked")
	private List<String> getSourceJobList() {
		return (List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST);
	}

	public void setSourceBoundList(String region,
			List<Boundary> srcBoundaryList) {
		if (StringUtils.isNotEmpty(region)) {
			context.setProperty("SOURCE_BOUND_LIST_" + region, srcBoundaryList);
		} else {
			context.setProperty(JobExecutionContext.SOURCE_BOUND_LIST,
					srcBoundaryList);
		}
	}

	public List<Boundary> getSourceBoundList(String regionId) {
		return (regionId != null ? getSourceBoundListForRegion(regionId)
				: getSourceBoundList());
	}

	private List<Boundary> getSourceBoundListForRegion(String regionId) {
		return (List<Boundary>) context
				.getProperty("SOURCE_BOUND_LIST_" + regionId);
	}

	private List<Boundary> getSourceBoundList() {
		return (List<Boundary>) context
				.getProperty(JobExecutionContext.SOURCE_BOUND_LIST);
	}

	public String getAdaptationFromJobName(String jobName) {
		JobDictionaryQuery jobDictionaryQuery = new JobDictionaryQuery();
		JobDictionary jobDictionary = jobDictionaryQuery.retrieve(jobName);
		return new StringBuilder(jobDictionary.getAdaptationId())
				.append(VERSION_SUFFIX).toString();
	}

	public String getUsageSpecId(String sourceJobId) {
		return sourceJobId.substring(sourceJobId.indexOf('_') + 1,
				sourceJobId.lastIndexOf(VERSION_SUFFIX));
	}

	public String getSpecIdVersion(String jobId) {
		return getUsageSpecId(jobId) + VERSION_SUFFIX;
	}

	protected boolean isSourceJobTypeUsage() {
		return JobTypeDictionary.USAGE_JOB_TYPE
				.equalsIgnoreCase((String) context
						.getProperty(JobExecutionContext.SOURCEJOBTYPE));
	}
}

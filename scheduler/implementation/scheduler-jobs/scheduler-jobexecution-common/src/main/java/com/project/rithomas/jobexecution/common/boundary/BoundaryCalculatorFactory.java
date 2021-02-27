
package com.project.rithomas.jobexecution.common.boundary;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class BoundaryCalculatorFactory {

	public static BoundaryCalculator getBoundaryCalculatorInstance(
			WorkFlowContext workFlowContext) {
		JobDetails jobDetails = new JobDetails(workFlowContext);
		JobPropertyRetriever jobPropertyRetriever = new JobPropertyRetriever(
				workFlowContext);
		BoundaryCalculator boundaryCalculator = null;
		if (jobDetails.isSourceJobTypeUsage()
				&& jobPropertyRetriever.arePartitionsLoadedInCouchbase()) {
			boundaryCalculator = new MultiSourceWithCache(workFlowContext);
		} else if (isMultiSource(workFlowContext)) {
			boundaryCalculator = new MultiSourceWithoutCache(workFlowContext);
		} else {
			boundaryCalculator = new SingleSource(workFlowContext);
		}
		return boundaryCalculator;
	}

	@SuppressWarnings("unchecked")
	private static boolean isMultiSource(WorkFlowContext context) {
		return CollectionUtils.size((List<String>) context
				.getProperty(JobExecutionContext.SOURCEJOBLIST)) > 1;
	}
}

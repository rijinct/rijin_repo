
package com.project.rithomas.jobexecution.common.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.generator.util.TNPGeneratorUtil;

public class KpiUtil {

	private static final String UNDER_SCORE = "_";

	private static final String TNP_JOB_PREFIX = "PS_TNP_";

	private static final String KPI_JOB_SUFFIX = "_KPIJob";

	private static final String PROFILE_JOB_SUFFIX = "_ProfileJob";

	public static List<PerformanceIndicatorSpec> getListOfAllKpis(String jobId,
			String jobDescription) {
		EntityManager entityManager = null;
		List<PerformanceIndicatorSpec> kpiList = null;
		try {
			entityManager = ModelResources.getEntityManager();
			String jobTrim = jobId.replace(TNP_JOB_PREFIX, "")
					.replace(KPI_JOB_SUFFIX, "")
					.replace(PROFILE_JOB_SUFFIX, "");
			jobTrim = jobTrim.substring(0, jobTrim.lastIndexOf(UNDER_SCORE));
			String intervalId = jobTrim
					.substring(jobTrim.lastIndexOf(UNDER_SCORE) + 1);
			jobTrim = jobTrim.substring(0, jobTrim.lastIndexOf(UNDER_SCORE));
			String version = jobTrim
					.substring(jobTrim.lastIndexOf(UNDER_SCORE) + 1);
			jobTrim = jobTrim.substring(0, jobTrim.lastIndexOf(UNDER_SCORE));
			String specId = jobTrim;
			TypedQuery<PerformanceIndicatorSpec> query = null;
			if (TNPGeneratorUtil.GROUPING_NONE.equals(jobDescription)) {
				query = entityManager.createNamedQuery(
								PerformanceIndicatorSpec.FIND_BY_PERF_SPEC_INTERVAL_GROUPBY_NULL,
								PerformanceIndicatorSpec.class);
			} else {
				query = entityManager.createNamedQuery(
								PerformanceIndicatorSpec.FIND_BY_PERF_SPEC_INTERVAL_GROUPBY,
								PerformanceIndicatorSpec.class);
			}
			query.setHint("javax.persistence.cacheStoreMode",
					CacheStoreMode.REFRESH);
			query = query
					.setParameter(PerformanceIndicatorSpec.PERF_SPEC_ID, specId)
					.setParameter(PerformanceIndicatorSpec.PERF_SPEC_VERSION,
							version)
					.setParameter(PerformanceIndicatorSpec.INTERVAL_ID,
							intervalId);
			if (!TNPGeneratorUtil.GROUPING_NONE.equals(jobDescription)) {
				query = query.setParameter(
						PerformanceIndicatorSpec.GROUPING_EXPR, jobDescription);
			}
			kpiList = query.getResultList();
		} finally {
			ModelResources.releaseEntityManager(entityManager);
		}
		return kpiList;
	}

	@SuppressWarnings("unchecked")
	public static Map<Object, Object> getBoundaryTimeTakenMap(
			WorkFlowContext context) {
		Map<Object, Object> boundaryTimeTakenMap = new HashMap<Object, Object>();
		if (context
				.getProperty(JobExecutionContext.MAP_FROM_SOURCE_JOB) != null) {
			boundaryTimeTakenMap = (Map<Object, Object>) context
					.getProperty(JobExecutionContext.MAP_FROM_SOURCE_JOB);
		}
		return boundaryTimeTakenMap;
	}

	public static void setFlagInContext(WorkFlowContext context, long nextLb,
			long ub) {
		if (nextLb == ub) {
			// Last cycle, so set the flag to true for deleting the
			// hive query log file
			context.setProperty(JobExecutionContext.FLAG, "true");
		} else {
			context.setProperty(JobExecutionContext.FLAG, "false");
		}
	}
}

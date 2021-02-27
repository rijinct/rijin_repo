
package com.project.rithomas.jobexecution.tnp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.KpiUtil;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.performance.PerformanceIndicatorSpec;
import com.project.rithomas.sdk.model.performance.ProfileIndicatorSpec;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;

public class ProfileCalculationJob extends AbstractWorkFlowStep {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ProfileCalculationJob.class);

	private static final Pattern PROF_INTERVAL_PATTERN = Pattern
			.compile("pi\\s*=\\s*'(\\d+)'");

	@Override
	public boolean execute(WorkFlowContext context)
			throws WorkFlowExecutionException {
		boolean success = true;
		UpdateJobStatus updateJobStatus = new UpdateJobStatus();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		String jobDescription = (String) context
				.getProperty(JobExecutionContext.JOB_DESCRIPTION);
		String sql = null;
		ExecutorService profileJobExecutors = null;
		try {
			sql = (String) context.getProperty(JobExecutionContext.SQL);
			List<PerformanceIndicatorSpec> kpiList = KpiUtil
					.getListOfAllKpis(jobId, jobDescription);
			String[] sqlSplit = sql
					.split(GeneratorWorkFlowContext.QUERY_SEPARATOR);
			profileJobExecutors = Executors.newFixedThreadPool(sqlSplit.length);
			List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
			List<Integer> profileIntervalList = new ArrayList<Integer>();
			for (String sqlForEachProfInterval : sqlSplit) {
				Integer profileInterval = 1;
				Matcher intervalMatcher = PROF_INTERVAL_PATTERN
						.matcher(sqlForEachProfInterval);
				if (intervalMatcher.find()) {
					profileInterval = Integer
							.parseInt(intervalMatcher.group(1));
				}
				profileIntervalList.add(profileInterval);
				LOGGER.debug("Profile interval is: {}", profileInterval);
				List<ProfileIndicatorSpec> profileList = getProfileList(kpiList,
						profileInterval);
				// trigger import job thread for each region,if tz
				// disabled default region considered
				ProfileExecutionThread thread = new ProfileExecutionThread(
						Thread.currentThread(), context, sqlForEachProfInterval,
						profileInterval, profileList);
				if (!profileJobExecutors.isShutdown()) {
					Future<Boolean> f = profileJobExecutors.submit(thread);
					futures.add(f);
				}
			}
			context.setProperty(JobExecutionContext.PROFILE_INTERVALS,
					profileIntervalList);
			if (!(futures.isEmpty())) {
				for (Future<Boolean> future : futures) {
					success = future.get();
				}
			}
		} catch (InterruptedException e) {
			LOGGER.error(
					"Thread interrupted exception while calculating Profile: {}",
					e.getMessage(), e);
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Thread interrupted exception while calculating Profile: "
							+ e.getMessage(),
					e);
		} catch (ExecutionException e) {
			LOGGER.error("Exception while calculating Profile: {}",
					e.getMessage(), e);
			updateJobStatus.updateETLErrorStatusInTable(e, context);
			throw new WorkFlowExecutionException(
					"Exception while calculating Profile: " + e.getMessage(),
					e);
		} finally {
			if (profileJobExecutors != null) {
				profileJobExecutors.shutdown();
			}
		}
		return success;
	}

	List<ProfileIndicatorSpec> getProfileList(
			List<PerformanceIndicatorSpec> kpiList, Integer profileInterval) {
		EntityManager entityManager = null;
		List<ProfileIndicatorSpec> profList = new ArrayList<ProfileIndicatorSpec>();
		try {
			entityManager = ModelResources.getEntityManager();
			TypedQuery<ProfileIndicatorSpec> query = null;
			for (PerformanceIndicatorSpec perfIndiSpec : kpiList) {
				List<ProfileIndicatorSpec> resultList = null;
				query = entityManager.createNamedQuery(
								ProfileIndicatorSpec.FIND_BY_DERIVATION_METHOD_ALGORITHM,
								ProfileIndicatorSpec.class);
				query.setHint("javax.persistence.cacheStoreMode",
						CacheStoreMode.REFRESH);
				query.setParameter(ProfileIndicatorSpec.DERIVATION_METHOD,
						"%(" + perfIndiSpec.getIndicatorspecId() + ")%");
				query.setParameter(ProfileIndicatorSpec.DERIVATION_ALGORITHM,
						"%INTERVAL=" + profileInterval + "%");
				resultList = query.getResultList();
				if (resultList != null) {
					profList.addAll(resultList);
				}
			}
		} finally {
			ModelResources.releaseEntityManager(entityManager);
		}
		return profList;
	}
}

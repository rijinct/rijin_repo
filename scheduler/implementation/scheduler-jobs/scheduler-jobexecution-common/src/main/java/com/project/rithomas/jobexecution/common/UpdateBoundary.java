
package com.project.rithomas.jobexecution.common;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

public class UpdateBoundary {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(UpdateBoundary.class);

	public boolean updateBoundaryTable(WorkFlowContext context, String regionId)
			throws WorkFlowExecutionException {
		boolean success = true;
		Calendar calendar = new GregorianCalendar();
		String jobId = (String) context
				.getProperty(JobExecutionContext.JOB_NAME);
		EntityManager entityManager = null;
		EntityTransaction transaction = null;
		try {
			entityManager = ModelResources.getEntityManager();
			transaction = entityManager.getTransaction();
			transaction.begin();
			BoundaryQuery boundaryQuery = new BoundaryQuery();
			Boundary boundaryUpdate = null;
			List<Boundary> boundaryList = null;
			if (regionId == null || regionId.isEmpty()) {
				boundaryList = boundaryQuery.retrieveByJobId(jobId);
			} else {
				// For timezone, retrieve boundary based on jobid and region id.
				boundaryList = boundaryQuery.retrieveByJobIdAndRegionId(jobId,
						regionId);
			}
			if (!boundaryList.isEmpty()) {
				boundaryUpdate = entityManager.find(Boundary.class,
						boundaryList.get(0).getId());
			} else {
				success = false;
				throw new WorkFlowExecutionException(
						"The boundary list is empty for jobid :" + jobId);
			}
			// for timezone, set the region id value as well.
			if (regionId != null && !regionId.isEmpty()) {
				boundaryUpdate.setRegionId(regionId);
			}
			Long lbBound = (Long) context
					.getProperty(JobExecutionContext.QUERYDONEBOUND);
			calendar.setTimeInMillis(lbBound);
			String formattedDate = DateFunctionTransformation.getInstance()
					.getFormattedDate(calendar.getTime());
			if (regionId != null && !regionId.isEmpty()) {
				LOGGER.debug("Aggregation Done Bound for region {} is: {}",
						regionId, formattedDate);
			} else {
				LOGGER.debug("Aggregation Done Bound:" + formattedDate);
			}
			context.setProperty(JobExecutionContext.AGGREGATIONDONEDATE,
					formattedDate);
			calendar.setTime(DateFunctionTransformation.getInstance()
					.getDate(formattedDate));
			boundaryUpdate
					.setMaxValue(new Timestamp(calendar.getTimeInMillis()));
			transaction.commit();
		} finally {
			if (!success && transaction != null && transaction.isActive()) {
				LOGGER.error(
						"Errors while executing WorkFlow. Rolling back the transaction");
				transaction.rollback();
			}
			ModelResources.releaseEntityManager(entityManager);
		}
		return success;
	}
}

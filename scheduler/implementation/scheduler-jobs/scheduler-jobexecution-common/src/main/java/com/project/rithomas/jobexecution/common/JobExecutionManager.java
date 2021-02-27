
package com.project.rithomas.jobexecution.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.eclipse.persistence.config.CacheUsage;
import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.config.QueryType;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.beans.WorkFlowStepContextLoader;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.others.JobWorkFlowStepSequenceComparator;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.model.workflow.JobWorkflowCatalog;
import com.project.rithomas.sdk.model.workflow.JobWorkflowSteps;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowStepConfig;
import com.project.rithomas.sdk.workflow.WorkFlowUndefinedException;

public class JobExecutionManager {

	private JobExecutionContext workflowContext = new JobExecutionContext();

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(JobExecutionManager.class);

	private JobExecutionWorkflow workFlow;

	public void setWorkflowStepsAndExecute(String jobName,
			Map<String, Object> jobMetaData)
			throws WorkFlowUndefinedException, WorkFlowExecutionException {
		LOGGER.debug("Trying to retrieve JobWorkFlowCatalog for {}", jobName);
		try {
			workFlow = new JobExecutionWorkflow();
			workflowContext.setProperty(JobExecutionContext.JOB_NAME, jobName);
			initializeContextWithMetadata(jobMetaData);
			JobWorkflowCatalog jobWorkFlowCatalog = retrieveJobCatalog(jobName);
			LOGGER.debug(
					"Retrieved the JobWorkFlowCatalog for {} successfully. Constructing the WorkFlow object",
					jobName);
			Collection<JobWorkflowSteps> jobWfSteps = jobWorkFlowCatalog
					.getJobWorkflowSteps();
			if (jobWfSteps.isEmpty()) {
				LOGGER.debug("No Steps in the JobWorkFlow: {}",
						jobWorkFlowCatalog.getName());
				throw new WorkFlowExecutionException(
						"No Steps in the JobWorkFlow");
			}
			initializeWorkflow(jobWfSteps);
			workFlow.executeWorkflow(workflowContext);
		} catch (WorkFlowExecutionException exception) {
			LOGGER.error("Error while executing the workflow", exception);
			throw new WorkFlowExecutionException(
					"Exception while executing the workflow : ", exception);
		}
	}

	private void initializeContextWithMetadata(
			Map<String, Object> jobMetaData) {
		if (jobMetaData != null && !(jobMetaData.isEmpty())) {
			workflowContext.setProperty(
					JobExecutionContext.KPI_CALC_JOB_DETIALS,
					jobMetaData.get(JobExecutionContext.KPI_CALC_JOB_DETIALS));
			workflowContext.setProperty(
					JobExecutionContext.PROFILE_CALC_JOB_DETAILS, jobMetaData
							.get(JobExecutionContext.PROFILE_CALC_JOB_DETAILS));
			workflowContext.setProperty(JobExecutionContext.MAP_FROM_SOURCE_JOB,
					jobMetaData
							.get(JobExecutionContext.MAP_FOR_DEPENDENT_JOBS));
		}
	}

	private JobWorkflowCatalog retrieveJobCatalog(String jobName) {
		EntityManager entityManager = null;
		try {
			entityManager = ModelResources.getEntityManager();
			TypedQuery<JobWorkflowCatalog> query = entityManager
					.createNamedQuery(JobWorkflowCatalog.FIND_BY_JOBWFID,
							JobWorkflowCatalog.class);
			query.setHint(QueryHints.CACHE_USAGE,
					CacheUsage.CheckCacheThenDatabase);
			query.setHint(QueryHints.QUERY_TYPE, QueryType.ReadObject);
			List<JobWorkflowCatalog> jobWorkFlowCatalogList = query
					.setParameter(JobWorkflowCatalog.JOB_WFID, jobName)
					.getResultList();
			return jobWorkFlowCatalogList.get(0);
		} finally {
			ModelResources.releaseEntityManager(entityManager);
		}
	}

	private void initializeWorkflow(Collection<JobWorkflowSteps> jobWfSteps) throws WorkFlowUndefinedException {
		List<JobWorkflowSteps> jobWorkflowStepsList = new ArrayList<>(jobWfSteps);
		Collections.sort(jobWorkflowStepsList, new JobWorkFlowStepSequenceComparator());
		WorkFlowStepContextLoader applicationContext = WorkFlowStepContextLoader.getInstance();
		for (JobWorkflowSteps jobWfStep : jobWorkflowStepsList) {
			WorkFlowStep wfStep = getWorkFlowStep(applicationContext, jobWfStep);
			if (wfStep != null) {
				workFlow.addToWorkFlow(wfStep);
			}
		}
	}

	private WorkFlowStep getWorkFlowStep(WorkFlowStepContextLoader applicationContext, JobWorkflowSteps jobWfStep)
			throws WorkFlowUndefinedException {
		WorkFlowStep wfStep = null;
		try {
			wfStep = applicationContext.getJobWorkFlowStepExecutor(jobWfStep.getJobWorkFlowStepCatalog().getWfstepid());
		} catch (NoSuchBeanDefinitionException exception) {
			LOGGER.info("Fetching workflow step from DB");
			wfStep = getJobWorkFlowStepFromDB(jobWfStep);
		}
		WorkFlowStepConfig config = getWorkFlowStepConfig(jobWfStep);
		wfStep.setConfig(config);

		return wfStep;
	}

	private WorkFlowStep getJobWorkFlowStepFromDB(JobWorkflowSteps jobWfStep) throws WorkFlowUndefinedException {
		WorkFlowStep wfStep = null;
		String jobWfClass = jobWfStep.getJobWorkFlowStepCatalog().getExecutor();
		try {
			wfStep = (WorkFlowStep) Class.forName(jobWfClass).newInstance();
		} catch (InstantiationException | IllegalAccessException |  ClassNotFoundException e) {
			LOGGER.error("Failed to retrieve job work flow step from DB",e);
			throw new WorkFlowUndefinedException("Unable to create new instance of WorkFlow class" + e.getMessage(), e);
		}
		return wfStep;
	}
	
	private WorkFlowStepConfig getWorkFlowStepConfig(JobWorkflowSteps jobWfStep) {
		WorkFlowStepConfig config = new WorkFlowStepConfig();
		config.setName(jobWfStep.getJobWorkFlowStepCatalog().getName());
		config.setWfId(jobWfStep.getJobWorkFlowCatalog().getJobWfid());
		config.setSequenceId(jobWfStep.getSequence().intValue());
		config.setWfStepId(jobWfStep.getJobWorkFlowStepCatalog().getWfstepid());
		config.setAbortIfFails(Boolean.valueOf(jobWfStep.getJobWorkFlowStepCatalog().getAbort()));
		return config;
	}

	public void interrupt(boolean interrupted) {
		LOGGER.info("Sending interrupt signal to JobExecutionWorkFlow");
		this.workFlow.interrupt(interrupted);
	}
}

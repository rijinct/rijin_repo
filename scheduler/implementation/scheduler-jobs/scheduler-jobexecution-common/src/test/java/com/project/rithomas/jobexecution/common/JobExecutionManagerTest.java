
package com.project.rithomas.jobexecution.common;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.JobExecutionManager;
import com.project.rithomas.jobexecution.common.JobExecutionWorkflow;
import com.project.rithomas.jobexecution.common.beans.WorkFlowStepContextLoader;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.JobDetails;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.model.workflow.JobWorkflowCatalog;
import com.project.rithomas.sdk.model.workflow.JobWorkflowStepCatalog;
import com.project.rithomas.sdk.model.workflow.JobWorkflowSteps;
import com.project.rithomas.sdk.workflow.AbstractWorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowStep;
import com.project.rithomas.sdk.workflow.WorkFlowUndefinedException;
import com.project.rithomas.sdk.xstream.processor.ModelConvertor;


@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.project.rithomas.jobexecution.common.beans.WorkFlowStepContextLoader")
@PrepareForTest(value = { WorkFlowStepContextLoader.class, JobExecutionManager.class, EntityManager.class,
		ModelResources.class })
public class JobExecutionManagerTest {

	WorkFlowContext context = new JobExecutionContext();

	JobExecutionManager jobExecSteps = null;

	@Mock
	EntityManager entityManager;

	@Mock
	TypedQuery query;

	@Mock
	WorkFlowStep workFlowStep;

	@Mock
	JobWorkflowCatalog jobWorkflowCataMock;

	@Mock
	JobExecutionWorkflow jobExecWorkflow;
	
	@Mock
	WorkFlowStepContextLoader mockApplicationContextLoader;

	@Mock
	AbstractWorkFlowStep mockAbstractWorkFlowStep;
	
	List<JobWorkflowCatalog> jobWorkFlowCatalogList = new ArrayList<JobWorkflowCatalog>();

	JobWorkflowCatalog jobWorkflowCata = new JobWorkflowCatalog();

	List<JobWorkflowStepCatalog> jobWorkflowStepCataList = new ArrayList<JobWorkflowStepCatalog>();

	Collection<JobWorkflowSteps> jobWorkflowStepsList = new ArrayList<JobWorkflowSteps>();

	JobWorkflowSteps jobWorkflowSteps = new JobWorkflowSteps();

	JobWorkflowStepCatalog jobWorkflowStepCata = new JobWorkflowStepCatalog();


	
	@Before
	public void setUp() throws Exception {
		jobExecSteps = new JobExecutionManager();
		mockStatic(ModelResources.class);
		mockStatic(WorkFlowStepContextLoader.class);
		PowerMockito.whenNew(JobWorkflowCatalog.class).withNoArguments()
				.thenReturn(jobWorkflowCataMock);
		PowerMockito.whenNew(JobExecutionWorkflow.class).withNoArguments()
				.thenReturn(jobExecWorkflow);
		PowerMockito.whenNew(WorkFlowStepContextLoader.class).withNoArguments().thenReturn(mockApplicationContextLoader);
		PowerMockito.when(WorkFlowStepContextLoader.getInstance()).thenReturn(mockApplicationContextLoader);
		// mocking EntityManager
		Mockito.when(
				entityManager.createNamedQuery(Mockito.anyString(),
						(Class) Mockito.any())).thenReturn(query);
		Mockito.when(
				query.setParameter(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(query);
		Mockito.when(ModelResources.getEntityManager()).thenReturn(
				entityManager);
		// Setting jobworkflowcatalog		
		
		File testResources = new File("src/test/resources/jobworkflowcata.txt");
		String filePath = testResources.getAbsolutePath();
		JobDetails jobDetails = (JobDetails) ModelConvertor.getInstance()
				.parseFile(filePath);
		jobWorkflowCata = jobDetails.getJobWorkflowCatalog().get(0);
		jobWorkflowStepCataList = jobDetails.getJobWorkflowStepCatalog();
		jobWorkflowSteps.setJobWorkFlowCatalog(jobWorkflowCata);
		int sequence = 1;
		for (JobWorkflowStepCatalog jobWorkflowStepsCata : jobWorkflowStepCataList) {
			jobWorkflowSteps.setJobWorkFlowStepCatalog(jobWorkflowStepsCata);
			jobWorkflowSteps.setSequence(sequence);
			sequence++;
			jobWorkflowStepsList.add(jobWorkflowSteps);
		}
		jobWorkflowCata.setJobWorkflowSteps(jobWorkflowStepsList);
		jobWorkFlowCatalogList.add(jobWorkflowCata);
		Collection<JobWorkflowSteps> jobWorkflowStepsCol = null;
		Mockito.when(jobWorkflowCataMock.getJobWorkflowSteps()).thenReturn(
				jobWorkflowStepsCol);
		Mockito.when(query.getResultList()).thenReturn(jobWorkFlowCatalogList);
		Mockito.when(workFlowStep.execute(context)).thenReturn(true);
		Mockito.when(mockApplicationContextLoader.getJobWorkFlowStepExecutor(Mockito.anyString())).thenReturn(mockAbstractWorkFlowStep);
	}

	@Test
	public void testJobExexcutionSteps() throws WorkFlowUndefinedException,
			WorkFlowExecutionException {
		jobExecSteps.setWorkflowStepsAndExecute(
				"Perf_VLR_1_15MIN_AggregateJob", null);
	}
}

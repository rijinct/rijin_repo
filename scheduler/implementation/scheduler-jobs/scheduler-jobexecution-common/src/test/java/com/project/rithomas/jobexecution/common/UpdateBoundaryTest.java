
package com.project.rithomas.jobexecution.common;

import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.sdk.model.meta.Boundary;
import com.project.rithomas.sdk.model.meta.query.BoundaryQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { UpdateBoundary.class, ModelResources.class })
public class UpdateBoundaryTest {

	WorkFlowContext context = new JobExecutionContext();

	UpdateBoundary updateBoundary = null;

	@Mock
	BoundaryQuery boundQuery;

	@Mock
	EntityManager entityManager;

	@Mock
	EntityTransaction transaction;

	Boundary boundary = new Boundary();

	List<Boundary> boundaryList = new ArrayList<Boundary>();

	@Before
	public void setUp() throws Exception {
		updateBoundary = new UpdateBoundary();
		// mocking new object call
		PowerMockito.whenNew(BoundaryQuery.class).withNoArguments()
				.thenReturn(boundQuery);
		context.setProperty(JobExecutionContext.ENTITY_MANAGER, entityManager);
		mockStatic(ModelResources.class);
		PowerMockito.when(ModelResources.getEntityManager()).thenReturn(
				entityManager);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.QUERYDONEBOUND,
				Long.parseLong("1354311000000"));
		Mockito.when(entityManager.getTransaction()).thenReturn(transaction);
		Timestamp timeStamp = new Timestamp(new SimpleDateFormat(
				"yyyy.MM.dd HH:mm:ss", Locale.ENGLISH).parse(
				"2012.12.02 02:00:00").getTime());
		// setting boundary values
		boundary.setSourceJobId("Perf_VLR_1_15MIN_AggregateJob");
		boundary.setSourcePartColumn("REPORT_TIME");
		boundary.setMaxValue(timeStamp);
		boundary.setSourceJobType("Aggregation");
		boundary.setJobId("Perf_VLR_1_HOUR_AggregateJob");
		boundary.setId(1);
	}

	@Test
	public void testUpdateBoundary() throws WorkFlowExecutionException {
		// setting to the list
		boundaryList.add(boundary);
		Mockito.when(boundQuery.retrieveByJobId("Perf_VLR_1_HOUR_AggregateJob"))
				.thenReturn(boundaryList);
		Mockito.when(
				entityManager.find(Boundary.class, boundaryList.get(0).getId()))
				.thenReturn(boundary);
		updateBoundary.updateBoundaryTable(context, null);
	}

	@Test
	public void testUpdateBoundaryWithRegionID()
			throws WorkFlowExecutionException {
		boundary.setRegionId("Region1");
		// setting to the list
		boundaryList.add(boundary);
		Mockito.when(
				boundQuery.retrieveByJobIdAndRegionId(
						"Perf_VLR_1_HOUR_AggregateJob", "Region1")).thenReturn(
				boundaryList);
		Mockito.when(
				entityManager.find(Boundary.class, boundaryList.get(0).getId()))
				.thenReturn(boundary);
		updateBoundary.updateBoundaryTable(context, "Region1");
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testUpdateBoundaryWithNoEntries()
			throws WorkFlowExecutionException {
		Mockito.when(
				boundQuery.retrieveByJobIdAndRegionId(
						"Perf_VLR_1_HOUR_AggregateJob", "Region1")).thenReturn(
				null);
		Mockito.when(transaction.isActive()).thenReturn(true);
		Mockito.doNothing().when(transaction).rollback();
		updateBoundary.updateBoundaryTable(context, null);
	}
}


package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.sql.SQLException;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { UpdateJobStatus.class })
public class UpdateJobStatusTest {

	UpdateJobStatus updateJobStatus = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	Session session;

	@Mock
	QueryExecutor queryExec;

	@Before
	public void setUp() throws Exception {
		updateJobStatus = new UpdateJobStatus();
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		// setting parameters in context
		context.setProperty(JobExecutionContext.DESCRIPTION,
				"Job completed successfully");
		context.setProperty(JobExecutionContext.STATUS, "S");
		BigInteger i = new BigInteger("5");
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, i);
		context.setProperty(JobExecutionContext.ERROR_DESCRIPTION,
				"Error in Query");
	}

	@Test
	public void testUpdateFinalJobStatus()
			throws SQLException, WorkFlowExecutionException {
		updateJobStatus.updateFinalJobStatus(context);
	}

	@Test
	public void testUpdateFinalJobStatusForI()
			throws SQLException, WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.STATUS, "I");
		updateJobStatus.updateFinalJobStatus(context);
	}

	@Test
	public void testInsertJobStatusForException()
			throws WorkFlowExecutionException, JobExecutionException {
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						"select nextval('rithomas.ETL_STAT_SEQ')", context))
				.thenThrow(new JobExecutionException(
						"Exception while executing query "));
		try {
			updateJobStatus.insertJobStatus(context);
		} catch (WorkFlowExecutionException e) {
			assertEquals("Exception while executing query", e.getMessage());
		}
	}

	@Test
	public void testInsertJobStatusForS()
			throws WorkFlowExecutionException, JobExecutionException {
		// setting parameters in context
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.STATUS, "S");
		Object[] resultSetSeq = { 4 };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						"select nextval('rithomas.ETL_STAT_SEQ')", context))
				.thenReturn(resultSetSeq);
		updateJobStatus.insertJobStatus(context);
	}

	@Test
	public void testInsertJobStatusForW()
			throws WorkFlowExecutionException, JobExecutionException {
		// setting parameters in context
		context.setProperty(JobExecutionContext.JOBTYPE, "Aggregation");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.STATUS, "W");
		Object[] resultSetSeq = { 4 };
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						"select nextval('rithomas.ETL_STAT_SEQ')", context))
				.thenReturn(resultSetSeq);
		updateJobStatus.insertJobStatus(context);
	}

	@Test
	public void shouldUpdateETLErrorStatusInTable()
			throws WorkFlowExecutionException {
		Exception ex = new Exception("Some Error");
		updateJobStatus.updateETLErrorStatusInTable(ex, context);
	}

	@Test
	public void shouldThrowErrorWhileUpdatingETLErrorStatusInTable()
			throws JobExecutionException {
		PowerMockito
				.when(queryExec.executeMetadatasqlQuery(
						"select nextval('rithomas.ETL_STAT_SEQ')", context))
				.thenThrow(new JobExecutionException(
						"Exception while executing query "));
		Exception ex = new Exception("Some Error");
		try {
			updateJobStatus.updateETLErrorStatusInTable(ex, context);
		} catch (WorkFlowExecutionException e) {
			assertEquals("Exception while executing query", e.getMessage());
		}
	}
}

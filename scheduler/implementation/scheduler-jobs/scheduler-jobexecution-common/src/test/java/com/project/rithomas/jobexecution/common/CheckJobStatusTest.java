
package com.project.rithomas.jobexecution.common;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Calendar;

import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.ca4ci.pm.client.PasswordManagementClient;
import com.rijin.ca4ci.pm.helper.DbType;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobTypeDictionary;
import com.project.rithomas.sdk.model.utils.SDKEncryptDecryptUtils;
import com.project.rithomas.sdk.model.utils.SDKSystemEnvironment;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { CheckJobStatus.class, GetDBResource.class, Thread.class, SDKSystemEnvironment.class,SDKEncryptDecryptUtils.class,PasswordManagementClient.class })
public class CheckJobStatusTest {

	CheckJobStatus checkJobStatus = null;

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	QueryExecutor queryExec;

	@Mock
	Session session;

	@Mock
	UpdateJobStatus updateJobStatus;
	
	@Mock
	PasswordManagementClient pwdCient;

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(SDKSystemEnvironment.class);
		PowerMockito.mockStatic(SDKEncryptDecryptUtils.class);
		checkJobStatus = new CheckJobStatus();
		// setting new object
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(queryExec);
		// setting in context
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.WAIT_MINUTE,
				Long.parseLong("0"));
		context.setProperty(JobExecutionContext.ALEVEL, "15MIN");
		
		File testResourcesDirectory = new File("src/test/resources");
		
		PowerMockito.when(SDKSystemEnvironment.getschedulerConfDirectory()).thenReturn(testResourcesDirectory.getAbsolutePath()+File.separatorChar);
		PowerMockito.when(SDKSystemEnvironment.getSDKApplicationConfDirectory()).thenReturn(testResourcesDirectory.getAbsolutePath()+File.separatorChar);
		PowerMockito.when(SDKEncryptDecryptUtils.decryptPassword(Mockito.anyString())).thenReturn("rithomas");
		PowerMockito.whenNew(PasswordManagementClient.class).withNoArguments().thenReturn(pwdCient);
		Mockito.when(pwdCient.readPassword(DbType.POSTGRES, "sai", "rithomas")).thenReturn("rithomas");
		
	}

	@Test
	public void testCheckJobStatusWithR() throws WorkFlowExecutionException,
			JobExecutionException {
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_JOB_TYPE);
		Object[] resultSet = { "R" };
		Object[] resultSet1 = { "C" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet, resultSet1);
		checkJobStatus.execute(context);
	}

	@Test
	public void testCheckJobStatusWithS() throws WorkFlowExecutionException,
			JobExecutionException {
		Object[] resultSet = { "S" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet);
		checkJobStatus.execute(context);
	}

	@Test
	public void testCheckJobStatusWithI() throws WorkFlowExecutionException,
			JobExecutionException {
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_JOB_TYPE);
		Object[] resultSet = { "I" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet);
		checkJobStatus.execute(context);
	}

	@Test
	public void testCheckJobStatusWithJobException()
			throws WorkFlowExecutionException, JobExecutionException {
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenThrow(
						new JobExecutionException(
								"Exception while executing query  "));
		try {
			checkJobStatus.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertEquals("Exception while checking job status ", e.getMessage());
		}
	}

	@Test
	public void testReaggJob() throws WorkFlowExecutionException,
			JobExecutionException {
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		Object[] resultSet = { "R" };
		Object[] resultSet1 = { "S" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet, resultSet1);
		checkJobStatus.execute(context);
	}

	@Test
	public void testReaggJob1() throws WorkFlowExecutionException,
			JobExecutionException {
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		Object[] resultSet = { "C" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet);
		checkJobStatus.execute(context);
	}

	@Test
	public void testReaggJobOffPeakHour1() throws WorkFlowExecutionException,
			JobExecutionException {
		Integer hour2 = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		context.setProperty(JobExecutionContext.OFF_PEAK_HOURS,
				String.valueOf(hour2));
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		Object[] resultSet = { "S" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet);
		checkJobStatus.execute(context);
	}

	@Test
	public void testReaggJobOffPeakHour2() throws WorkFlowExecutionException,
			JobExecutionException {
		Integer hour2 = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + 2;
		Integer hour3 = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + 3;
		Integer hour5 = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + 5;
		String hour2to3 = new String();
		hour2to3 = hour2 + "-" + hour3;
		String hourOffPeak = new String();
		hourOffPeak = hour2to3 + "," + hour5;
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		context.setProperty(JobExecutionContext.OFF_PEAK_HOURS, hourOffPeak);
		Object[] resultSet = { "S" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet);
		checkJobStatus.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testInterruptedException() throws InterruptedException,
			WorkFlowExecutionException, JobExecutionException {
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_JOB_TYPE);
		Object[] resultSet = { "R" };
		Object[] resultSet1 = { "C" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet, resultSet1);
		PowerMockito.mockStatic(Thread.class);
		PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
		Thread.sleep(0);
		checkJobStatus.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testReaggJobInterruptedException()
			throws WorkFlowExecutionException, JobExecutionException,
			InterruptedException {
		context.setProperty(JobExecutionContext.ALEVEL, "HOUR");
		context.setProperty(JobExecutionContext.JOBTYPE,
				JobTypeDictionary.PERF_REAGG_JOB_TYPE);
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Perf_VLR_1_HOUR_ReaggregateJob");
		context.setProperty(JobExecutionContext.PERF_JOB_NAME,
				"Perf_VLR_1_HOUR_AggregateJob");
		Object[] resultSet = { "R" };
		Object[] resultSet1 = { "S" };
		PowerMockito
				.when(queryExec
						.executeMetadatasqlQuery(
								"select STATUS from rithomas.etl_status where proc_id=(select max(proc_id) from rithomas.etl_status where job_name='Perf_VLR_1_HOUR_AggregateJob' and status != 'W')",
								context)).thenReturn(resultSet, resultSet1);
		PowerMockito.mockStatic(Thread.class);
		PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
		Thread.sleep(0);
		checkJobStatus.execute(context);
	}
}